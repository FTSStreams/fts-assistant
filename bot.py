import discord
from discord.ext import commands, tasks
import os
import requests
import asyncio
from datetime import datetime
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from discord import app_commands
from tenacity import retry, stop_after_attempt, wait_exponential
import datetime as dt

# Load environment variables
load_dotenv()

# Validate environment variables
required_env_vars = ["DISCORD_TOKEN", "ROOBET_API_TOKEN", "TIPPING_API_TOKEN", "ROOBET_USER_ID", "DATABASE_URL", "GUILD_ID"]
for var in required_env_vars:
    if not os.getenv(var):
        raise ValueError(f"Missing required environment variable: {var}")

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("bot.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Set up the bot
intents = discord.Intents.default()
intents.members = True
intents.message_content = False  # Explicitly disable, not needed for slash commands
bot = commands.Bot(command_prefix="!", intents=intents)

# Roobet API configuration
AFFILIATE_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
TIPPING_API_URL = "https://roobet.com/_api/tipping/send"
ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
TIPPING_API_TOKEN = os.getenv("TIPPING_API_TOKEN")
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")
GUILD_ID = int(os.getenv("GUILD_ID"))
LEADERBOARD_CHANNEL_ID = 1324462489404051487
MILESTONE_CHANNEL_ID = 1339413771000614982  # 🔓︱wager-milestone

# Prizes distribution ($1,500 total)
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]

# Milestone tiers
MILESTONES = [
    {"tier": "Bronze", "threshold": 500, "tip": 2.85, "color": discord.Color.orange(), "emoji": "🥉"},
    {"tier": "Silver", "threshold": 1000, "tip": 2.85, "color": discord.Color.light_grey(), "emoji": "🥈"},
    {"tier": "Gold", "threshold": 2500, "tip": 8.55, "color": discord.Color.gold(), "emoji": "🥇"},
    {"tier": "Platinum", "threshold": 5000, "tip": 14.25, "color": discord.Color.teal(), "emoji": "💎"},
    {"tier": "Diamond", "threshold": 10000, "tip": 28.50, "color": discord.Color.blue(), "emoji": "💠"},
    {"tier": "Master", "threshold": 25000, "tip": 85.50, "color": discord.Color.purple(), "emoji": "👑"},
    {"tier": "Grand Master", "threshold": 50000, "tip": 142.50, "color": discord.Color.red(), "emoji": "🌟"},
    {"tier": "Legend", "threshold": 100000, "tip": 285.00, "color": discord.Color.green(), "emoji": "🏆"}
]

# In-memory tracking
CURRENT_CYCLE_TIPS = set()  # Format: {(user_id, tier)}

# Database connection pool
db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, os.getenv("DATABASE_URL"))

def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Failed to get DB connection: {e}")
        raise

def release_db_connection(conn):
    db_pool.putconn(conn)

def init_db():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tips (
                    user_id TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    tipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, tier)
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            conn.commit()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        release_db_connection(conn)

def load_tips():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, tier FROM tips;")
            tips = {(row[0], row[1]) for row in cur.fetchall()}
        logger.info(f"Loaded {len(tips)} tips from database.")
        return tips
    except Exception as e:
        logger.error(f"Error loading tips from database: {e}")
        return set()
    finally:
        release_db_connection(conn)

def save_tip(user_id, tier):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tips (user_id, tier) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (user_id, tier)
            )
            conn.commit()
        logger.info(f"Saved tip for user_id: {user_id}, tier: {tier}")
    except Exception as e:
        logger.error(f"Error saving tip to database: {e}")
    finally:
        release_db_connection(conn)

def save_leaderboard_message_id(message_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                ("leaderboard_message_id", str(message_id), str(message_id))
            )
            conn.commit()
        logger.info(f"Saved leaderboard message ID: {message_id}")
    except Exception as e:
        logger.error(f"Error saving leaderboard message ID: {e}")
    finally:
        release_db_connection(conn)

def get_leaderboard_message_id():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", ("leaderboard_message_id",))
            result = cur.fetchone()
            return int(result[0]) if result else None
    except Exception as e:
        logger.error(f"Error retrieving leaderboard message ID: {e}")
        return None
    finally:
        release_db_connection(conn)

# Initialize database and tips
init_db()
SENT_TIPS = load_tips()

# Fetch total wager with retry
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_total_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.now(dt.UTC).isoformat(),
    }
    try:
        response = requests.get(AFFILIATE_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Total Wager API Response: {data}")
        return data
    except requests.RequestException as e:
        logger.error(f"Total Wager API Request Failed: {e}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing Total Wager JSON response: {e}")
        raise

# Fetch weighted wager with retry
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_weighted_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.now(dt.UTC).isoformat(),
        "categories": "slots,provably fair",
        "gameIdentifiers": "-housegames:dice"
    }
    try:
        response = requests.get(AFFILIATE_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Weighted Wager API Response: {data}")
        return data
    except requests.RequestException as e:
        logger.error(f"Weighted Wager API Request Failed: {e}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing Weighted Wager JSON response: {e}")
        raise

# Send tip via Tipping API
def send_tip(user_id, to_username, to_user_id, amount, show_in_chat=True, balance_type="usdt"):
    headers = {"Authorization": f"Bearer {TIPPING_API_TOKEN}"}
    payload = {
        "userId": user_id,
        "toUserName": to_username,
        "toUserId": to_user_id,
        "amount": amount,
        "showInChat": show_in_chat,
        "balanceType": balance_type
    }
    logger.debug(f"Sending tip request for {to_username}: Payload={payload}")
    try:
        response = requests.post(TIPPING_API_URL, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Tip sent to {to_username}: ${amount}")
        return response.json()
    except requests.RequestException as e:
        try:
            error_response = response.json()
            logger.error(f"Tipping API Request Failed for {to_username}: {e}, Response: {error_response}")
        except (ValueError, AttributeError):
            logger.error(f"Tipping API Request Failed for {to_username}: {e}, No valid JSON response")
        return {"success": False, "message": str(e)}

# Process tip queue with 30-second delays
async def process_tip_queue(queue, channel):
    while not queue.empty():
        user_id, username, milestone = await queue.get()
        masked_username = username[:-3] + "***" if len(username) > 3 else "***"
        tier = milestone["tier"]
        tip_amount = milestone["tip"]

        # Check if tip was already sent
        if (user_id, tier) in SENT_TIPS:
            logger.info(f"Skipping duplicate tip for {username} ({tier})")
            queue.task_done()
            continue

        # Send tip
        response = send_tip(ROOBET_USER_ID, username, user_id, tip_amount, show_in_chat=True, balance_type="usdt")
        if response.get("success"):
            # Update database
            SENT_TIPS.add((user_id, tier))
            save_tip(user_id, tier)
            CURRENT_CYCLE_TIPS.add((user_id, tier))
            # Create embed
            embed = discord.Embed(
                title=f"{milestone['emoji']} {tier} Wager Milestone Achieved! {milestone['emoji']}",
                description=(
                    f"🎉 **{masked_username}** has conquered the **{tier} Milestone**!\n"
                    f"✨ **Weighted Wagered**: ${milestone['threshold']:,.2f}\n"
                    f"💸 **Tip Received**: **${tip_amount:.2f} USD**\n"
                    f"Keep rocking the slots! 🚀"
                ),
                color=milestone["color"]
            )
            embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
            embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
            try:
                await channel.send(embed=embed)
                logger.info(f"Sent milestone embed for {username} ({tier})")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in milestone channel.")
        else:
            logger.error(f"Failed to tip {username} for {tier}: {response.get('message')}")

        queue.task_done()
        await asyncio.sleep(30)  # 30-second delay between tips

# Clear tips slash command
@bot.tree.command(
    name="clear_tips",
    description="Clear all milestone tips from the database (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
async def clear_tips(interaction: discord.Interaction):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE tips;")
            conn.commit()
            global SENT_TIPS
            SENT_TIPS = set()
            logger.info("Cleared all milestone tips from database and in-memory set.")
            await interaction.response.send_message("✅ All milestone tips have been cleared from the database.", ephemeral=True)
    except Exception as e:
        logger.error(f"Failed to clear milestone tips: {e}")
        await interaction.response.send_message(f"❌ Error clearing milestone tips: {e}", ephemeral=True)
    finally:
        release_db_connection(conn)

# Sync slash command
@bot.tree.command(
    name="sync",
    description="Sync slash commands and optionally clear old ones (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    clear="Clear all existing guild commands before syncing",
    global_clear="Clear all existing global commands before syncing"
)
async def sync(interaction: discord.Interaction, clear: bool = False, global_clear: bool = False):
    try:
        await interaction.response.defer(ephemeral=True)  # Defer response immediately
        guild = discord.Object(id=GUILD_ID)
        messages = []

        # Clear guild commands if requested
        if clear:
            bot.tree.clear_commands(guild=guild)  # Synchronous, no await
            logger.info(f"Cleared all commands from guild {guild.id}.")
            messages.append("Cleared all guild commands.")
            await asyncio.sleep(1)  # Avoid rate-limiting

        # Clear global commands if requested
        if global_clear:
            bot.tree.clear_commands(guild=None)  # Synchronous, no await
            logger.info("Cleared all global commands.")
            messages.append("Cleared all global commands.")
            await asyncio.sleep(1)  # Avoid rate-limiting

        # Sync commands
        synced = await bot.tree.sync(guild=guild)
        logger.info(f"Synced {len(synced)} commands to guild {guild.id}: {[cmd.name for cmd in synced]}")
        messages.append(f"Synced {len(synced)} commands to the guild: {[cmd.name for cmd in synced]}")

        # Send response
        await interaction.followup.send("\n".join(messages), ephemeral=True)
    except Exception as e:
        logger.error(f"Failed to sync commands: {e}")
        try:
            await interaction.followup.send(f"Error syncing commands: {e}", ephemeral=True)
        except discord.errors.InteractionResponded:
            logger.warning("Interaction already responded, skipping followup.")

# Leaderboard update task
@tasks.loop(minutes=5)
async def update_roobet_leaderboard():
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        logger.error("Leaderboard channel not found.")
        return

    start_date = "2025-05-01T00:00:00"
    end_date = "2025-05-31T23:59:59"

    start_unix = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_unix = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S").timestamp())

    # Fetch data
    logger.info("Fetching leaderboard data...")
    try:
        total_wager_data = fetch_total_wager(start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to fetch total wager data: {e}")
        total_wager_data = []
    try:
        weighted_wager_data = fetch_weighted_wager(start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to fetch weighted wager data: {e}")
        weighted_wager_data = []

    if not weighted_wager_data:
        logger.error("No weighted wager data received from API.")
        try:
            await channel.send("No leaderboard data available at the moment.")
            logger.info("Sent no-data message to leaderboard channel.")
        except discord.errors.Forbidden:
            logger.error("Bot lacks permission to send messages in leaderboard channel.")
        return

    # Create a dictionary for total wagers by UID
    total_wager_dict = {entry.get("uid"): entry.get("wagered", 0) for entry in total_wager_data}

    # Sort weighted wager data
    weighted_wager_data.sort(
        key=lambda x: x.get("weightedWagered", 0) if isinstance(x.get("weightedWagered"), (int, float)) and x.get("weightedWagered") >= 0 else 0,
        reverse=True
    )

    # Create the embed
    embed = discord.Embed(
        title="🏆 **$1,500 USD Roobet Monthly Leaderboard** 🏆",
        description=(
            f"**Leaderboard Period:**\n"
            f"From: <t:{start_unix}:F>\n"
            f"To: <t:{end_unix}:F>\n\n"
            f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
            "📜 **Leaderboard Rules & Disclosure**:\n"
            "• Games with an RTP of **97% or less** contribute **100%** to your weighted wager.\n"
            "• Games with an RTP **above 97%** contribute **50%** to your weighted wager.\n"
            "• Games with an RTP **98% and above** contribute **10%** to your weighted wager.\n"
            "• **Only Slots and House Games count** (Dice is excluded).\n\n"
            "💵 **All amounts displayed are in USD.**\n\n"
        ),
        color=discord.Color.gold()
    )

    # Populate the leaderboard (up to 10 ranks)
    for i in range(10):
        if i < len(weighted_wager_data):
            entry = weighted_wager_data[i]
            username = entry.get("username", "Unknown")
            if len(username) > 3:
                username = username[:-3] + "***"
            else:
                username = "***"
            uid = entry.get("uid")
            total_wagered = total_wager_dict.get(uid, 0) if uid in total_wager_dict else 0
            weighted_wagered = entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) else 0
            prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
        else:
            username = "N/A"
            total_wagered = 0
            weighted_wagered = 0
            prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0

        embed.add_field(
            name=f"**#{i + 1} - {username}**",
            value=(
                f"💰 **Total Wagered**: ${total_wagered:,.2f}\n"
                f"✨ **Weighted Wagered**: ${weighted_wagered:,.2f}\n"
                f"🎁 **Prize**: **${prize} USD**"
            ),
            inline=False
        )

    embed.set_footer(text="All payouts will be made within 24 hours of leaderboard ending.")

    # Update or send the leaderboard message
    message_id = get_leaderboard_message_id()
    logger.info(f"Retrieved leaderboard message ID: {message_id}")
    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(embed=embed)
            logger.info("Leaderboard message updated.")
        except discord.errors.NotFound:
            logger.warning(f"Leaderboard message ID {message_id} not found, sending new message.")
            try:
                message = await channel.send(embed=embed)
                save_leaderboard_message_id(message.id)
                logger.info("New leaderboard message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in leaderboard channel.")
        except discord.errors.Forbidden:
            logger.error("Bot lacks permission to edit messages in leaderboard channel.")
    else:
        logger.info("No leaderboard message ID found, sending new message.")
        try:
            message = await channel.send(embed=embed)
            save_leaderboard_message_id(message.id)
            logger.info("New leaderboard message sent.")
        except discord.errors.Forbidden:
            logger.error("Bot lacks permission to send messages in leaderboard channel.")

# Milestone checking task
milestone_lock = asyncio.Lock()

@tasks.loop(minutes=15)
async def check_wager_milestones():
    global CURRENT_CYCLE_TIPS
    async with milestone_lock:
        channel = bot.get_channel(MILESTONE_CHANNEL_ID)
        if not channel:
            logger.error("Milestone channel not found.")
            return

        # Ensure previous queue is empty
        if hasattr(check_wager_milestones, "tip_queue") and not check_wager_milestones.tip_queue.empty():
            logger.info("Waiting for previous queue to finish.")
            await check_wager_milestones.tip_queue.join()

        # Timestamps (GMT)
        start_date = "2025-05-01T00:00:00"
        end_date = "2025-05-31T23:59:59"

        # Fetch weighted wager data
        try:
            weighted_wager_data = fetch_weighted_wager(start_date, end_date)
        except Exception:
            weighted_wager_data = []
        if not weighted_wager_data:
            logger.error("No weighted wager data received from API.")
            return

        # Create queue for tips
        check_wager_milestones.tip_queue = asyncio.Queue()

        # Check milestones for each user
        for entry in weighted_wager_data:
            user_id = entry.get("uid")
            username = entry.get("username", "Unknown")
            weighted_wagered = entry.get("weightedWagered", 0)
            if not isinstance(weighted_wagered, (int, float)) or weighted_wagered < 0:
                logger.warning(f"Invalid weightedWagered for {username}: {weighted_wagered}")
                continue

            # Check all applicable milestones in order
            for milestone in MILESTONES:
                tier = milestone["tier"]
                threshold = milestone["threshold"]
                if weighted_wagered >= threshold and (user_id, tier) not in CURRENT_CYCLE_TIPS and (user_id, tier) not in SENT_TIPS:
                    await check_wager_milestones.tip_queue.put((user_id, username, milestone))

        # Process tip queue
        if not check_wager_milestones.tip_queue.empty():
            await process_tip_queue(check_wager_milestones.tip_queue, channel)

        # Clear cycle tips for next refresh
        CURRENT_CYCLE_TIPS = set()

@check_wager_milestones.before_loop
async def before_milestone_loop():
    await bot.wait_until_ready()

# Command version for conditional syncing
COMMAND_VERSION = "1.0"

def get_command_version():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", ("command_version",))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error retrieving command version: {e}")
        return None
    finally:
        release_db_connection(conn)

def save_command_version(version):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                ("command_version", version, version)
            )
            conn.commit()
        logger.info(f"Saved command version: {version}")
    except Exception as e:
        logger.error(f"Error saving command version: {e}")
    finally:
        release_db_connection(conn)

@bot.event
async def on_ready():
    last_version = get_command_version()
    try:
        guild = discord.Object(id=GUILD_ID)
        current_commands = await bot.tree.fetch_commands(guild=guild)
        logger.info(f"Current guild commands: {[cmd.name for cmd in current_commands]}")
        if last_version != COMMAND_VERSION or len(current_commands) != 2:  # Expect exactly 2 commands
            bot.tree.clear_commands(guild=guild)  # Synchronous, no await
            logger.info(f"Cleared all commands from guild {guild.id}.")
            await asyncio.sleep(1)  # Avoid rate-limiting
            synced = await bot.tree.sync(guild=guild)
            save_command_version(COMMAND_VERSION)
            logger.info(f"Synced {len(synced)} commands to guild {guild.id}: {[cmd.name for cmd in synced]}")
        else:
            logger.info(f"No sync needed, version {last_version} matches and {len(current_commands)} commands exist.")
    except Exception as e:
        logger.error(f"Failed to sync slash commands: {e}")

    update_roobet_leaderboard.start()
    check_wager_milestones.start()
    logger.info(f"{bot.user.name} is now online and ready!")

@bot.event
async def on_shutdown():
    update_roobet_leaderboard.stop()
    check_wager_milestones.stop()
    if hasattr(check_wager_milestones, "tip_queue"):
        await check_wager_milestones.tip_queue.join()
    db_pool.closeall()
    logger.info("Bot shutting down.")

bot.run(os.getenv("DISCORD_TOKEN"), log_handler=None)
