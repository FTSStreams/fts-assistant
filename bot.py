import discord
from discord.ext import commands, tasks
import os
import requests
import asyncio
from time import time
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from discord import app_commands
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import time as dt

# Load environment variables
load_dotenv()

# Vali environment variables
required_env_vars = ["DISCORD_TOKEN", "ROOBET_API_TOKEN", "TIPPING_API_TOKEN", "ROOBET_USER_ID", "DATABASE_URL", "GUILD_ID", "LEADERBOARD_CHANNEL_ID", "MILESTONE_CHANNEL_ID", "TIP_CONFIRMATION_CHANNEL_ID"]
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
try:
    GUILD_ID = int(os.getenv("GUILD_ID"))
    LEADERBOARD_CHANNEL_ID = int(os.getenv("LEADERBOARD_CHANNEL_ID"))
    MILESTONE_CHANNEL_ID = int(os.getenv("MILESTONE_CHANNEL_ID"))
    TIP_CONFIRMATION_CHANNEL_ID = int(os.getenv("TIP_CONFIRMATION_CHANNEL_ID"))
except (TypeError, ValueError):
    raise ValueError("GUILD_ID, LEADERBOARD_CHANNEL_ID, MILESTONE_CHANNEL_ID, and TIP_CONFIRMATION_CHANNEL_ID must be valid integers")

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
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, os.getenv("DATABASE_URL"))
    if db_pool is None:
        raise psycopg2.Error("Failed to initialize database connection pool")
except psycopg2.Error as e:
    logger.critical(f"Failed to initialize database connection pool: {e}")
    raise

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
                CREATE TABLE IF NOT EXISTS pending_tips (
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, tier)
                );
                CREATE TABLE IF NOT EXISTS tip_logs (
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    tip_type TEXT NOT NULL,
                    tipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tips_tipped_at ON tips (tipped_at);
                CREATE INDEX IF NOT EXISTS idx_tip_logs_tipped_at ON tip_logs (tipped_at);
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

def save_tip_log(user_id, username, amount, tip_type):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tip_logs (user_id, username, amount, tip_type) VALUES (%s, %s, %s, %s);",
                (user_id, username, amount, tip_type)
            )
            conn.commit()
        logger.info(f"Saved tip log for user_id: {user_id}, username: {username}, amount: {amount}, type: {tip_type}")
    except Exception as e:
        logger.error(f"Error saving tip log to database: {e}")
    finally:
        release_db_connection(conn)

def get_tip_stats():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Current time
            now = time.now(dt.UTC)
            # 24 hours ago
            last_24h = now - dt.timedelta(hours=24)
            # 7 days ago
            last_7d = now - dt.timedelta(days=7)
            # 30 days ago
            last_30d = now - dt.timedelta(days=30)
            # Since Jan 1, 2025
            since_jan1 = datetime(2025, 1, 1, tzinfo=dt.UTC)

            # Query for each time range
            cur.execute("""
                SELECT 
                    COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_24h,
                    COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_7d,
                    COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_30d,
                    COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS since_jan1
                FROM tip_logs;
            """, (last_24h, last_7d, last_30d, since_jan1))
            result = cur.fetchone()
            # Add hardcoded $11,295.53 to Lifetime (since Jan 1, 2025)
            return {
                "last_24h": float(result[0]),
                "last_7d": float(result[1]),
                "last_30d": float(result[2]),
                "since_jan1": float(result[3]) + 11295.53
            }
    except Exception as e:
        logger.error(f"Error retrieving tip stats: {e}")
        return {"last_24h": 0.0, "last_7d": 0.0, "last_30d": 0.0, "since_jan1": 11295.53}
    finally:
        release_db_connection(conn)

def save_pending_tip(user_id, username, tier, amount):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pending_tips (user_id, username, tier, amount) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (user_id, username, tier, amount)
            )
            conn.commit()
        logger.info(f"Saved pending tip for user_id: {user_id}, username: {username}, tier: {tier}")
    except Exception as e:
        logger.error(f"Error saving pending tip to database: {e}")
    finally:
        release_db_connection(conn)

def delete_pending_tip(user_id, tier):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pending_tips WHERE user_id = %s AND tier = %s;",
                (user_id, tier)
            )
            conn.commit()
        logger.info(f"Deleted pending tip for user_id: {user_id}, tier: {tier}")
    except Exception as e:
        logger.error(f"Error deleting pending tip from database: {e}")
    finally:
        release_db_connection(conn)

def load_pending_tips():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, username, tier, amount FROM pending_tips;")
            pending_tips = [(row[0], row[1], row[2], row[3]) for row in cur.fetchall()]
        logger.info(f"Loaded {len(pending_tips)} pending tips from database.")
        return pending_tips
    except Exception as e:
        logger.error(f"Error loading pending tips from database: {e}")
        return []
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

# Locks
leaderboard_lock = asyncio.Lock()
milestone_lock = asyncio.Lock()

# Fetch total wager with retry
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_total_wager(start_date, end_date):
    """
    Fetch total wager data from Roobet API for a given date range.
    
    Args:
        start_date (str): Start date in ISO format.
        end_date (str): End date in ISO format.
    
    Returns:
        list: List of wager data entries.
    """
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
        # Validate response structure (adjust based on actual API response)
        data = data.get("data", []) if isinstance(data, dict) else data
        if not isinstance(data, list):
            logger.warning(f"Unexpected total wager response format: {data}")
            return []
        logger.info(f"Total Wager API Response: {len(data)} entries")
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
    """
    Fetch weighted wager data from Roobet API for a given date range.
    
    Args:
        start_date (str): Start date in ISO format.
        end_date (str): End date in ISO format.
    
    Returns:
        list: List of weighted wager data entries.
    """
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
        # Validate response structure
        data = data.get("data", []) if isinstance(data, dict) else data
        if not isinstance(data, list):
            logger.warning(f"Unexpected weighted wager response format: {data}")
            return []
        logger.info(f"Weighted Wager API Response: {len(data)} entries")
        return data
    except requests.RequestException as e:
        logger.error(f"Weighted Wager API Request Failed: {e}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing Weighted Wager JSON response: {e}")
        raise

# Send tip via Tipping API with rate limit retry
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception(lambda e: isinstance(e, requests.HTTPError) and e.response.status_code == 429)
)
def send_tip(user_id, to_username, to_user_id, amount, show_in_chat=True, balance_type="crypto"):
    """
    Send a tip to a user via the Roobet Tipping API.
    
    Args:
        user_id (str): Sender's user ID.
        to_username (str): Recipient's username.
        to_user_id (str): Recipient's user ID.
        amount (float): Tip amount in USD.
        show_in_chat (bool): Whether to show the tip in chat.
        balance_type (str): Balance type (e.g., "crypto").
    
    Returns:
        dict: API response.
    """
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
            logger.error(f"Failed to parse Tipping API response for {to_username}: {e}")
        return {"success": False, "message": str(e)}

# Process tip queue with 30-second delays
async def process_tip_queue(queue, channel):
    confirmation_channel = bot.get_channel(TIP_CONFIRMATION_CHANNEL_ID)
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

        # Save to pending tips
        save_pending_tip(user_id, username, tier, tip_amount)

        # Send tip
        response = send_tip(ROOBET_USER_ID, username, user_id, tip_amount, show_in_chat=True, balance_type="crypto")
        if response.get("success"):
            # Update database
            SENT_TIPS.add((user_id, tier))
            save_tip(user_id, tier)
            save_tip_log(user_id, username, tip_amount, "milestone")
            delete_pending_tip(user_id, tier)
            CURRENT_CYCLE_TIPS.add((user_id, tier))
            # Create embed for milestone channel
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
            # Log to confirmation channel
            if confirmation_channel:
                try:
                    await confirmation_channel.send(f"💰 A payout of ${tip_amount:.2f} was sent to {masked_username}")
                    logger.info(f"Logged milestone tip to confirmation channel for {username} (${tip_amount})")
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in tip confirmation channel.")
            else:
                logger.error("Tip confirmation channel not found.")
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
            cur.execute("TRUNCATE tips; TRUNCATE pending_tips;")
            conn.commit()
            global SENT_TIPS
            SENT_TIPS = set()
            logger.info("Cleared all milestone tips and pending tips from database and in-memory set.")
            await interaction.response.send_message("✅ All milestone tips have been cleared from the database.", ephemeral=True)
    except Exception as e:
        logger.error(f"Failed to clear milestone tips: {e}")
        await interaction.response.send_message(f"❌ Error clearing milestone tips: {e}", ephemeral=True)
    finally:
        release_db_connection(conn)

# Status slash command
@bot.tree.command(
    name="status",
    description="Check bot status (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
async def status(interaction: discord.Interaction):
    db_status = "Connected"
    try:
        conn = get_db_connection()
        release_db_connection(conn)
    except Exception:
        db_status = "Disconnected"
    await interaction.response.send_message(
        f"Bot Status:\n"
        f"- Database: {db_status}\n"
        f"- Leaderboard Task: {'Running' if update_roobet_leaderboard.is_running() else 'Stopped'}\n"
        f"- Milestone Task: {'Running' if check_wager_milestones.is_running() else 'Stopped'}\n"
        f"- Pending Tips: {len(load_pending_tips())}",
        ephemeral=True
    )

# Tipuser slash command
@bot.tree.command(
    name="tipuser",
    description="Manually tip a Roobet user (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    username="The Roobet username of the player",
    userid="The Roobet UID of the player",
    amount="The tip amount in USD (e.g., 5.00)"
)
async def tipuser(interaction: discord.Interaction, username: str, userid: str, amount: float):
    """
    Manually send a tip to a Roobet user.
    Args:
        username: Roobet username of the recipient.
        userid: Roobet UID of the recipient.
        amount: Tip amount in USD.
    """
    # Validate inputs
    if amount <= 0:
        await interaction.response.send_message("❌ Tip amount must be greater than 0.", ephemeral=True)
        logger.error(f"Invalid tip amount: {amount} by {interaction.user}")
        return

    # Defer the response to avoid interaction timeout
    await interaction.response.defer()

    # Send the tip using the existing send_tip function
    logger.info(f"Attempting to send manual tip of ${amount} to {username} (UID: {userid})")
    response = send_tip(
        user_id=ROOBET_USER_ID,
        to_username=username,
        to_user_id=userid,
        amount=amount,
        show_in_chat=True,
        balance_type="crypto"
    )

    confirmation_channel = bot.get_channel(TIP_CONFIRMATION_CHANNEL_ID)
    masked_username = username[:-3] + "***" if len(username) > 3 else "***"

    # Handle the response
    if response.get("success"):
        # Save to tip_logs
        save_tip_log(userid, username, amount, "manual")
        # Send confirmation embed
        embed = discord.Embed(
            title="🎉 Manual Tip Sent!",
            description=(
                f"**{masked_username}** received a tip of **${amount:.2f} USD**!\n"
                f"Sent by: **{interaction.user.display_name}**\n"
                f"Keep shining! ✨"
            ),
            color=discord.Color.green()
        )
        embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed)
        logger.info(f"Manual tip of ${amount} sent to {username} (UID: {userid})")
        # Log to confirmation channel
        if confirmation_channel:
            try:
                await confirmation_channel.send(f"💰 A payout of ${amount:.2f} was sent to {masked_username}")
                logger.info(f"Logged manual tip to confirmation channel for {username} (${amount})")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in tip confirmation channel.")
        else:
            logger.error("Tip confirmation channel not found.")
    else:
        error_message = response.get("message", "Unknown error")
        await interaction.followup.send(
            f"❌ Failed to send tip to {username}: {error_message}", ephemeral=True
        )
        logger.error(f"Failed to send tip to {username} (UID: {userid}): {error_message}")

# Finduid slash command
@bot.tree.command(
    name="finduid",
    description="Find the Roobet UID of a player who wagered in 2025 (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    username="The Roobet username to search for"
)
async def finduid(interaction: discord.Interaction, username: str):
    """
    Find the Roobet UID of a player who wagered under the affiliate code in 2025.
    Args:
        username: Roobet username to search for.
    """
    await interaction.response.defer(ephemeral=True)
    try:
        # Fetch wager data for all of 2025
        start_date = "2025-01-01T00:00:00"
        end_date = "2025-12-31T23:59:59"
        wager_data = fetch_weighted_wager(start_date, end_date)
        
        # Search for the username (case-insensitive, partial match)
        username_lower = username.lower()
        for entry in wager_data:
            entry_username = entry.get("username", "").lower()
            if username_lower in entry_username:
                uid = entry.get("uid")
                if uid:
                    await interaction.followup.send(
                        f"✅ Found UID for {entry.get('username')}: `{uid}`", ephemeral=True
                    )
                    logger.info(f"Found UID {uid} for username {username} requested by {interaction.user}")
                    return
        
        # If no match found
        await interaction.followup.send(
            f"❌ No user found with username '{username}' who wagered in 2025.", ephemeral=True
        )
        logger.info(f"No UID found for username {username} requested by {interaction.user}")
    except Exception as e:
        await interaction.followup.send(
            f"❌ Error searching for UID: {str(e)}", ephemeral=True
        )
        logger.error(f"Error in /finduid for username {username}: {str(e)}")

# Tipstats slash command
@bot.tree.command(
    name="tipstats",
    description="Display tip statistics (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
async def tipstats(interaction: discord.Interaction):
    """
    Display statistics for tips sent in various time ranges.
    """
    await interaction.response.defer()
    stats = get_tip_stats()
    embed = discord.Embed(
        title="📊 Tip Statistics",
        description=(
            f"**Past 24 Hours**: ${stats['last_24h']:.2f} USD\n"
            f"**Past 7 Days**: ${stats['last_7d']:.2f} USD\n"
            f"**Past 30 Days**: ${stats['last_30d']:.2f} USD\n"
            f"**Lifetime (Since Jan. 1st 2025)**: ${stats['since_jan1']:.2f} USD"
        ),
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
    await interaction.followup.send(embed=embed)
    logger.info(f"Tip stats requested by {interaction.user}")
    
# Monthlygoal slash command
@bot.tree.command(
    name="monthlygoal",
    description="Display total wager and weighted wager for the current month",
    guild=discord.Object(id=GUILD_ID)
)
async def monthlygoal(interaction: discord.Interaction):
    """
    Display total wager and total weighted wager for the current month.
    """
    await interaction.response.defer()

    # Define the date range for May 2025
    start_date = "2025-05-01T00:00:00"
    end_date = "2025-05-31T23:59:59"

    try:
        # Fetch total wager data
        total_wager_data = fetch_total_wager(start_date, end_date)
        # Fetch weighted wager data
        weighted_wager_data = fetch_weighted_wager(start_date, end_date)

        # Calculate totals
        total_wager = sum(
            entry.get("wagered", 0) 
            for entry in total_wager_data 
            if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
        )
        total_weighted_wager = sum(
            entry.get("weightedWagered", 0) 
            for entry in weighted_wager_data 
            if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
        )

        # Create embed
        embed = discord.Embed(
            title="📈 Monthly Wager Stats",
            description=(
                f"**TOTAL WAGER THIS MONTH**: ${total_wager:,.2f} USD\n"
                f"**TOTAL WEIGHTED WAGER THIS MONTH**: ${total_weighted_wager:,.2f} USD"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")

        await interaction.followup.send(embed=embed)
        logger.info(f"Monthly goal stats requested by {interaction.user}")
    except Exception as e:
        await interaction.followup.send(
            f"❌ Error retrieving monthly stats: {str(e)}", ephemeral=True
        )
        logger.error(f"Error in /monthlygoal: {str(e)}")
        
# Command error handler
@bot.tree.error
async def on_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    logger.error(f"Command error: {error}")
    try:
        await interaction.response.send_message("An error occurred while processing the command.", ephemeral=True)
    except discord.errors.InteractionResponded:
        await interaction.followup.send("An error occurred while processing the command.", ephemeral=True)

# Leaderboard update task
@tasks.loop(minutes=5)
async def update_roobet_leaderboard():
    async with leaderboard_lock:
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
                f"⏰ **Last Upd:** <t:{int(time.now(dt.UTC).timestamp())}:R>\n\n"
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

        # Up or send the leaderboard message
        message_id = get_leaderboard_message_id()
        logger.info(f"Retrieved leaderboard message ID: {message_id}")
        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
                logger.info("Leaderboard message upd.")
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
        start_ = "2025-05-01T00:00:00"
        end_ = "2025-05-31T23:59:59"

        # Load pending tips
        check_wager_milestones.tip_queue = asyncio.Queue()
        for user_id, username, tier, amount in load_pending_tips():
            milestone = next((m for m in MILESTONES if m["tier"] == tier), None)
            if milestone and milestone["tip"] == amount:
                await check_wager_milestones.tip_queue.put((user_id, username, milestone))

        # Fetch weighted wager data
        try:
            weighted_wager_data = fetch_weighted_wager(start_, end_)
        except Exception:
            weighted_wager_data = []
        if not weighted_wager_data:
            logger.error("No weighted wager data received from API.")
            if not check_wager_milestones.tip_queue.empty():
                await process_tip_queue(check_wager_milestones.tip_queue, channel)
            return

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

@bot.event
async def on_ready():
    try:
        guild = discord.Object(id=GUILD_ID)
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        logger.info(f"Synced {len(synced)} commands to guild {guild.id}: {[cmd.name for cmd in synced]}")
    except Exception as e:
        logger.error(f"Failed to sync slash commands: {e}")

    update_roobet_leaderboard.start()
    check_wager_milestones.start()
    logger.info(f"{bot.user.name} is now online and ready!")

@bot.event
async def on_shutdown():
    logger.info("Shutting down bot...")
    update_roobet_leaderboard.cancel()
    check_wager_milestones.cancel()
    if hasattr(check_wager_milestones, "tip_queue"):
        await check_wager_milestones.tip_queue.join()
    db_pool.closeall()
    await bot.close()
    logger.info("Bot shutdown complete.")

# Run the bot
try:
    bot.run(os.getenv("DISCORD_TOKEN"), log_handler=None)
except Exception as e:
    logger.critical(f"Failed to start bot: {e}")
    raise
