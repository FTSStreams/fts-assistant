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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import datetime as dt

# Load environment variables
load_dotenv()

# Validate environment variables
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
VALIDATE_USER_API_URL = "https://roobet.com/_api/affiliate/validateUser"
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
                CREATE TABLE IF NOT EXISTS all_tips (
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    tier TEXT,
                    tipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS pending_tips (
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    tier TEXT NOT NULL,
                    amount NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, tier)
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tips_tipped_at ON tips (tipped_at);
                CREATE INDEX IF NOT EXISTS idx_all_tips_tipped_at ON all_tips (tipped_at);
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

def save_tip(user_id, tier, username, amount):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Save to tips table (milestone tracking)
            cur.execute(
                "INSERT INTO tips (user_id, tier) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (user_id, tier)
            )
            # Save to all_tips table (all tips tracking)
            cur.execute(
                "INSERT INTO all_tips (user_id, username, amount, tier) VALUES (%s, %s, %s, %s);",
                (user_id, username, amount, tier)
            )
            conn.commit()
        logger.info(f"Saved tip for user_id: {user_id}, tier: {tier}, username: {username}, amount: {amount}")
    except Exception as e:
        logger.error(f"Error saving tip to database: {e}")
    finally:
        release_db_connection(conn)

def save_manual_tip(user_id, username, amount):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Save to all_tips table only (no tier for manual tips)
            cur.execute(
                "INSERT INTO all_tips (user_id, username, amount, tier) VALUES (%s, %s, %s, NULL);",
                (user_id, username, amount)
            )
            conn.commit()
        logger.info(f"Saved manual tip for user_id: {user_id}, username: {username}, amount: {amount}")
    except Exception as e:
        logger.error(f"Error saving manual tip to database: {e}")
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def validate_user(username):
    """
    Validate a user by username using the Roobet Affiliate User Validation API and retrieve their userId.
    Falls back to fetch_weighted_wager if userId is not returned.
    
    Args:
        username (str): The Roobet username to validate.
    
    Returns:
        tuple: (user_id, bool) where user_id is the Roobet user ID (or None if not found) and bool indicates if the user is an affiliate.
    """
    # Try validateUser API first
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "username": username,
        "affiliateId": ROOBET_USER_ID
    }
    try:
        response = requests.get(VALIDATE_USER_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"ValidateUser API response for {username}: {data}")
        is_affiliate = data.get("isAffiliate", False)
        user_id = data.get("userId")  # May be None if not included in response
        
        if user_id:
            logger.info(f"Validated user {username} via validateUser: isAffiliate={is_affiliate}, userId={user_id}")
            return user_id, is_affiliate
        elif is_affiliate:
            # Fallback to fetch_weighted_wager if userId is not provided but user is an affiliate
            try:
                wager_data = fetch_weighted_wager("2025-05-01T00:00:00", "2025-05-31T23:59:59")
                user = next((u for u in wager_data if u.get("username").lower() == username.lower()), None)
                if user:
                    user_id = user.get("uid")
                    logger.info(f"Found user {username} in wager data: userId={user_id}")
                    return user_id, True
            except Exception as e:
                logger.error(f"Fallback to fetch_weighted_wager failed for {username}: {e}")
        logger.info(f"Validated user {username} via validateUser: isAffiliate={is_affiliate}, userId={user_id}")
        return None, is_affiliate
    except requests.RequestException as e:
        logger.error(f"Validate User API Request Failed for {username}: {e}")
        return None, False
    except ValueError as e:
        logger.error(f"Error parsing Validate User JSON response: {e}")
        return None, False

async def log_tip(username, amount):
    channel = bot.get_channel(TIP_CONFIRMATION_CHANNEL_ID)
    if not channel:
        logger.error("Tip log channel not found.")
        return
    masked_username = username[:-3] + "***" if len(username) > 3 else "***"
    message = f"✅ A payout of ${amount:.2f} was sent to {masked_username} on Roobet"
    try:
        await channel.send(message)
        logger.info(f"Logged tip to {masked_username}: ${amount}")
    except discord.errors.Forbidden:
        logger.error("Bot lacks permission to send messages in tip log channel.")

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
def send_tip(user_id, to_username, to_user_id, amount, show_in_chat=True, balance_type="usdt"):
    """
    Send a tip to a user via the Roobet Tipping API.
    
    Args:
        user_id (str): Sender's user ID.
        to_username (str): Recipient's username.
        to_user_id (str): Recipient's user ID.
        amount (float): Tip amount in USD.
        show_in_chat (bool): Whether to show the tip in chat.
        balance_type (str): Balance type (e.g., "usdt").
    
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
        logger.info(f"Tip sent to {to_username}: ${amount} ({balance_type})")
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
    while not queue.empty():
        user_id, username, milestone = await queue.get()
        masked_username = username[:-3] + "***" if len(username) > 3 else "***"
        tier = milestone["tier"]
        tip_amount = milestone["tip"]

        # Debug log for milestone tipping
        logger.info(f"Processing milestone tip for user_id: {user_id}, username: {username}, tier: {tier}, amount: {tip_amount}")

        # Check if tip was already sent
        if (user_id, tier) in SENT_TIPS:
            logger.info(f"Skipping duplicate tip for {username} ({tier})")
            queue.task_done()
            continue

        # Save to pending tips
        save_pending_tip(user_id, username, tier, tip_amount)

        # Send tip
        response = send_tip(ROOBET_USER_ID, username, user_id, tip_amount, show_in_chat=True, balance_type="usdt")
        if response.get("success"):
            # Update database
            SENT_TIPS.add((user_id, tier))
            save_tip(user_id, tier, username, tip_amount)
            delete_pending_tip(user_id, tier)
            CURRENT_CYCLE_TIPS.add((user_id, tier))
            # Log tip to tip confirmation channel
            await log_tip(username, tip_amount)
            # Create milestone embed for milestone channel
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

# Manual tip slash command
@bot.tree.command(
    name="tipuser",
    description="Manually tip a user via Roobet API (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    id="The Roobet username of the recipient",
    amount="The tip amount in USD (supports decimals, e.g., 2.85)",
    roobet_id="The Roobet user ID (optional, use if automatic lookup fails)"
)
async def tipuser(interaction: discord.Interaction, id: str, amount: float, roobet_id: str = None):
    if amount <= 0:
        await interaction.response.send_message("❌ Tip amount must be greater than 0.", ephemeral=True)
        return

    # Use provided roobet_id if given, otherwise validate username
    if roobet_id:
        user_id = roobet_id
        is_affiliate = True  # Assume valid if manually provided
    else:
        user_id, is_affiliate = validate_user(id)
        if not is_affiliate:
            await interaction.response.send_message(f"❌ User {id} is not a valid affiliate or does not exist.", ephemeral=True)
            return
        if not user_id:
            await interaction.response.send_message(
                f"❌ Could not retrieve Roobet ID for {id}. The user is a valid affiliate but not found in wager data. "
                f"Please provide the Roobet ID manually using the roobet_id parameter or contact support.",
                ephemeral=True
            )
            return

    # Send tip
    response = send_tip(ROOBET_USER_ID, id, user_id, amount, show_in_chat=True, balance_type="usdt")
    confirmation_channel = bot.get_channel(TIP_CONFIRMATION_CHANNEL_ID)

    if not confirmation_channel:
        logger.error("Tip confirmation channel not found.")
        await interaction.response.send_message("❌ Error: Tip confirmation channel not found.", ephemeral=True)
        return

    masked_username = id[:-3] + "***" if len(id) > 3 else "***"
    if response.get("success"):
        # Save to all_tips
        save_manual_tip(user_id, id, amount)
        # Log tip to tip confirmation channel
        await log_tip(id, amount)
        # Create confirmation embed
        embed = discord.Embed(
            title="💸 Manual Tip Sent! 💸",
            description=(
                f"🎉 **{masked_username}** received a tip!\n"
                f"💰 **Amount**: ${amount:.2f} USD\n"
                f"🆔 **Roobet ID**: {user_id}\n"
                f"Thank you for the generosity! 🚀"
            ),
            color=discord.Color.green()
        )
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        try:
            await confirmation_channel.send(embed=embed)
            await interaction.response.send_message(f"✅ Tip of ${amount:.2f} sent to {masked_username}!", ephemeral=True)
            logger.info(f"Manual tip sent to {id} (ID: {user_id}): ${amount}")
        except discord.errors.Forbidden:
            logger.error("Bot lacks permission to send messages in tip confirmation channel.")
            await interaction.response.send_message("❌ Error: Bot lacks permission to send confirmation message.", ephemeral=True)
    else:
        error_message = response.get("message", "Unknown error")
        await interaction.response.send_message(f"❌ Failed to send tip: {error_message}", ephemeral=True)
        logger.error(f"Failed to send manual tip to {id} (ID: {user_id}): {error_message}")

# Debug user command
@bot.tree.command(
    name="debug_user",
    description="Debug user data from fetch_weighted_wager (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(username="The Roobet username to debug")
async def debug_user(interaction: discord.Interaction, username: str):
    try:
        wager_data = fetch_weighted_wager("2025-05-01T00:00:00", "2025-05-31T23:59:59")
        user = next((u for u in wager_data if u.get("username").lower() == username.lower()), None)
        if user:
            await interaction.response.send_message(
                f"User {username}:\n"
                f"- Username: {user.get('username')}\n"
                f"- User ID: {user.get('uid')}\n"
                f"- Weighted Wagered: {user.get('weightedWagered', 0):.2f}",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(f"User {username} not found in wager data.", ephemeral=True)
    except Exception as e:
        logger.error(f"Debug user failed for {username}: {e}")
        await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

# Total tips slash command
@bot.tree.command(
    name="totaltips",
    description="Show total tips sent over various time periods (admin only)",
    guild=discord.Object(id=GUILD_ID)
)
@app_commands.default_permissions(administrator=True)
async def totaltips(interaction: discord.Interaction):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Total tips in past 24 hours
            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM all_tips WHERE tipped_at >= %s;",
                (datetime.now(dt.UTC) - dt.timedelta(hours=24),)
            )
            total_24h = cur.fetchone()[0] or 0

            # Total tips in past 7 days
            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM all_tips WHERE tipped_at >= %s;",
                (datetime.now(dt.UTC) - dt.timedelta(days=7),)
            )
            total_7d = cur.fetchone()[0] or 0

            # Total tips in past 30 days
            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM all_tips WHERE tipped_at >= %s;",
                (datetime.now(dt.UTC) - dt.timedelta(days=30),)
            )
            total_30d = cur.fetchone()[0] or 0

            # Total tips since May 2, 2025
            cur.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM all_tips WHERE tipped_at >= %s;",
                (datetime(2025, 5, 2, tzinfo=dt.UTC),)
            )
            total_since_may2 = cur.fetchone()[0] or 0

        # Create embed
        embed = discord.Embed(
            title="📊 Total Tips Sent 📊",
            description=(
                f"**Total Tip Amounts Sent (USD):**\n"
                f"🕒 **Past 24 Hours**: ${total_24h:.2f}\n"
                f"📅 **Past 7 Days**: ${total_7d:.2f}\n"
                f"📆 **Past 30 Days**: ${total_30d:.2f}\n"
                f"🏁 **Since May 2, 2025**: ${total_since_may2:.2f}\n"
            ),
            color=discord.Color.blue()
        )
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.response.send_message(embed=embed, ephemeral=True)
        logger.info("Generated total tips report.")
    except Exception as e:
        logger.error(f"Failed to generate total tips report: {e}")
        await interaction.response.send_message(f"❌ Error generating report: {e}", ephemeral=True)
    finally:
        release_db_connection(conn)

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
            cur.execute("TRUNCATE tips; TRUNCATE pending_tips;")  # Note: Does not clear all_tips
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

        # Load pending tips
        check_wager_milestones.tip_queue = asyncio.Queue()
        for user_id, username, tier, amount in load_pending_tips():
            milestone = next((m for m in MILESTONES if m["tier"] == tier), None)
            if milestone and milestone["tip"] == amount:
                await check_wager_milestones.tip_queue.put((user_id, username, milestone))

        # Fetch weighted wager data
        try:
            weighted_wager_data = fetch_weighted_wager(start_date, end_date)
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
