import discord
from discord.ext import commands, tasks
import os
import requests
import asyncio
from datetime import datetime
import logging
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Set up the bot
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# Roobet API configuration
AFFILIATE_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
TIPPING_API_URL = "https://roobet.com/_api/tipping/send"
ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")
LEADERBOARD_CHANNEL_ID = 1324462489404051487
MILESTONE_CHANNEL_ID = 1339413771000614982  # 🔓︱wager-milestone

# Prizes distribution ($1,500 total)
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]

# Milestone tiers (for testing)
MILESTONES = [
    {"tier": "Bronze", "threshold": 5, "tip": 0.10, "color": discord.Color.orange(), "emoji": "🥉"},
    {"tier": "Silver", "threshold": 10, "tip": 0.12, "color": discord.Color.light_grey(), "emoji": "🥈"},
    {"tier": "Gold", "threshold": 15, "tip": 0.15, "color": discord.Color.gold(), "emoji": "🥇"},
    {"tier": "Platinum", "threshold": 20, "tip": 0.20, "color": discord.Color.teal(), "emoji": "💎"},
    {"tier": "Diamond", "threshold": 25, "tip": 0.25, "color": discord.Color.blue(), "emoji": "💠"},
    {"tier": "Master", "threshold": 30, "tip": 0.30, "color": discord.Color.purple(), "emoji": "👑"},
    {"tier": "Grand Master", "threshold": 35, "tip": 0.35, "color": discord.Color.red(), "emoji": "🌟"},
    {"tier": "Legend", "threshold": 40, "tip": 0.40, "color": discord.Color.green(), "emoji": "🏆"}
]

# In-memory tracking
CURRENT_CYCLE_TIPS = set()  # Format: {(user_id, tier)}

# Database functions
def get_db_connection():
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tips (
                        user_id TEXT NOT NULL,
                        tier TEXT NOT NULL,
                        tipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (user_id, tier)
                    );
                """)
                conn.commit()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def load_tips():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, tier FROM tips;")
                tips = {(row[0], row[1]) for row in cur.fetchall()}
        logger.info(f"Loaded {len(tips)} tips from database.")
        return tips
    except Exception as e:
        logger.error(f"Error loading tips from database: {e}")
        return set()

def save_tip(user_id, tier):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tips (user_id, tier) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (user_id, tier)
                )
                conn.commit()
        logger.info(f"Saved tip for user_id: {user_id}, tier: {tier}")
    except Exception as e:
        logger.error(f"Error saving tip to database: {e}")

# Initialize tips
SENT_TIPS = load_tips()

# Fetch total wager (all games and categories)
def fetch_total_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        response = requests.get(AFFILIATE_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Total Wager API Response: {data}")
        return data
    except requests.RequestException as e:
        logger.error(f"Total Wager API Request Failed: {e}")
        return []
    except ValueError as e:
        logger.error(f"Error parsing Total Wager JSON response: {e}")
        return []

# Fetch weighted wager (slots and house games, excluding dice)
def fetch_weighted_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.utcnow().isoformat(),
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
        return []
    except ValueError as e:
        logger.error(f"Error parsing Weighted Wager JSON response: {e}")
        return []

# Send tip via Tipping API
def send_tip(user_id, to_username, to_user_id, amount, show_in_chat=True, balance_type="usdt"):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    payload = {
        "userId": user_id,
        "toUserName": to_username,
        "toUserId": to_user_id,
        "amount": amount,
        "showInChat": show_in_chat,
        "balanceType": balance_type,
        "nonce": str(int(datetime.utcnow().timestamp() * 1000))  # Add nonce as timestamp in milliseconds
    }
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

        # Send tip (pass user_id as to_user_id)
        response = send_tip(ROOBET_USER_ID, username, user_id, tip_amount, show_in_chat=True, balance_type="usdt")
        if response.get("success"):
            # Update database
            SENT_TIPS.add((user_id, tier))
            save_tip(user_id, tier)
            CURRENT_CYCLE_TIPS.add((user_id, tier))
            # Create unique embed
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
            embed.set_thumbnail(url="https://roobet.com/favicon.ico")
            embed.set_footer(text=f"Tipped on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} GMT")
            try:
                await channel.send(embed=embed)
                logger.info(f"Sent milestone embed for {username} ({tier})")
            except discord.errors.Forbidden:
                logger.error("Bot can't send messages in milestone channel.")
        else:
            logger.error(f"Failed to tip {username} for {tier}: {response.get('message')}")

        queue.task_done()
        await asyncio.sleep(30)  # 30-second delay between tips

# Leaderboard update task
@tasks.loop(minutes=5)
async def update_roobet_leaderboard():
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        logger.error("Leaderboard channel not found.")
        return

    start_date = "2025-04-01T00:00:00"
    end_date = "2025-04-30T23:59:59"

    start_unix = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_unix = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S").timestamp())

    # Fetch data
    total_wager_data = fetch_total_wager(start_date, end_date)
    weighted_wager_data = fetch_weighted_wager(start_date, end_date)

    if not weighted_wager_data:
        logger.error("No weighted wager data received from API.")
        try:
            await channel.send("No leaderboard data available at the moment.")
        except discord.errors.Forbidden:
            logger.error("Bot doesn't have permission to send messages in the leaderboard channel.")
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
            f"⏰ **Last Updated:** <t:{int(datetime.utcnow().timestamp())}:R>\n\n"
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
    async for message in channel.history(limit=10):
        if message.author == bot.user and message.embeds:
            try:
                await message.edit(embed=embed)
                logger.info("Leaderboard message updated.")
                break
            except discord.errors.Forbidden:
                logger.error("Bot doesn't have permission to edit messages in the leaderboard channel.")
        else:
            try:
                await channel.send(embed=embed)
                logger.info("New leaderboard message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot doesn't have permission to send messages in the leaderboard channel.")

# Milestone checking task
@tasks.loop(minutes=15)
async def check_wager_milestones():
    global CURRENT_CYCLE_TIPS
    channel = bot.get_channel(MILESTONE_CHANNEL_ID)
    if not channel:
        logger.error("Milestone channel not found.")
        return

    # Ensure previous queue is empty to prevent overlap
    if hasattr(check_wager_milestones, "tip_queue") and not check_wager_milestones.tip_queue.empty():
        logger.info("Waiting for previous queue to finish.")
        await check_wager_milestones.tip_queue.join()

    # Timestamps (GMT)
    start_date = "2025-04-17T06:30:00"  # April 17, 2025, 05:45:00 GMT
    end_date = "2025-04-30T23:59:59"    # April 30, 2025, 23:59:59 GMT

    # Fetch weighted wager data
    weighted_wager_data = fetch_weighted_wager(start_date, end_date)
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
    init_db()  # Initialize database on startup

@bot.event
async def on_ready():
    update_roobet_leaderboard.start()
    check_wager_milestones.start()
    logger.info(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
