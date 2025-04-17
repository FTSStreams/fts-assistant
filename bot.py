import discord
from discord.ext import commands, tasks
import os
import requests
from datetime import datetime

# Set up the bot with minimal intents
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# Roobet API configuration
ROOBET_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")
LEADERBOARD_CHANNEL_ID = 1324462489404051487

# Prizes distribution ($1,500 total)
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]

# Fetch total wager (all games)
def fetch_total_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.utcnow().isoformat()
    }
    try:
        response = requests.get(ROOBET_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"DEBUG: Total Wager API Response: {data}")
        return data
    except requests.RequestException as e:
        print(f"DEBUG: Total Wager API Request Failed: {e}")
        return []
    except ValueError as e:
        print(f"DEBUG: Error parsing Total Wager JSON response: {e}")
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
        response = requests.get(ROOBET_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"DEBUG: Weighted Wager API Response: {data}")
        return data
    except requests.RequestException as e:
        print(f"DEBUG: Weighted Wager API Request Failed: {e}")
        return []
    except ValueError as e:
        print(f"DEBUG: Error parsing Weighted Wager JSON response: {e}")
        return []

@tasks.loop(minutes=5)
async def update_roobet_leaderboard():
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        print("DEBUG: Leaderboard channel not found.")
        return

    start_date = "2025-04-01T00:00:00"
    end_date = "2025-04-30T23:59:59"

    start_unix = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_unix = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S").timestamp())

    # Fetch data
    total_wager_data = fetch_total_wager(start_date, end_date)
    weighted_wager_data = fetch_weighted_wager(start_date, end_date)

    if not weighted_wager_data:
        print("DEBUG: No weighted wager data received from API.")
        try:
            await channel.send("No leaderboard data available at the moment.")
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the leaderboard channel.")
        return

    # Create a dictionary for total wagers by UID
    total_wager_dict = {entry.get("uid"): entry.get("wagered", 0) for entry in total_wager_data}

    # Sort weighted wager data by weightedWagered
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
                print("DEBUG: Leaderboard message updated.")
                break
            except discord.errors.Forbidden:
                print("DEBUG: Bot doesn't have permission to edit messages in the leaderboard channel.")
    else:
        try:
            await channel.send(embed=embed)
            print("DEBUG: New leaderboard message sent.")
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the leaderboard channel.")

@update_roobet_leaderboard.before_loop
async def before_leaderboard_loop():
    await bot.wait_until_ready()

@bot.event
async def on_ready():
    update_roobet_leaderboard.start()
    print(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
