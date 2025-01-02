import discord
from discord.ext import commands, tasks
import asyncio
import os
import random
import psycopg2
import requests
from datetime import datetime, timedelta

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content
bot = commands.Bot(command_prefix="!", intents=intents)

# Connect to the database
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cur = conn.cursor()

# Create the points table if it doesn't exist
cur.execute("""
CREATE TABLE IF NOT EXISTS points (
    user_id TEXT PRIMARY KEY,
    points INTEGER NOT NULL
)
""")
conn.commit()

# Roobet API configuration
ROOBET_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")
LEADERBOARD_CHANNEL_ID = 1324462489404051487

# Prizes distribution
PRIZE_DISTRIBUTION = [450, 300, 225, 150, 120, 75, 60, 45, 45, 30]

# Cooldown for earning points
last_message_time = {}

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    user_id = str(message.author.id)
    current_time = datetime.utcnow()
    cooldown = timedelta(seconds=30)

    if user_id not in last_message_time or current_time - last_message_time[user_id] > cooldown:
        update_points(user_id, 1)
        last_message_time[user_id] = current_time

    await bot.process_commands(message)

def get_points(user_id):
    cur.execute("SELECT points FROM points WHERE user_id = %s", (user_id,))
    result = cur.fetchone()
    return result[0] if result else 0

def update_points(user_id, points_to_add):
    cur.execute("""
    INSERT INTO points (user_id, points) VALUES (%s, %s)
    ON CONFLICT (user_id) DO UPDATE SET points = points.points + EXCLUDED.points
    """, (user_id, points_to_add))
    conn.commit()

# Roobet leaderboard
def fetch_roobet_leaderboard(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {"userId": ROOBET_USER_ID, "startDate": start_date, "endDate": end_date}
    response = requests.get(ROOBET_API_URL, headers=headers, params=params)
    return response.json() if response.status_code == 200 else []

@tasks.loop(minutes=1)
async def update_roobet_leaderboard():
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        return

    start_date = "2025-01-01T00:00:00"
    end_date = "2025-01-31T23:59:59"
    leaderboard_data = fetch_roobet_leaderboard(start_date, end_date)
    leaderboard_data.sort(key=lambda x: x.get("weightedWagered", 0), reverse=True)

    current_unix_time = int(datetime.utcnow().timestamp())  # Current time in UNIX

    embed = discord.Embed(
        title="🏆 **Roobet Monthly Leaderboard** 🏆",
        description=(
            f"**Leaderboard Period:**\nFrom: <t:1735707600:f>\nTo: <t:1738385940:f>\n\n"
            f"⏰ **Last Updated:** <t:{current_unix_time}:f>\n\n"
            "📜 **Leaderboard Rules & Disclosure**:\n"
            "• Games with an RTP of **97% or less** contribute **100%** to your weighted wager.\n"
            "• Games with an RTP **above 97%** contribute **50%** to your weighted wager.\n"
            "• Games with an RTP **98% and above** contribute **10%** to your weighted wager.\n"
            "• **Only Slots and House Games count** (Dice is excluded).\n\n"
            "💵 **All amounts displayed are in USD.**\n\n"
        ),
        color=discord.Color.gold()
    )

    for i, entry in enumerate(leaderboard_data[:10]):
        username = entry.get("username", "Unknown")
        wagered = entry.get("wagered", 0)
        weighted_wagered = entry.get("weightedWagered", 0)
        prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0

        embed.add_field(
            name=f"**#{i + 1} - {username}**",
            value=(
                f"💰 **Wagered**: ${wagered:,.2f}\n"
                f"✨ **Weighted Wagered**: ${weighted_wagered:,.2f}\n"
                f"🎁 **Prize**: **${prize} USD**"
            ),
            inline=False
        )

    # Updated footer
    embed.set_footer(text="All payouts will be made within 24 hours of leaderboard ending.")

    async for message in channel.history(limit=10):
        if message.author == bot.user and message.embeds:
            await message.edit(embed=embed)
            break
    else:
        await channel.send(embed=embed)

@update_roobet_leaderboard.before_loop
async def before_leaderboard_loop():
    await bot.wait_until_ready()

# Commands
@bot.tree.command(name="my-points", description="Check your total points")
async def my_points(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    user_points = get_points(user_id)
    await interaction.response.send_message(f"You have **{user_points} points**.", ephemeral=True)

@bot.tree.command(name="add-points", description="Add points to a user")
async def add_points(interaction: discord.Interaction, user: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("Amount must be greater than zero.", ephemeral=True)
        return
    update_points(str(user.id), amount)
    await interaction.response.send_message(f"Added {amount} points to {user.mention}.")

@bot.tree.command(name="remove-points", description="Remove points from a user")
async def remove_points(interaction: discord.Interaction, user: discord.Member, amount: int):
    current_points = get_points(str(user.id))
    if amount <= 0 or amount > current_points:
        await interaction.response.send_message(f"Invalid amount. {user.mention} has {current_points} points.", ephemeral=True)
        return
    update_points(str(user.id), -amount)
    await interaction.response.send_message(f"Removed {amount} points from {user.mention}.")

@bot.tree.command(name="reset-points", description="Reset all points")
async def reset_points(interaction: discord.Interaction):
    cur.execute("TRUNCATE TABLE points")
    conn.commit()
    await interaction.response.send_message("All points have been reset.")

@bot.tree.command(name="points-leaderboard", description="Show the points leaderboard")
async def points_leaderboard(interaction: discord.Interaction, page: int = 1):
    limit = 10
    offset = (page - 1) * limit
    cur.execute("SELECT user_id, points FROM points ORDER BY points DESC LIMIT %s OFFSET %s", (limit, offset))
    leaderboard_data = cur.fetchall()

    if not leaderboard_data:
        await interaction.response.send_message("No leaderboard data available.", ephemeral=True)
        return

    embed = discord.Embed(title="Points Leaderboard", description=f"Page {page}", color=discord.Color.blue())
    for rank, (user_id, points) in enumerate(leaderboard_data, start=offset + 1):
        user = await bot.fetch_user(int(user_id))
        embed.add_field(name=f"#{rank} - {user.name}", value=f"{points} points", inline=False)

    await interaction.response.send_message(embed=embed)

EMOJIS = [
    "<:outlaw:1320915199619764328>",
    "<:bullshead:1320915198663589888>",
    "<:whiskybottle:1320915512967823404>",
    "<:moneybag:1320915200471466014>",
    "<:revolver:1107173516752719992>"
]

OUTCOMES = [
    {"name": "No Match", "odds": 72, "payout": 0},
    {"name": "3 Outlaws", "odds": 12, "payout": 2},
    {"name": "3 Bull's Heads", "odds": 8, "payout": 3},
    {"name": "3 Whisky Bottles", "odds": 5, "payout": 5},
    {"name": "3 Money Bags", "odds": 2, "payout": 7},
    {"name": "3 Revolvers", "odds": 1, "payout": 10}
]

@bot.tree.command(name="spin-wanted", description="Bet your points on the Wanted slot machine!")
async def spin_wanted(interaction: discord.Interaction, amount: int):
    user_id = str(interaction.user.id)
    current_points = get_points(user_id)

    if amount <= 0 or current_points < amount:
        await interaction.response.send_message("Invalid bet amount.", ephemeral=True)
        return

    rand = random.uniform(0, 100)
    cumulative_probability = 0
    result = None

    for outcome in OUTCOMES:
        cumulative_probability += outcome["odds"]
        if rand <= cumulative_probability:
            result = outcome
            break

    slot_emojis = random.choices(EMOJIS, k=3)
    if result["name"] != "No Match":
        slot_emojis = [EMOJIS[OUTCOMES.index(result) - 1]] * 3

    if result["payout"] == 0:
        update_points(user_id, -amount)
        await interaction.response.send_message(
            f"🎰 {' | '.join(slot_emojis)}\nUnlucky! You lost {amount} points."
        )
    else:
        winnings = amount * result["payout"]
        update_points(user_id, winnings - amount)
        await interaction.response.send_message(
            f"🎰 {' | '.join(slot_emojis)}\n{result['name']}! You win {winnings} points!"
        )

@bot.tree.command(name="sync-commands", description="Manually sync commands.")
async def sync_commands(interaction: discord.Interaction):
    synced = await bot.tree.sync()
    await interaction.response.send_message(
        f"Commands synced successfully: {[command.name for command in synced]}",
        ephemeral=True
    )

@bot.event
async def on_ready():
    await bot.tree.sync()
    update_roobet_leaderboard.start()
    print(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
