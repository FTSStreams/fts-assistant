import discord
from discord.ext import commands, tasks
import asyncio
import os
import time
import random
import psycopg2
from discord import SelectOption

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content

# Define the bot
bot = commands.Bot(command_prefix="!", intents=intents)

# Connect to the database
DATABASE_URL = os.getenv("DATABASE_URL")  # Heroku provides this automatically
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

# Function to get points for a user
def get_points(user_id):
    cur.execute("SELECT points FROM points WHERE user_id = %s", (user_id,))
    result = cur.fetchone()
    return result[0] if result else 0

# Function to update points for a user
def update_points(user_id, points_to_add):
    cur.execute("""
    INSERT INTO points (user_id, points) VALUES (%s, %s)
    ON CONFLICT (user_id) DO UPDATE SET points = points.points + EXCLUDED.points
    """, (user_id, points_to_add))
    conn.commit()

# Slot Machine Settings
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

# Command: Slot Machine
@bot.tree.command(name="slot", description="Bet your points on a slot machine!")
async def slot(interaction: discord.Interaction, amount: int):
    user_id = str(interaction.user.id)
    current_points = get_points(user_id)

    # Validate bet amount
    if amount <= 0:
        await interaction.response.send_message("Please enter a valid bet greater than 0.", ephemeral=True)
        return

    if current_points < amount:
        await interaction.response.send_message("You don't have enough points to make this bet.", ephemeral=True)
        return

    # Generate slot result
    rand = random.uniform(0, 100)
    cumulative_probability = 0
    result = None

    for outcome in OUTCOMES:
        cumulative_probability += outcome["odds"]
        if rand <= cumulative_probability:
            result = outcome
            break

    # Generate random emojis for the slot display
    slot_emojis = random.choices(EMOJIS, k=3)
    if result["name"] != "No Match":  # Ensure 3 of a kind for winning outcomes
        slot_emojis = [EMOJIS[OUTCOMES.index(result) - 1]] * 3

    # Handle payouts or losses
    if result["payout"] == 0:
        update_points(user_id, -amount)  # Deduct bet
        await interaction.response.send_message(
            f"🎰 {' | '.join(slot_emojis)}\n"
            f"Unlucky! Better luck next time! You lost {amount} points."
        )
    else:
        winnings = amount * result["payout"]
        update_points(user_id, winnings - amount)  # Add net winnings
        await interaction.response.send_message(
            f"🎰 {' | '.join(slot_emojis)}\n"
            f"{result['name']}! You win {winnings} points! (Multiplier: {result['payout']}x)"
        )

# Command: Check Points
@bot.tree.command(name="checkpoints", description="Check your total points")
async def checkpoints(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    user_points = get_points(user_id)
    await interaction.response.send_message(f"You have **{user_points} points**.", ephemeral=True)

# Command: Leaderboard
@bot.tree.command(name="leaderboard", description="Display the points leaderboard")
async def leaderboard(interaction: discord.Interaction, page: int = 1):
    limit = 10  # Number of entries per page
    offset = (page - 1) * limit
    cur.execute("SELECT user_id, points FROM points ORDER BY points DESC LIMIT %s OFFSET %s", (limit, offset))
    leaderboard_data = cur.fetchall()

    if not leaderboard_data:
        await interaction.response.send_message("No leaderboard data available.", ephemeral=True)
        return

    embed = discord.Embed(
        title="Points Leaderboard",
        description=f"Page {page}",
        color=discord.Color.gold()
    )
    for rank, (user_id, points) in enumerate(leaderboard_data, start=offset + 1):
        user = await bot.fetch_user(int(user_id))
        embed.add_field(name=f"#{rank} - {user.name}", value=f"{points} points", inline=False)

    await interaction.response.send_message(embed=embed)

# Command: Reset Points
@bot.tree.command(name="resetpoints", description="Reset all points in the system")
@commands.has_role("Streamer")
async def resetpoints(interaction: discord.Interaction):
    cur.execute("TRUNCATE TABLE points")
    conn.commit()
    await interaction.response.send_message("All points have been reset.", ephemeral=True)

# Command: Add Points
@bot.tree.command(name="addpoints", description="Add points to a user")
@commands.has_role("Streamer")
async def addpoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    user_id = str(user.id)
    if amount <= 0:
        await interaction.response.send_message("Please enter a valid amount greater than 0.", ephemeral=True)
        return
    update_points(user_id, amount)
    updated_points = get_points(user_id)
    await interaction.response.send_message(
        f"Added {amount} points to {user.mention}. They now have {updated_points} points.",
        ephemeral=False
    )

# Command: Remove Points
@bot.tree.command(name="removepoints", description="Remove points from a user")
@commands.has_role("Streamer")
async def removepoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    user_id = str(user.id)
    current_points = get_points(user_id)
    if amount <= 0 or amount > current_points:
        await interaction.response.send_message(
            f"Invalid amount. {user.mention} only has {current_points} points.", ephemeral=True
        )
        return
    update_points(user_id, -amount)
    updated_points = get_points(user_id)
    await interaction.response.send_message(
        f"Removed {amount} points from {user.mention}. They now have {updated_points} points.",
        ephemeral=False
    )

# Flash Giveaway Scheduler
@tasks.loop(hours=72)
async def flash_giveaway_scheduler():
    await asyncio.sleep(random.randint(0, 259200))  # Random delay up to 72 hours
    await start_flash_giveaway()

async def start_flash_giveaway():
    channel_id = 1051896276255522938  # Replace with your channel ID
    channel = bot.get_channel(channel_id)

    embed = discord.Embed(
        title="🎉 FLASH GIVEAWAY 🎉",
        description="Prize: **$5.00 RainBet Credit**\nReact with 🆚 to join!\n\nHurry! You have 10 minutes to enter.",
        color=discord.Color.gold()
    )
    embed.set_footer(text="Good luck!")

    message = await channel.send(content="@everyone", embed=embed)
    await message.add_reaction("🆚")
    await asyncio.sleep(600)
    await end_giveaway(message)

async def end_giveaway(message):
    message = await message.channel.fetch_message(message.id)
    reaction = discord.utils.get(message.reactions, emoji="🆚")
    if reaction and reaction.count > 1:
        users = [user async for user in reaction.users() if not user.bot]
        winner = random.choice(users)
        await message.channel.send(f"The giveaway is over! Winner: {winner.mention}.")
    else:
        await message.channel.send("No one joined the giveaway.")

bot.run(os.getenv("DISCORD_TOKEN"))
