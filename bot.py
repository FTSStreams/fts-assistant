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

# Function to get the leaderboard
def get_leaderboard(limit=10, offset=0):
    cur.execute("SELECT user_id, points FROM points ORDER BY points DESC LIMIT %s OFFSET %s", (limit, offset))
    return cur.fetchall()

# Function to reset all points
def reset_all_points():
    cur.execute("TRUNCATE TABLE points")
    conn.commit()

# Cooldown tracking
cooldowns = {}

# Emoji for the giveaway
giveaway_emoji = '🆚'
giveaway_prize = "$5.00 RainBet Credit"

@bot.event
async def on_ready():
    try:
        synced_commands = await bot.tree.sync()
        print(f"Commands synced successfully: {len(synced_commands)} commands")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

    print(f"{bot.user.name} is now online and ready!")

# Event: When a user sends a message
@bot.event
async def on_message(message):
    if message.author.bot:
        return  # Ignore bot messages

    user_id = str(message.author.id)
    now = time.time()
    cooldown_time = 30  # 30 seconds

    # Check if the user is on cooldown
    if user_id in cooldowns and now - cooldowns[user_id] < cooldown_time:
        return  # User is still on cooldown; ignore the message

    # Award points and update cooldown
    update_points(user_id, 1)
    total_points = get_points(user_id)
    cooldowns[user_id] = now  # Update the last point-earned timestamp

    print(f"Awarded 1 point to {message.author.name}. Total: {total_points} points.")
    await bot.process_commands(message)

# Command: Check points
@bot.tree.command(name="checkpoints", description="Check your total points")
async def checkpoints(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    user_points = get_points(user_id)
    await interaction.response.send_message(f"You have **{user_points} points**.", ephemeral=True)

# Command: Add points to a user (restricted to Streamer role)
@bot.tree.command(name="addpoints", description="Add points to a user")
@commands.has_role("Streamer")  # Restrict to users with the 'Streamer' role
async def addpoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("Amount must be greater than 0.", ephemeral=True)
        return

    user_id = str(user.id)
    update_points(user_id, amount)
    updated_points = get_points(user_id)
    await interaction.response.send_message(
        f"Added {amount} points to {user.mention}. They now have {updated_points} points.",
        ephemeral=False
    )

# Command: Remove points from a user (restricted to Streamer role)
@bot.tree.command(name="removepoints", description="Remove points from a user")
@commands.has_role("Streamer")  # Restrict to users with the 'Streamer' role
async def removepoints(interaction: discord.Interaction, user: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("Amount must be greater than 0.", ephemeral=True)
        return

    user_id = str(user.id)
    current_points = get_points(user_id)

    if current_points < amount:
        await interaction.response.send_message(
            f"{user.mention} doesn't have enough points to remove {amount}. They only have {current_points} points.",
            ephemeral=True
        )
        return

    update_points(user_id, -amount)
    updated_points = get_points(user_id)
    await interaction.response.send_message(
        f"Removed {amount} points from {user.mention}. They now have {updated_points} points.",
        ephemeral=False
    )

# Add the rest of your bot’s commands and features here (e.g., /coinflip, leaderboard, etc.)

bot.run(os.getenv("DISCORD_TOKEN"))
