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

# Remaining commands (checkpoints, leaderboard, coinflip, giveaways, etc.)
# Add them here (shortened for brevity)
# ...

bot.run(os.getenv("DISCORD_TOKEN"))
