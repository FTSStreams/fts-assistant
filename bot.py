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

# Command: Display the leaderboard
@bot.tree.command(name="leaderboard", description="Display the points leaderboard")
async def leaderboard(interaction: discord.Interaction, page: int = 1):
    limit = 10  # Number of entries per page
    offset = (page - 1) * limit
    leaderboard_data = get_leaderboard(limit, offset)

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

# Command: Reset all points (restricted to the 'Streamer' role)
@bot.tree.command(name="resetpoints", description="Reset all points in the system")
@commands.has_role("Streamer")  # Restrict to users with the 'Streamer' role
async def resetpoints(interaction: discord.Interaction):
    reset_all_points()
    await interaction.response.send_message("All points have been reset.", ephemeral=True)

# Command: Clear messages
@bot.tree.command(name="clear", description="Clears a specified number of messages")
@commands.has_role("Streamer")  # Only users with the 'Streamer' role can use this
async def clear(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(ephemeral=True)

    max_clear_limit = 50
    amount = min(amount, max_clear_limit)

    deleted_count = 0
    while amount > 0:
        delete_count = min(amount, 10)
        deleted_messages = await interaction.channel.purge(limit=delete_count)
        deleted_count += len(deleted_messages)
        amount -= delete_count
        await asyncio.sleep(1)

    await interaction.followup.send(f"Deleted {deleted_count} messages.")

# Coinflip Dropdown
class CoinflipDropdown(discord.ui.Select):
    def __init__(self, amount, user_id):
        self.amount = amount
        self.user_id = user_id
        options = [
            SelectOption(label="Heads", description="Bet on Heads", value="heads"),
            SelectOption(label="Tails", description="Bet on Tails", value="tails"),
        ]
        super().__init__(placeholder="Choose Heads or Tails", options=options)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This is not your bet!", ephemeral=True)
            return

        result = random.choice(["heads", "tails"])

        if self.values[0] == result:
            # User wins: Return original bet + profit
            update_points(str(interaction.user.id), self.amount)
            await interaction.response.send_message(
                f"🎉 You won! The coin landed on **{result.capitalize()}**. "
                f"You gained {self.amount} points and now have {get_points(str(interaction.user.id))} points!"
            )
        else:
            # User loses: Deduct the bet amount
            update_points(str(interaction.user.id), -self.amount)
            await interaction.response.send_message(
                f"😢 You lost! The coin landed on **{result.capitalize()}**. "
                f"You now have {get_points(str(interaction.user.id))} points."
            )


class CoinflipView(discord.ui.View):
    def __init__(self, amount, user_id):
        super().__init__()
        self.add_item(CoinflipDropdown(amount, user_id))

@bot.tree.command(name="coinflip", description="Bet on a coinflip to double your points!")
async def coinflip(interaction: discord.Interaction, amount: int):
    user_id = str(interaction.user.id)
    current_points = get_points(user_id)

    # Validate the bet amount
    if amount <= 0:
        await interaction.response.send_message("Please enter a valid amount greater than 0.", ephemeral=True)
        return

    if current_points < amount:
        await interaction.response.send_message("You don't have enough points to make this bet.", ephemeral=True)
        return

    # Show the coinflip dropdown
    view = CoinflipView(amount, interaction.user.id)
    await interaction.response.send_message(
        f"You bet **{amount} points**. Choose Heads or Tails to start!",
        view=view,
        ephemeral=True
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
        description=f"Prize: **{giveaway_prize}**\nReact with {giveaway_emoji} to join!\n\nHurry! You have 10 minutes to enter.",
        color=discord.Color.gold()
    )
    embed.set_footer(text="Good luck!")
    embed.set_thumbnail(url="https://example.com/image.png")  # Optional

    message = await channel.send(content="@everyone", embed=embed)
    await message.add_reaction(giveaway_emoji)

    await asyncio.sleep(600)
    await end_giveaway(message, giveaway_prize)

async def end_giveaway(message, prize):
    message = await message.channel.fetch_message(message.id)
    reaction = discord.utils.get(message.reactions, emoji=giveaway_emoji)
    
    if reaction and reaction.count > 1:
        users = [user async for user in reaction.users() if not user.bot]
        winner = random.choice(users)
        await message.channel.send(f"The giveaway for **{prize}** is over! Winner: {winner.mention}. Please make a ticket to claim your balance.")
    else:
        await message.channel.send("No one joined the giveaway.")

bot.run(os.getenv("DISCORD_TOKEN"))
