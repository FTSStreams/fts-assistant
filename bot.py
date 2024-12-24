import discord
from discord.ext import commands, tasks
import asyncio
import os
import random
import psycopg2

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content

# Define the bot
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

# Flash Giveaway
@tasks.loop(hours=72)
async def flash_giveaway_scheduler():
    await asyncio.sleep(random.randint(0, 259200))
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

# Slot-Wanted Command (Simplified)
@bot.tree.command(name="slot-wanted", description="Bet your points on the Wanted slot machine!")
async def slot_wanted(interaction: discord.Interaction, amount: int):
    await interaction.response.send_message("The slot is slotting...")

# Checkpoints Command
@bot.tree.command(name="checkpoints", description="Check your total points")
async def checkpoints(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    user_points = get_points(user_id)
    await interaction.response.send_message(f"You have **{user_points} points**.", ephemeral=True)

# Leaderboard Command
@bot.tree.command(name="leaderboard", description="Display the points leaderboard")
async def leaderboard(interaction: discord.Interaction, page: int = 1):
    limit = 10
    offset = (page - 1) * limit
    cur.execute("SELECT user_id, points FROM points ORDER BY points DESC LIMIT %s OFFSET %s", (limit, offset))
    leaderboard_data = cur.fetchall()
    if not leaderboard_data:
        await interaction.response.send_message("No leaderboard data available.", ephemeral=True)
        return
    embed = discord.Embed(title="Points Leaderboard", description=f"Page {page}", color=discord.Color.gold())
    for rank, (user_id, points) in enumerate(leaderboard_data, start=offset + 1):
        user = await bot.fetch_user(int(user_id))
        embed.add_field(name=f"#{rank} - {user.name}", value=f"{points} points", inline=False)
    await interaction.response.send_message(embed=embed)

# Add Points Command
@bot.tree.command(name="addpoints", description="Add points to a user")
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

# Remove Points Command
@bot.tree.command(name="removepoints", description="Remove points from a user")
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

# Reset Points Command
@bot.tree.command(name="resetpoints", description="Reset all points in the system")
async def resetpoints(interaction: discord.Interaction):
    cur.execute("TRUNCATE TABLE points")
    conn.commit()
    await interaction.response.send_message("All points have been reset.", ephemeral=True)

# Manual Sync Command
@bot.tree.command(name="sync", description="Manually sync commands.")
async def sync(interaction: discord.Interaction):
    synced = await bot.tree.sync()
    await interaction.response.send_message(
        f"Commands synced successfully: {[command.name for command in synced]}",
        ephemeral=True
    )

bot.run(os.getenv("DISCORD_TOKEN"))
