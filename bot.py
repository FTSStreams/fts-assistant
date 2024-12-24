import discord
from discord.ext import commands
import os
import psycopg2

# Set up the bot
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL, sslmode="require")
cur = conn.cursor()

# Test /ping command
@bot.tree.command(name="ping", description="Test if the bot is responding.")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong!", ephemeral=True)

# Test /sync command
@bot.tree.command(name="sync", description="Manually sync commands.")
async def sync(interaction: discord.Interaction):
    try:
        synced = await bot.tree.sync()
        await interaction.response.send_message(
            f"Commands synced successfully: {[command.name for command in synced]}",
            ephemeral=True
        )
    except Exception as e:
        await interaction.response.send_message(f"Failed to sync commands: {e}", ephemeral=True)

# Simplified /slot-wanted for testing
@bot.tree.command(name="slot-wanted", description="Test slot-wanted registration.")
async def slot_wanted(interaction: discord.Interaction):
    await interaction.response.send_message("The slot is slotting...")

# Log commands on startup
@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        print(f"DEBUG: Commands synced successfully: {[command.name for command in synced]}")
    except Exception as e:
        print(f"DEBUG: Failed to sync commands: {e}")
    print(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
