import discord
from discord.ext import commands, tasks
import asyncio
import os
import random
import psycopg2
import requests

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content
bot = commands.Bot(command_prefix="!", intents=intents)

# Roobet API configuration
ROOBET_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
LEADERBOARD_CHANNEL_ID = 1324462489404051487  # Monthly leaderboard channel

# Prizes distribution
PRIZE_DISTRIBUTION = [450, 300, 225, 150, 120, 75, 60, 45, 45, 30]

# Fetch leaderboard data from Roobet API
def fetch_roobet_leaderboard(start_date, end_date):
    headers = {
        "Authorization": f"Bearer {ROOBET_API_TOKEN}"
    }
    params = {
        "startDate": start_date,
        "endDate": end_date
    }
    response = requests.get(ROOBET_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"DEBUG: Failed to fetch leaderboard data. Status code: {response.status_code}")
        return []

# Format and send leaderboard to the channel
@tasks.loop(minutes=1)
async def update_roobet_leaderboard():
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        print("DEBUG: Leaderboard channel not found.")
        return

    # Fetch leaderboard data
    start_date = "2025-01-01T00:00:00"
    end_date = "2025-01-31T23:59:59"
    leaderboard_data = fetch_roobet_leaderboard(start_date, end_date)

    # Sort by weighted wager
    leaderboard_data.sort(key=lambda x: x.get("weightedWagered", 0), reverse=True)

    # Build the embed
    embed = discord.Embed(title="Roobet Monthly Leaderboard", description=f"From {start_date} to {end_date}", color=discord.Color.gold())
    for i, entry in enumerate(leaderboard_data[:10]):
        username = entry.get("username", "Unknown")
        wagered = entry.get("wagered", 0)
        weighted_wagered = entry.get("weightedWagered", 0)
        prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
        embed.add_field(name=f"#{i + 1} - {username}", value=f"Wagered: ${wagered:.2f}\nWeighted: ${weighted_wagered:.2f}\nPrize: ${prize}", inline=False)

    # Send or edit the leaderboard message
    async for message in channel.history(limit=10):
        if message.author == bot.user and message.embeds:
            await message.edit(embed=embed)
            break
    else:
        await channel.send(embed=embed)

@update_roobet_leaderboard.before_loop
async def before_leaderboard_loop():
    await bot.wait_until_ready()

# Sync-Commands Command
@bot.tree.command(name="sync-commands", description="Manually sync commands.")
async def sync_commands(interaction: discord.Interaction):
    synced = await bot.tree.sync()
    await interaction.response.send_message(
        f"Commands synced successfully: {[command.name for command in synced]}",
        ephemeral=True
    )

# Bot Ready Event
@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        print(f"DEBUG: Commands synced successfully: {[command.name for command in synced]}")
        update_roobet_leaderboard.start()
        print("DEBUG: Leaderboard updater started.")
    except Exception as e:
        print(f"DEBUG: Failed to sync commands or start updater: {e}")
    print(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
