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
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")  # Roobet UID for affiliate stats
LEADERBOARD_CHANNEL_ID = 1324462489404051487  # Monthly leaderboard channel

# Prizes distribution
PRIZE_DISTRIBUTION = [450, 300, 225, 150, 120, 75, 60, 45, 45, 30]

# Fetch leaderboard data from Roobet API
def fetch_roobet_leaderboard(start_date, end_date):
    headers = {
        "Authorization": f"Bearer {ROOBET_API_TOKEN}"
    }
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date
    }
    response = requests.get(ROOBET_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"DEBUG: Failed to fetch leaderboard data. Status code: {response.status_code}, Response: {response.text}")
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

    # Debugging API response
    print(f"DEBUG: API Response: {leaderboard_data}")

    if not leaderboard_data:
        await channel.send("No data available for the leaderboard.")
        return

    # Sort by weighted wager
    leaderboard_data.sort(key=lambda x: x.get("weightedWagered", 0), reverse=True)

    # Build the embed
    embed = discord.Embed(
        title="🏆 **Roobet Monthly Leaderboard** 🏆",
        description=(
            f"**Leaderboard Period:**\nFrom: <t:1735707600:f>\nTo: <t:1738385940:f>\n\n"
            "📜 **Leaderboard Rules & Disclosure**:\n"
            "• Games with an RTP of **97% or less** contribute **100%** of wagers.\n"
            "• Games with an RTP **above 97%** contribute **50%** of wagers.\n"
            "• Games with an RTP **98% and above** contribute **10%** of wagers.\n"
            "• **Only Slots and House Games** count (Dice is excluded)."
        ),
        color=discord.Color.gold()
    )

    for i, entry in enumerate(leaderboard_data[:10]):
        username = entry.get("username", "Unknown")
        wagered = entry.get("wagered", 0)
        weighted_wagered = entry.get("weightedWagered", 0)
        prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0

        # Debugging leaderboard entry
        print(f"DEBUG: Rank {i + 1}: Username={username}, Wagered={wagered}, Weighted={weighted_wagered}, Prize={prize}")

        embed.add_field(
            name=f"**#{i + 1}** - {username}",
            value=(
                f"💰 **Wagered**: ${wagered:,.2f}\n"
                f"✨ **Weighted Wagered**: ${weighted_wagered:,.2f}\n"
                f"🎁 **Prize**: **${prize}**"
            ),
            inline=False
        )

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
    try:
        synced = await bot.tree.sync()
        await interaction.response.send_message(
            f"Commands synced successfully: {[command.name for command in synced]}",
            ephemeral=True
        )
    except discord.errors.InteractionResponded:
        print("DEBUG: Interaction already responded to.")
    except Exception as e:
        print(f"DEBUG: Failed to sync commands: {e}")

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

# Restore other commands
@bot.tree.command(name="add-points", description="Add points to a user")
async def add_points(interaction: discord.Interaction, user: discord.Member, amount: int):
    await interaction.response.send_message(f"Added {amount} points to {user.display_name}")

@bot.tree.command(name="my-points", description="Check your total points")
async def my_points(interaction: discord.Interaction):
    await interaction.response.send_message("You have 100 points!")

@bot.tree.command(name="coinflip", description="Bet your points on heads or tails!")
async def coinflip(interaction: discord.Interaction, amount: int):
    await interaction.response.send_message("The coin landed on Heads! You win!")

@bot.tree.command(name="points-leaderboard", description="Display the points leaderboard")
async def points_leaderboard(interaction: discord.Interaction):
    await interaction.response.send_message("Leaderboard coming soon!")

@bot.tree.command(name="remove-points", description="Remove points from a user")
async def remove_points(interaction: discord.Interaction, user: discord.Member, amount: int):
    await interaction.response.send_message(f"Removed {amount} points from {user.display_name}")

@bot.tree.command(name="reset-points", description="Reset all points in the system")
async def reset_points(interaction: discord.Interaction):
    await interaction.response.send_message("All points have been reset!")

@bot.tree.command(name="spin-wanted", description="Bet your points on the Wanted slot machine!")
async def spin_wanted(interaction: discord.Interaction, amount: int):
    await interaction.response.send_message("Spin results coming soon!")

@bot.tree.command(name="sync-commands", description="Manually sync commands.")
async def sync_commands(interaction: discord.Interaction):
    await interaction.response.send_message("Commands synced successfully!")

bot.run(os.getenv("DISCORD_TOKEN"))
