import discord
from discord.ext import commands, tasks
import asyncio
import os
import random
import psycopg2
import requests
from datetime import datetime, timedelta
from discord.ui import View, Button
from discord import ButtonStyle, Embed, Interaction

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

# New: Create shop and inventory tables
cur.execute("""
CREATE TABLE IF NOT EXISTS shop_items (
    item_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price INTEGER NOT NULL,
    quantity INTEGER NOT NULL
)
""")
conn.commit()

cur.execute("""
CREATE TABLE IF NOT EXISTS inventory (
    user_id TEXT NOT NULL,
    item_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES points(user_id),
    FOREIGN KEY (item_id) REFERENCES shop_items(item_id),
    PRIMARY KEY (user_id, item_id)
)
""")
conn.commit()

# Roobet API configuration
ROOBET_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")
LEADERBOARD_CHANNEL_ID = 1324462489404051487

# Prizes distribution
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 20, 10, 8, 7, 6, 4]

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
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.utcnow().isoformat()  # Unique timestamp to bypass caching
    }

    try:
        response = requests.get(ROOBET_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"DEBUG: API Request Failed: {e}")
        return []
    
    try:
        return response.json()
    except ValueError as e:
        print(f"DEBUG: Error parsing JSON response: {e}")
        return []

@tasks.loop(minutes=5)
async def update_roobet_leaderboard():
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        print("DEBUG: Leaderboard channel not found.")
        return

    start_date = "2025-02-01T00:00:00"
    end_date = "2025-02-28T23:59:59"
    leaderboard_data = fetch_roobet_leaderboard(start_date, end_date)

    if not leaderboard_data:
        print("DEBUG: No data received from API.")
        try:
            await channel.send("No leaderboard data available at the moment.")
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the leaderboard channel.")
        return

    # Sort leaderboard by weighted wagered
    leaderboard_data.sort(key=lambda x: x.get("weightedWagered", 0), reverse=True)
    print(f"DEBUG: Sorted Leaderboard Data: {leaderboard_data}")

    current_unix_time = int(datetime.utcnow().timestamp())
    embed = discord.Embed(
        title="🏆 **$1,500 USD Roobet Monthly Leaderboard** 🏆",
        description=(
            f"**Leaderboard Period:**\nFrom: \nTo: \n\n"
            f"⏰ **Last Updated:** \n\n"
            "📜 **Leaderboard Rules & Disclosure**:\n"
            "• Games with an RTP of **97% or less** contribute **100%** to your weighted wager.\n"
            "• Games with an RTP **above 97%** contribute **50%** to your weighted wager.\n"
            "• Games with an RTP **98% and above** contribute **10%** to your weighted wager.\n"
            "• **Only Slots and House Games count** (Dice is excluded).\n\n"
            "💵 **All amounts displayed are in USD.**\n\n"
        ),
        color=discord.Color.gold()
    )

    for i, entry in enumerate(leaderboard_data[:15]):
        username = entry.get("username", "Unknown")
        if len(username) > 3:
            username = username[:-3] + "***"
        else:
            username = "***"

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

    embed.set_footer(text="All payouts will be made within 24 hours of leaderboard ending.")

    async for message in channel.history(limit=10):
        if message.author == bot.user and message.embeds:
            try:
                await message.edit(embed=embed)
                break
            except discord.errors.Forbidden:
                print("DEBUG: Bot doesn't have permission to edit messages in the leaderboard channel.")
    else:
        try:
            await channel.send(embed=embed)
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the leaderboard channel.")

@update_roobet_leaderboard.before_loop
async def before_leaderboard_loop():
    await bot.wait_until_ready()

# Rest of the existing commands...

@bot.tree.command(name="boost", description="Start a temporary leaderboard")
async def boost(interaction: Interaction, minutes: int):
    if minutes <= 0:
        await interaction.response.send_message("Please specify a positive number of minutes for the leaderboard duration.", ephemeral=True)
        return

    warning_period = 15  # minutes
    leaderboard_duration = minutes

    # Announcement 15 minutes before the leaderboard starts with an embed
    warning_embed = Embed(
        title="🚨 Flash Leaderboard Alert 🚨",
        description=f"@everyone\n**{leaderboard_duration} Minute Leaderboard** starts <t:{int((datetime.utcnow() + timedelta(minutes=warning_period)).timestamp())}:R>!\n\n💰 Get your deposits ready and prepare to climb the ranks! 🏆",
        color=discord.Color.purple()
    )
    warning_embed.set_thumbnail(url="https://example.com/leaderboard-icon.jpg")  # Replace with your own icon URL
    warning_embed.set_footer(text="Powered by Roobet API")
    try:
        await interaction.channel.send(embed=warning_embed)
    except discord.errors.Forbidden:
        await interaction.response.send_message("The bot doesn't have permission to send messages in this channel.", ephemeral=True)
        return
    
    await interaction.response.send_message("Leaderboard boost initiated!", ephemeral=True)

    # Use a task for timing instead of blocking sleep
    @tasks.loop(count=1)
    async def wait_for_start():
        await asyncio.sleep(warning_period * 60)
        
        start_embed = Embed(
            title="🏁 Leaderboard Launch 🚀",
            description=f"🎉 The **{leaderboard_duration} Minute Leaderboard** has officially started!\n\n📈 Make your way to the top spot now! 🏅",
            color=discord.Color.green()
        )
        start_embed.set_footer(text="Good luck, and may the best player win!")
        try:
            await interaction.channel.send(embed=start_embed)
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the channel.")

    wait_for_start.start()

    # For leaderboard duration and result display, we'll use another task
    @tasks.loop(count=1)
    async def wait_for_end():
        await asyncio.sleep(leaderboard_duration * 60)
        
        closure_embed = Embed(
            title="🏁 Leaderboard Closed ⏹️",
            description=f"The leaderboard has ended! 🎊\n\nResults will be processed and displayed <t:{int((datetime.utcnow() + timedelta(minutes=60)).timestamp())}:R> to ensure all data is up-to-date.\n\nStay tuned! 📊",
            color=discord.Color.red()
        )
        closure_embed.set_footer(text="Thank you for participating!")
        try:
            await interaction.channel.send(embed=closure_embed)
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the channel.")
        
        # Calculate end time as just before displaying results
        start_time = datetime.utcnow() - timedelta(minutes=leaderboard_duration + warning_period)
        end_time = datetime.utcnow() + timedelta(minutes=60)  # End time is now when results are about to be displayed
        
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")

        leaderboard_data = fetch_roobet_leaderboard(start_time_str, end_time_str)

        if not leaderboard_data:
            no_data_embed = Embed(
                title="📉 No Data Available",
                description="Oops! It looks like there was no activity during this leaderboard session. 😕\n\nBetter luck next time! 🍀",
                color=discord.Color.purple()
            )
            try:
                await interaction.channel.send(embed=no_data_embed)
            except discord.errors.Forbidden:
                print("DEBUG: Bot doesn't have permission to send messages in the channel.")
            return

        # Sort leaderboard by weighted wager
        sorted_leaderboard = sorted(leaderboard_data, key=lambda x: x.get("weightedWagered", 0), reverse=True)

        # Create and send embed with leaderboard results
        results_embed = Embed(
            title=f"🏆 {leaderboard_duration} Minute Leaderboard Results 🎉",
            description="Here are the top performers! 🌟\n\nCongratulations to all participants! 🏅",
            color=discord.Color.gold()
        )
        for i, entry in enumerate(sorted_leaderboard[:10]):  # Top 10 or adjust as needed
            username = entry.get("username", "Unknown")
            if len(username) > 3:
                username = username[:-3] + "***"
            else:
                username = "***"
            weighted_wagered = entry.get("weightedWagered", 0)
            results_embed.add_field(
                name=f"**{i + 1}. {username}** 🎖️",
                value=f"✨ Weighted Wagered: **${weighted_wagered:,.2f}** 💸",
                inline=False
            )
        results_embed.set_footer(text="Thanks for playing! More leaderboards coming soon!")
        try:
            await interaction.channel.send(embed=results_embed)
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the channel.")

    wait_for_end.start()

@bot.event
async def on_ready():
    await bot.tree.sync()
    update_roobet_leaderboard.start()
    print(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
