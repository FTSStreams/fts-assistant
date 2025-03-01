import discord
from discord.ext import commands, tasks
import asyncio
import os
import random
import psycopg2
import requests
from datetime import datetime, timezone, timedelta
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

    start_date = "2025-03-01T00:00:00"
    end_date = "2025-03-31T23:59:59"

    # Convert to Unix timestamps
    start_unix = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S").timestamp())
    end_unix = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S").timestamp())

    leaderboard_data = fetch_roobet_leaderboard(start_date, end_date)
    if not leaderboard_data:
        print("DEBUG: No data received from API.")
        try:
            await channel.send("No leaderboard data available at the moment.")
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the leaderboard channel.")
        return

    leaderboard_data.sort(key=lambda x: x.get("weightedWagered", 0), reverse=True)
    
    embed = discord.Embed(
        title="🏆 **$1,500 USD Roobet Monthly Leaderboard** 🏆",
        description=(
            f"**Leaderboard Period:**\n"
            f"From: <t:{start_unix}:F>\n"
            f"To: <t:{end_unix}:F>\n\n"
            f"⏰ **Last Updated:** <t:{int(datetime.utcnow().timestamp())}:R>\n\n"
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

# Commands
@bot.tree.command(name="coinflip", description="Bet your points on heads or tails!")
async def coinflip(interaction: discord.Interaction, amount: int, choice: str):
    user_id = str(interaction.user.id)
    current_points = get_points(user_id)

    if amount <= 0 or current_points < amount:
        await interaction.response.send_message("Invalid bet amount.", ephemeral=True)
        return

    choice = choice.lower()
    if choice not in ["heads", "tails"]:
        await interaction.response.send_message("Please choose either 'heads' or 'tails'.", ephemeral=True)
        return

    outcome = random.choice(["heads", "tails"])
    if outcome == choice:
        update_points(user_id, amount)
        await interaction.response.send_message(f"The coin landed on **{outcome.capitalize()}**! You won {amount} points!")
    else:
        update_points(user_id, -amount)
        await interaction.response.send_message(f"The coin landed on **{outcome.capitalize()}**! You lost {amount} points.")

@bot.tree.command(name="my-points", description="Check your total points")
async def my_points(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    user_points = get_points(user_id)
    await interaction.response.send_message(f"You have **{user_points} points**.", ephemeral=True)

@bot.tree.command(name="add-points", description="Add points to a user (Admin only)")
async def add_points(interaction: discord.Interaction, user: discord.Member, amount: int):
    if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
        await interaction.response.send_message("You do not have permission to add points.", ephemeral=True)
        return
    if amount <= 0:
        await interaction.response.send_message("Amount must be greater than zero.", ephemeral=True)
        return
    update_points(str(user.id), amount)
    await interaction.response.send_message(f"Added {amount} points to {user.mention}.")

@bot.tree.command(name="remove-points", description="Remove points from a user (Admin only)")
async def remove_points(interaction: discord.Interaction, user: discord.Member, amount: int):
    if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
        await interaction.response.send_message("You do not have permission to remove points.", ephemeral=True)
        return
    current_points = get_points(str(user.id))
    if amount <= 0 or amount > current_points:
        await interaction.response.send_message(f"Invalid amount. {user.mention} has {current_points} points.", ephemeral=True)
        return
    update_points(str(user.id), -amount)
    await interaction.response.send_message(f"Removed {amount} points from {user.mention}.")

@bot.tree.command(name="reset-points", description="Reset all points (Bot Owner Only)")
async def reset_points(interaction: discord.Interaction):
    if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
        await interaction.response.send_message("You do not have permission to reset points.", ephemeral=True)
        return

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

@bot.tree.command(name="sync-commands", description="Manually sync commands (Admin only)")
async def sync_commands(interaction: discord.Interaction):
    if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
        await interaction.response.send_message("You do not have permission to sync commands.", ephemeral=True)
        return
    synced = await bot.tree.sync()
    await interaction.response.send_message(
        f"Commands synced successfully: {[command.name for command in synced]}",
        ephemeral=True
    )

# New Shop Commands

@bot.tree.command(name="shop", description="Displays all items in the shop")
async def shop(interaction: discord.Interaction):
    cur.execute("SELECT item_id, name, price, quantity FROM shop_items")
    items = cur.fetchall()
    
    if not items:
        await interaction.response.send_message("The shop is currently empty.", ephemeral=True)
        return

    shop_list = []
    for item in items:
        shop_list.append(f"**#{item[0]}** - **{item[1]}**: ${item[2]}, Quantity: {item[3]}")

    await interaction.response.send_message("\n".join(shop_list) or "No items in the shop yet.", ephemeral=True)

@bot.tree.command(name="shop-add", description="Add an item to the shop (Admin only)")
async def shop_add(interaction: discord.Interaction, name: str, price: int, inventory: int):
    if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
        await interaction.response.send_message("You do not have permission to add items to the shop.", ephemeral=True)
        return

    cur.execute("INSERT INTO shop_items (name, price, quantity) VALUES (%s, %s, %s) RETURNING item_id", (name, price, inventory))
    new_item_id = cur.fetchone()[0]
    conn.commit()
    await interaction.response.send_message(f"Added item **{name}** with ID #{new_item_id} to the shop.", ephemeral=True)

@bot.tree.command(name="inventory", description="Check user's inventory")
async def inventory(interaction: discord.Interaction, user: discord.Member = None):
    user_check = user or interaction.user
    user_id = str(user_check.id)
    
    cur.execute("""
    SELECT shop_items.name, inventory.quantity 
    FROM inventory 
    JOIN shop_items ON inventory.item_id = shop_items.item_id 
    WHERE inventory.user_id = %s
    """, (user_id,))
    items = cur.fetchall()
    
    if not items:
        await interaction.response.send_message(f"{user_check.mention}'s inventory is empty.")
        return

    inv_list = [f"**{item[0]}**: {item[1]}" for item in items]
    await interaction.response.send_message(f"{user_check.mention}'s Inventory:\n" + "\n".join(inv_list))

@bot.tree.command(name="buy", description="Buy an item from the shop")
async def buy(interaction: discord.Interaction, product_number: int):
    user_id = str(interaction.user.id)
    current_points = get_points(user_id)
    
    # Fetch item details
    cur.execute("SELECT item_id, name, price, quantity FROM shop_items WHERE item_id = %s", (product_number,))
    item = cur.fetchone()

    if not item:
        await interaction.response.send_message("Item not found in the shop.", ephemeral=True)
        return

    item_id, item_name, price, quantity = item

    if current_points < price:
        await interaction.response.send_message(f"You don't have enough points to buy **{item_name}**.", ephemeral=True)
        return

    if quantity <= 0:
        await interaction.response.send_message(f"**{item_name}** is out of stock.", ephemeral=True)
        return

    # Deduct points
    update_points(user_id, -price)

    # Reduce shop quantity
    cur.execute("UPDATE shop_items SET quantity = quantity - 1 WHERE item_id = %s", (item_id,))
    conn.commit()

    # Add to user's inventory
    cur.execute("""
    INSERT INTO inventory (user_id, item_id, quantity) 
    VALUES (%s, %s, 1) 
    ON CONFLICT (user_id, item_id) 
    DO UPDATE SET quantity = inventory.quantity + 1
    """, (user_id, item_id))
    conn.commit()

    await interaction.response.send_message(f"You've bought **{item_name}** for {price} points. It's now in your inventory!")

# New command for removing items from inventory with buttons

class RemoveItemView(View):
    def __init__(self, user_id, items):
        super().__init__(timeout=60.0)
        for item_name, quantity in items:
            button = Button(label=f"{item_name} (x{quantity})", style=ButtonStyle.gray, custom_id=item_name)
            button.callback = self.remove_item
            self.add_item(button)
        self.user_id = user_id

    async def remove_item(self, interaction: discord.Interaction):
        # Check if the person interacting with the button is the bot owner
        if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
            await interaction.response.send_message("You do not have permission to remove items from inventory.", ephemeral=True)
            return

        item_name = interaction.data['custom_id']
        cur.execute("SELECT item_id FROM shop_items WHERE name = %s", (item_name,))
        item = cur.fetchone()
        if not item:
            await interaction.response.send_message(f"Error: Could not find item **{item_name}** in the shop.", ephemeral=True)
            return

        item_id = item[0]
        cur.execute("SELECT quantity FROM inventory WHERE user_id = %s AND item_id = %s", (self.user_id, item_id))
        quantity = cur.fetchone()

        if not quantity:
            await interaction.response.send_message(f"**{item_name}** not found in the user's inventory.", ephemeral=True)
            return

        # Remove one item from inventory
        new_quantity = quantity[0] - 1
        if new_quantity > 0:
            cur.execute("""
            UPDATE inventory 
            SET quantity = %s 
            WHERE user_id = %s AND item_id = %s
            """, (new_quantity, self.user_id, item_id))
        else:
            cur.execute("""
            DELETE FROM inventory 
            WHERE user_id = %s AND item_id = %s
            """, (self.user_id, item_id))
        conn.commit()
        
        await interaction.response.send_message(f"Removed one **{item_name}** from the inventory.", ephemeral=True)

@bot.tree.command(name="remove-from-inventory", description="Remove item from a user's inventory (Bot Owner Only)")
async def remove_from_inventory(interaction: discord.Interaction, user: discord.Member):
    if interaction.user.id != int(os.getenv("BOT_OWNER_ID", 0)):
        await interaction.response.send_message("You do not have permission to remove items from inventory.", ephemeral=True)
        return
    
    user_id = str(user.id)
    
    # Fetch user's inventory
    cur.execute("""
    SELECT shop_items.name, inventory.quantity 
    FROM inventory 
    JOIN shop_items ON inventory.item_id = shop_items.item_id 
    WHERE inventory.user_id = %s
    """, (user_id,))
    items = cur.fetchall()
    
    if not items:
        await interaction.response.send_message(f"{user.mention}'s inventory is empty.")
        return

    view = RemoveItemView(user_id, items)
    await interaction.response.send_message(f"Choose which item to remove from {user.mention}'s inventory:", view=view)

# Boost Command
# Flash Command
@bot.tree.command(name="boost", description="Start a temporary leaderboard")
async def boost(interaction: Interaction, minutes: int):
    if minutes <= 0:
        await interaction.response.send_message("Please specify a positive number of minutes for the leaderboard duration.", ephemeral=True)
        return

    boost_warning_period = 10  # 10-minute warning before leaderboard starts
    boost_leaderboard_duration = minutes
    boost_processing_time = 30  # Processing time (in minutes) after leaderboard ends before fetching results

    boost_current_time = datetime.now(timezone.utc)  # When the command is triggered (UTC)
    boost_warning_end_time = boost_current_time + timedelta(minutes=boost_warning_period)
    boost_leaderboard_end_time = boost_warning_end_time + timedelta(minutes=boost_leaderboard_duration)
    boost_results_time = boost_leaderboard_end_time + timedelta(minutes=boost_processing_time)

    # 📢 Announcement: Warning before leaderboard starts
    boost_warning_embed = Embed(
        title="🚨 Flash Leaderboard Alert 🚨",
        description=(
            f"@everyone\n**{boost_leaderboard_duration} Minute Flash Leaderboard** starts **<t:{int(boost_warning_end_time.timestamp())}:R>**!\n\n"
            "💰 Get your deposits ready and prepare to climb the ranks! 🏆"
        ),
        color=discord.Color.purple()
    )
    boost_warning_embed.set_footer(text="Powered by FlipTheSwitch")

    try:
        await interaction.channel.send(embed=boost_warning_embed)
    except discord.errors.Forbidden:
        await interaction.response.send_message("The bot doesn't have permission to send messages in this channel.", ephemeral=True)
        return

    await interaction.response.send_message("Flash leaderboard initiated!", ephemeral=True)

    # 🚀 Start the leaderboard sequence in the background, passing boost_current_time along
    asyncio.create_task(handle_boost_leaderboard_timing(
        interaction,
        boost_current_time,
        boost_warning_end_time,
        boost_leaderboard_end_time,
        boost_results_time,
        boost_leaderboard_duration,
        boost_processing_time
    ))

async def handle_boost_leaderboard_timing(interaction: Interaction,
                                          boost_current_time: datetime,
                                          boost_warning_end_time: datetime,
                                          boost_leaderboard_end_time: datetime,
                                          boost_results_time: datetime,
                                          boost_leaderboard_duration: int,
                                          boost_processing_time: int):
    print(f"DEBUG: Waiting for leaderboard warning period to end at {boost_warning_end_time.isoformat()} UTC")
    await asyncio.sleep((boost_warning_end_time - datetime.now(timezone.utc)).total_seconds())

    # 🏁 Start the flash leaderboard
    boost_start_embed = Embed(
        title="🏁 Flash Leaderboard Launch 🚀",
        description=(
            f"🎉 The **{boost_leaderboard_duration} Minute Flash Leaderboard** has officially started!\n\n"
            f"📈 Make your way to the top spot now! 🏅\n\n"
            f"🏁 **Leaderboard ends <t:{int(boost_leaderboard_end_time.timestamp())}:R>.**"
        ),
        color=discord.Color.green()
    )
    boost_start_embed.set_footer(text="Powered by FlipTheSwitch")

    try:
        await interaction.channel.send(embed=boost_start_embed)
    except discord.errors.Forbidden:
        print("DEBUG: Bot doesn't have permission to send messages in the channel.")

    print(f"DEBUG: Waiting for leaderboard duration to end at {boost_leaderboard_end_time.isoformat()} UTC")
    await asyncio.sleep((boost_leaderboard_end_time - datetime.now(timezone.utc)).total_seconds())

    # 🏁 Announce leaderboard closure & start processing timer
    boost_closure_embed = Embed(
        title="🏁 Flash Leaderboard Closed ⏹️",
        description=(
            "The leaderboard has ended! 🎊\n\n"
            f"⏳ **Processing results... Final rankings will be available <t:{int(boost_results_time.timestamp())}:R>.**\n\n"
            "📊 Stay tuned for the winners!"
        ),
        color=discord.Color.red()
    )

    try:
        await interaction.channel.send(embed=boost_closure_embed)
    except discord.errors.Forbidden:
        print("DEBUG: Bot doesn't have permission to send messages in the channel.")

    print(f"DEBUG: Waiting for processing period to end at {boost_results_time.isoformat()} UTC")
    await asyncio.sleep(boost_processing_time * 60)

    # 📡 Fetch the leaderboard data using the recorded start and final times
    boost_start_time_str = boost_current_time.isoformat()  # When the command was triggered
    boost_end_time_str = boost_results_time.isoformat()      # When results are finalized

    print(f"DEBUG: Fetching Flash Leaderboard from {boost_start_time_str} to {boost_end_time_str}")
    
    boost_leaderboard_data = fetch_roobet_leaderboard(boost_start_time_str, boost_end_time_str)

    if not boost_leaderboard_data:
        print("DEBUG: No leaderboard data retrieved. Check API response.")
        boost_no_data_embed = Embed(
            title="📉 No Data Available",
            description="Oops! It looks like there was no activity during this flash leaderboard session. 😕\n\nBetter luck next time! 🍀",
            color=discord.Color.purple()
        )
        try:
            await interaction.channel.send(embed=boost_no_data_embed)
        except discord.errors.Forbidden:
            print("DEBUG: Bot doesn't have permission to send messages in the channel.")
        return

    # 🏆 Sort leaderboard by weighted wager
    sorted_boost_leaderboard = sorted(boost_leaderboard_data, key=lambda x: x.get("weightedWagered", 0), reverse=True)

    # 🏅 Prize distribution structure (Top 3 Winners)
    boost_prize_distribution = [0.03, 0.02, 0.01]  # 🥇 3%, 🥈 2%, 🥉 1%
    top_3_boost_winners = sorted_boost_leaderboard[:3]  # Get the top 3 players

    # 🎉 Create and send final leaderboard results embed
    boost_results_embed = Embed(
        title=f"🏆 {boost_leaderboard_duration} Minute Flash Leaderboard Results 🎉",
        description="Here are the top performers! 🌟\n\n🏆 **Prizes are based on tomorrow’s end stream balance!** 📊",
        color=discord.Color.gold()
    )

    for i, entry in enumerate(top_3_boost_winners):
        username = entry.get("username", "Unknown")
        if len(username) > 3:
            username = username[:-3] + "***"
        else:
            username = "***"

        weighted_wagered = entry.get("weightedWagered", 0)
        percentage_reward = boost_prize_distribution[i] * 100  # Convert to percentage

        boost_results_embed.add_field(
            name=f"**{i + 1}. {username}** 🎖️",
            value=f"✨ Weighted Wagered: **${weighted_wagered:,.2f}** 💸\n🏆 Prize: **{percentage_reward}%** of tomorrow’s stream balance!",
            inline=False
        )

    boost_results_embed.set_footer(text="Final prize value will be calculated after tomorrow’s stream ends.")

    try:
        await interaction.channel.send(embed=boost_results_embed)
        print("DEBUG: Flash leaderboard results posted successfully.")
    except discord.errors.Forbidden:
        print("DEBUG: Bot lacks permission to post the final results.")
    except Exception as e:
        print(f"DEBUG: Error posting flash leaderboard results - {e}")

@bot.event
async def on_ready():
    await bot.tree.sync()
    update_roobet_leaderboard.start()
    print(f"{bot.user.name} is now online and ready!")

bot.run(os.getenv("DISCORD_TOKEN"))
