import discord
from discord import app_commands
from discord.ext import commands, tasks
import os
import asyncio
import random

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content

# Define the bot
bot = commands.Bot(command_prefix="!", intents=intents)

# Emoji for the giveaway
giveaway_emoji = '🆚'
giveaway_prize = "$5.00 RainBet Credit"

@bot.event
async def on_ready():
    try:
        # Sync commands to make them available as slash commands
        await bot.tree.sync()
        print(f"{bot.user.name} is now online and ready!")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

    # Start the flash giveaway scheduler
    flash_giveaway_scheduler.start()

@bot.event
async def on_disconnect():
    print("Bot has disconnected from Discord.")

# Define the slash command to clear messages
@bot.tree.command(name="clear", description="Clears a specified number of messages")
@commands.has_role("Streamer")  # Only users with the 'Streamer' role can use this
async def clear(interaction: discord.Interaction, amount: int):
    # Defer response to avoid timeout
    await interaction.response.defer(ephemeral=True)

    # Set a limit to prevent extremely large purges
    max_clear_limit = 50
    amount = min(amount, max_clear_limit)

    # Break up into smaller chunks if the amount is large
    deleted_count = 0
    while amount > 0:
        delete_count = min(amount, 10)  # Delete in chunks of up to 10
        deleted_messages = await interaction.channel.purge(limit=delete_count)
        deleted_count += len(deleted_messages)
        amount -= delete_count
        await asyncio.sleep(1)  # Short pause to avoid rate limits

    # Send the final confirmation
    await interaction.followup.send(f"Deleted {deleted_count} messages.")

# Flash Giveaway Scheduler
@tasks.loop(hours=24)
async def flash_giveaway_scheduler():
    # Wait for a random time within the next 24 hours
    await asyncio.sleep(random.randint(60, 120))  # Random delay up to 24 hours
    await start_flash_giveaway()

async def start_flash_giveaway():
    # Choose the channel to post the giveaway in (replace with your channel ID)
    channel_id = 1051896276255522938
    channel = bot.get_channel(channel_id)

    # Fancy embed for the giveaway
    embed = discord.Embed(
        title="🎉 FLASH GIVEAWAY 🎉",
        description=f"Prize: **{giveaway_prize}**\nReact with {giveaway_emoji} to join!\n\nHurry! You have 10 minutes to enter.",
        color=discord.Color.gold()
    )
    embed.set_footer(text="Good luck!")
    embed.set_thumbnail(url="https://example.com/image.png")  # Optional: Add a thumbnail for flair

    # Send the giveaway message, tagging everyone, and add the reaction
    message = await channel.send(content="@everyone", embed=embed)
    await message.add_reaction(giveaway_emoji)

    # Wait for the giveaway to end (10 minutes)
    await asyncio.sleep(30)
    await end_giveaway(message, giveaway_prize)

# End Giveaway Function
async def end_giveaway(message, prize):
    # Fetch the message to get updated reactions
    message = await message.channel.fetch_message(message.id)
    reaction = discord.utils.get(message.reactions, emoji=giveaway_emoji)
    
    if reaction and reaction.count > 1:  # Ensure there's at least one participant
        users = [user async for user in reaction.users() if not user.bot]
        winner = random.choice(users)
        await message.channel.send(f"The giveaway for **{prize}** is over! Winner: {winner.mention}. Please make a ticket to claim your balance.")
    else:
        await message.channel.send("No one joined the giveaway.")

# Run the bot using the token from Heroku's config vars
bot.run(os.getenv("DISCORD_TOKEN"))
