import discord
from discord import app_commands
from discord.ext import commands
import os
import asyncio

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content

# Define the bot
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    try:
        # Sync commands to make them available as slash commands
        await bot.tree.sync()
        print(f"{bot.user.name} is now online and ready!")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

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

# Run the bot using the token from Heroku's config vars
bot.run(os.getenv("DISCORD_TOKEN"))
