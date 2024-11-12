import discord
from discord.ext import commands
import os

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

# Define the slash command to clear messages
@bot.tree.command(name="clear", description="Clears a specified number of messages")
@commands.has_role("Streamer")  # Only users with the 'Streamer' role can use this
async def clear(interaction: discord.Interaction, amount: int):
    await interaction.channel.purge(limit=amount)
    await interaction.response.send_message(f"Deleted {amount} messages.", ephemeral=True)

# Run the bot using the token from Heroku's config vars
bot.run(os.getenv("DISCORD_TOKEN"))
