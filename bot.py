import discord
from discord.ext import commands
import os

# Set up the bot with required intents
intents = discord.Intents.default()
intents.message_content = True  # Allows the bot to read message content

# Define bot with command prefix
bot = commands.Bot(command_prefix="/", intents=intents)

@bot.event
async def on_ready():
    print(f'{bot.user.name} is now online and ready!')

# Clear command
@bot.command(name='clear', help="Clears a specified number of messages.")
@commands.has_role("Streamer")  # Only users with the 'Streamer' role can use this
async def clear(ctx, amount: int):
    await ctx.channel.purge(limit=amount)
    await ctx.send(f"Deleted {amount} messages.", delete_after=5)

# Run the bot using the token from environment variables
bot.run(os.getenv("DISCORD_TOKEN"))
