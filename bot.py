import discord
from discord.ext import commands
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("bot.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Set up the bot
intents = discord.Intents.default()
intents.members = True
intents.message_content = False
bot = commands.Bot(command_prefix="!", intents=intents)

# List of cogs to load
COGS = [
    "cogs.admin",
    "cogs.leaderboard",
    "cogs.milestones",
    "cogs.user"
]

@bot.event
async def on_ready():
    logger.info(f"{bot.user.name} is now online and ready!")
    # Remove all global commands and resync
    await bot.tree.clear_commands(guild=None)
    await bot.tree.sync()
    logger.info("Global slash commands cleared and resynced.")

async def load_cogs():
    for cog in COGS:
        try:
            await bot.load_extension(cog)
            logger.info(f"Loaded cog: {cog}")
        except Exception as e:
            logger.error(f"Failed to load cog {cog}: {e}")

if __name__ == "__main__":
    import asyncio
    async def main():
        await load_cogs()
        await bot.start(os.getenv("DISCORD_TOKEN"))
    asyncio.run(main())
