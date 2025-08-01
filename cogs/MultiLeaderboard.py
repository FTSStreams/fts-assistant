import discord
from discord.ext import commands, tasks
from utils import get_current_month_range
from db import get_leaderboard_message_id, save_leaderboard_message_id
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
MULTI_LEADERBOARD_CHANNEL_ID = int(os.getenv("MULTI_LEADERBOARD_CHANNEL_ID"))  # No default, must be set in env
if not MULTI_LEADERBOARD_CHANNEL_ID:
    raise RuntimeError("MULTI_LEADERBOARD_CHANNEL_ID environment variable must be set!")
PRIZE_DISTRIBUTION = [75, 50, 25, 10, 5]

class MultiLeaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.update_multi_leaderboard.start()

    def get_data_manager(self):
        """Get the DataManager cog"""
        return self.bot.get_cog('DataManager')

    @tasks.loop(minutes=10)
    async def update_multi_leaderboard(self):
        logger.info("[MultiLeaderboard] Starting multiplier leaderboard update cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        channel = self.bot.get_channel(MULTI_LEADERBOARD_CHANNEL_ID)
        if not channel:
            logger.error("MultiLeaderboard channel not found.")
            return
        
        # Get data from centralized data manager
        data_manager = self.get_data_manager()
        if not data_manager or not data_manager.is_data_fresh():
            logger.warning("DataManager not available or data not fresh, skipping multiplier leaderboard update")
            return
        
        cached_data = data_manager.get_cached_data()
        weighted_wager_data = cached_data.get('weighted_wager', [])
        period = cached_data.get('period', {})
        start_date = period.get('start_date')
        end_date = period.get('end_date')
        
        # Filter and sort by highestMultiplier
        multi_data = [entry for entry in weighted_wager_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
        multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
        embed = discord.Embed(
            title="🏆 **Top Multipliers Leaderboard** 🏆",
            description=(
                f"**Leaderboard Period:**\n"
                f"From: <t:{int(datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S%z').timestamp())}:F>\n"
                f"To: <t:{int(datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S%z').timestamp())}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "This leaderboard ranks users by their highest single multiplier hit this month.\n\n"
                "💵 **All amounts displayed are in USD.**\n\n"
            ),
            color=discord.Color.purple()
        )
        for i in range(5):
            if i < len(multi_data):
                entry = multi_data[i]
                username = entry.get("username", "Unknown")
                # Censor username and escape asterisks to prevent Discord markdown issues
                if len(username) > 3:
                    username = username[:-3] + "\\*\\*\\*"
                else:
                    username = "\\*\\*\\*"
                multiplier = entry["highestMultiplier"].get("multiplier", 0)
                game = entry["highestMultiplier"].get("gameTitle", "Unknown")
                game_identifier = entry["highestMultiplier"].get("gameIdentifier", None)
                wagered = entry["highestMultiplier"].get("wagered", 0)
                payout = entry["highestMultiplier"].get("payout", 0)
                prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
            else:
                username = "N/A"
                multiplier = 0
                game = "Unknown"
                game_identifier = None
                wagered = 0
                payout = 0
                prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
            # Hyperlink the game if identifier exists
            if game_identifier:
                game_url = f"https://roobet.com/casino/game/{game_identifier}"
                game_display = f"[{game}]({game_url})"
            else:
                game_display = game
            embed.add_field(
                name=f"**#{i + 1} - {username}**",
                value=(
                    f"💥 **Highest Multiplier:** `x{multiplier:,.2f}`\n"
                    f"🎮 **Game:** {game_display}\n"
                    f"💰 **Payout:** `${payout:,.2f}` (`${wagered:,.2f}` Base Bet)\n"
                    f"🎁 **Prize:** `${prize} USD`"
                ),
                inline=False
            )
        embed.set_footer(text="All payouts will be made within 24 hours of leaderboard ending.")
        
        # Prepare JSON data for export
        leaderboard_json = {
            "leaderboard_type": "multiplier",
            "period": {
                "start": start_date,
                "end": end_date,
                "start_timestamp": int(datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S%z').timestamp()),
                "end_timestamp": int(datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S%z').timestamp())
            },
            "last_updated": datetime.now(dt.UTC).isoformat(),
            "last_updated_timestamp": int(datetime.now(dt.UTC).timestamp()),
            "entries": []
        }
        
        # Add top 5 entries to JSON
        for i in range(5):
            if i < len(multi_data):
                entry = multi_data[i]
                username = entry.get("username", "Unknown")
                # Apply username masking for JSON export too and escape asterisks
                if len(username) > 3:
                    masked_username = username[:-3] + "\\*\\*\\*"
                else:
                    masked_username = "\\*\\*\\*"
                
                leaderboard_json["entries"].append({
                    "rank": i + 1,
                    "username": masked_username,
                    "multiplier": entry["highestMultiplier"].get("multiplier", 0),
                    "game": entry["highestMultiplier"].get("gameTitle", "Unknown"),
                    "game_identifier": entry["highestMultiplier"].get("gameIdentifier", None),
                    "wagered": entry["highestMultiplier"].get("wagered", 0),
                    "payout": entry["highestMultiplier"].get("payout", 0),
                    "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
                })
            else:
                # Add empty slot for consistent structure
                leaderboard_json["entries"].append({
                    "rank": i + 1,
                    "username": "N/A",
                    "multiplier": 0,
                    "game": "Unknown",
                    "game_identifier": None,
                    "wagered": 0,
                    "payout": 0,
                    "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
                })
        
        # All JSON uploads are now handled by DataManager
        # The DataManager will generate and upload multiplier leaderboard data automatically
        
        # Post or update the leaderboard message
        # Use a unique key for the multi leaderboard message
        message_id = get_leaderboard_message_id(key="multi_leaderboard_message_id")
        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
                logger.info("[MultiLeaderboard] Leaderboard message updated.")
            except discord.errors.NotFound:
                logger.warning(f"MultiLeaderboard message ID {message_id} not found, sending new message.")
                try:
                    message = await channel.send(embed=embed)
                    save_leaderboard_message_id(message.id, key="multi_leaderboard_message_id")
                    logger.info("[MultiLeaderboard] New leaderboard message sent.")
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in MultiLeaderboard channel.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit messages in MultiLeaderboard channel.")
        else:
            logger.info("[MultiLeaderboard] No leaderboard message ID found, sending new message.")
            try:
                message = await channel.send(embed=embed)
                save_leaderboard_message_id(message.id, key="multi_leaderboard_message_id")
                logger.info("[MultiLeaderboard] New leaderboard message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in MultiLeaderboard channel.")

    @update_multi_leaderboard.before_loop
    async def before_multi_leaderboard_loop(self):
        await self.bot.wait_until_ready()

    def cog_unload(self):
        self.update_multi_leaderboard.cancel()

async def setup(bot):
    await bot.add_cog(MultiLeaderboard(bot))
