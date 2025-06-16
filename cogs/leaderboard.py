import discord
from discord.ext import commands, tasks
from utils import fetch_total_wager, fetch_weighted_wager
from db import get_leaderboard_message_id, save_leaderboard_message_id
import os
import logging
from datetime import datetime
import datetime as dt

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
LEADERBOARD_CHANNEL_ID = int(os.getenv("LEADERBOARD_CHANNEL_ID"))
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]

class Leaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.update_roobet_leaderboard.start()

    @tasks.loop(minutes=5)
    async def update_roobet_leaderboard(self):
        channel = self.bot.get_channel(LEADERBOARD_CHANNEL_ID)
        if not channel:
            logger.error("Leaderboard channel not found.")
            return
        start_date = "2025-06-01T00:00:00"
        end_date = "2025-06-30T23:59:59"
        start_unix = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S").timestamp())
        end_unix = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S").timestamp())
        try:
            total_wager_data = fetch_total_wager(start_date, end_date)
        except Exception as e:
            logger.error(f"Failed to fetch total wager data: {e}")
            total_wager_data = []
        try:
            weighted_wager_data = fetch_weighted_wager(start_date, end_date)
        except Exception as e:
            logger.error(f"Failed to fetch weighted wager data: {e}")
            weighted_wager_data = []
        if not weighted_wager_data:
            logger.error("No weighted wager data received from API.")
            try:
                await channel.send("No leaderboard data available at the moment.")
                logger.info("Sent no-data message to leaderboard channel.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in leaderboard channel.")
            return
        total_wager_dict = {entry.get("uid"): entry.get("wagered", 0) for entry in total_wager_data}
        weighted_wager_data.sort(
            key=lambda x: x.get("weightedWagered", 0) if isinstance(x.get("weightedWagered"), (int, float)) and x.get("weightedWagered") >= 0 else 0,
            reverse=True
        )
        embed = discord.Embed(
            title="🏆 **$1,500 USD Roobet Monthly Leaderboard** 🏆",
            description=(
                f"**Leaderboard Period:**\n"
                f"From: <t:{start_unix}:F>\n"
                f"To: <t:{end_unix}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "📜 **Leaderboard Rules & Disclosure**:\n"
                "• Games with an RTP of **97% or less** contribute **100%** to your weighted wager.\n"
                "• Games with an RTP **above 97%** contribute **50%** to your weighted wager.\n"
                "• Games with an RTP **98% and above** contribute **10%** to your weighted wager.\n"
                "• **Only Slots and House Games count** (Dice is excluded).\n\n"
                "💵 **All amounts displayed are in USD.**\n\n"
            ),
            color=discord.Color.gold()
        )
        for i in range(10):
            if i < len(weighted_wager_data):
                entry = weighted_wager_data[i]
                username = entry.get("username", "Unknown")
                if len(username) > 3:
                    username = username[:-3] + "***"
                else:
                    username = "***"
                uid = entry.get("uid")
                total_wagered = total_wager_dict.get(uid, 0) if uid in total_wager_dict else 0
                weighted_wagered = entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) else 0
                prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
            else:
                username = "N/A"
                total_wagered = 0
                weighted_wagered = 0
                prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
            embed.add_field(
                name=f"**#{i + 1} - {username}**",
                value=(
                    f"💰 **Total Wagered**: ${total_wagered:,.2f}\n"
                    f"✨ **Weighted Wagered**: ${weighted_wagered:,.2f}\n"
                    f"🎁 **Prize**: **${prize} USD**"
                ),
                inline=False
            )
        embed.set_footer(text="All payouts will be made within 24 hours of leaderboard ending.")
        message_id = get_leaderboard_message_id()
        logger.info(f"Retrieved leaderboard message ID: {message_id}")
        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
                logger.info("Leaderboard message updated.")
            except discord.errors.NotFound:
                logger.warning(f"Leaderboard message ID {message_id} not found, sending new message.")
                try:
                    message = await channel.send(embed=embed)
                    save_leaderboard_message_id(message.id)
                    logger.info("New leaderboard message sent.")
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in leaderboard channel.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit messages in leaderboard channel.")
        else:
            logger.info("No leaderboard message ID found, sending new message.")
            try:
                message = await channel.send(embed=embed)
                save_leaderboard_message_id(message.id)
                logger.info("New leaderboard message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in leaderboard channel.")

    @update_roobet_leaderboard.before_loop
    async def before_leaderboard_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Leaderboard(bot))
