import discord
from discord.ext import commands, tasks
from utils import fetch_total_wager, fetch_weighted_wager, get_current_month_range
from db import get_leaderboard_message_id, save_leaderboard_message_id, save_announced_goals, load_announced_goals
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
LEADERBOARD_CHANNEL_ID = int(os.getenv("LEADERBOARD_CHANNEL_ID"))
MONTHLY_GOAL_CHANNEL_ID = 1036310766300700752
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]
GOAL_THRESHOLDS = [
    25000, 50000, 75000, 100000, 125000, 150000, 175000, 200000, 225000, 250000, 275000, 300000, 325000, 350000, 375000, 400000, 425000, 450000, 475000, 500000
]

class Leaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.announced_goals = load_announced_goals()
        self.auto_post_monthly_goal.start()
        self.update_roobet_leaderboard.start()

    @tasks.loop(minutes=5)
    async def update_roobet_leaderboard(self):
        channel = self.bot.get_channel(LEADERBOARD_CHANNEL_ID)
        if not channel:
            logger.error("Leaderboard channel not found.")
            return
        start_date, end_date = get_current_month_range()
        start_unix = int(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S%z").timestamp())
        end_unix = int(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S%z").timestamp())
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

    @tasks.loop(minutes=5)
    async def auto_post_monthly_goal(self):
        channel = self.bot.get_channel(MONTHLY_GOAL_CHANNEL_ID)
        if not channel:
            logger.error("Monthly goal channel not found.")
            return
        start_date, end_date = get_current_month_range()
        try:
            total_wager_data = fetch_total_wager(start_date, end_date)
            weighted_wager_data = fetch_weighted_wager(start_date, end_date)
            total_wager = sum(
                entry.get("wagered", 0)
                for entry in total_wager_data
                if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
            )
            total_weighted_wager = sum(
                entry.get("weightedWagered", 0)
                for entry in weighted_wager_data
                if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
            )
            crossed = [t for t in GOAL_THRESHOLDS if t <= total_wager and t not in self.announced_goals]
            if crossed:
                threshold = max(crossed)
                self.announced_goals.add(threshold)
                save_announced_goals(self.announced_goals)
                embed = discord.Embed(
                    title="📈 Monthly Wager Stats",
                    description=(
                        f"**TOTAL WAGER THIS MONTH**: ${total_wager:,.2f} USD\n"
                        f"**TOTAL WEIGHTED WAGER THIS MONTH**: ${total_weighted_wager:,.2f} USD"
                    ),
                    color=discord.Color.blue()
                )
                embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
                await channel.send(embed=embed)
                await channel.send(f"🎉 Thanks to @everyone who helped reach ${threshold:,.0f} wager this month. Your support is truly appreciated! 🚀")
        except Exception as e:
            logger.error(f"Error in auto_post_monthly_goal: {e}")

    @update_roobet_leaderboard.before_loop
    @auto_post_monthly_goal.before_loop
    async def before_leaderboard_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Leaderboard(bot, guild=discord.Object(id=GUILD_ID)))
