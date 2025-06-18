import discord
from discord.ext import commands, tasks
from utils import fetch_weighted_wager, send_tip, get_current_month_range
from db import get_db_connection, release_db_connection, save_tip_log, load_sent_tips, save_tip
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
MILESTONE_CHANNEL_ID = int(os.getenv("MILESTONE_CHANNEL_ID"))
TIP_CONFIRMATION_CHANNEL_ID = int(os.getenv("TIP_CONFIRMATION_CHANNEL_ID"))
MILESTONES = [
    {"tier": "Bronze", "threshold": 500, "tip": 2.85, "color": discord.Color.orange(), "emoji": "🥉"},
    {"tier": "Silver", "threshold": 1000, "tip": 2.85, "color": discord.Color.light_grey(), "emoji": "🥈"},
    {"tier": "Gold", "threshold": 2500, "tip": 8.55, "color": discord.Color.gold(), "emoji": "🥇"},
    {"tier": "Platinum", "threshold": 5000, "tip": 14.25, "color": discord.Color.teal(), "emoji": "💎"},
    {"tier": "Diamond", "threshold": 10000, "tip": 28.50, "color": discord.Color.blue(), "emoji": "💠"},
    {"tier": "Master", "threshold": 25000, "tip": 85.50, "color": discord.Color.purple(), "emoji": "👑"},
    {"tier": "Grand Master", "threshold": 50000, "tip": 142.50, "color": discord.Color.red(), "emoji": "🌟"},
    {"tier": "Legend", "threshold": 100000, "tip": 285.00, "color": discord.Color.green(), "emoji": "🏆"}
]

class Milestones(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tip_queue = asyncio.Queue()
        self.check_wager_milestones.start()
        # process_tip_queue_task will be started in cog_load

    async def cog_load(self):
        self.process_tip_queue_task = asyncio.create_task(self.process_tip_queue())

    async def process_tip_queue(self):
        while True:
            # Always get or fetch the channel each time
            channel = self.bot.get_channel(MILESTONE_CHANNEL_ID)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(MILESTONE_CHANNEL_ID)
                except Exception as e:
                    logger.error(f"Failed to fetch milestone channel: {e}")
                    channel = None
            if channel is None:
                logger.error(f"Milestone channel with ID {MILESTONE_CHANNEL_ID} not found. Cannot send milestone embed.")
                self.tip_queue.task_done()
                await asyncio.sleep(5)
                continue
            user_id, username, milestone, month, year = await self.tip_queue.get()
            bot_user_id = os.getenv("ROOBET_USER_ID")
            tip_response = await send_tip(bot_user_id, username, user_id, milestone["tip"])
            if tip_response.get("success"):
                save_tip(user_id, milestone["tier"], month, year)
                save_tip_log(user_id, username, milestone["tip"], "milestone", month, year)
                embed = discord.Embed(
                    title=f"{milestone['emoji']} {milestone['tier']} Wager Milestone Achieved! {milestone['emoji']}",
                    description=(
                        f"🎉 **{username}** has conquered the **{milestone['tier']} Milestone**!\n"
                        f"✨ **Weighted Wagered**: ${milestone['threshold']:,.2f}\n"
                        f"💸 **Tip Received**: **${milestone['tip']:.2f} USD**\n"
                        f"Keep rocking the slots! 🚀"
                    ),
                    color=milestone["color"]
                )
                embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
                embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
                await channel.send(embed=embed)
            else:
                logger.error(f"Failed to send milestone tip to {username}: {tip_response.get('message')}")
            await asyncio.sleep(30)
            self.tip_queue.task_done()

    @tasks.loop(minutes=15)
    async def check_wager_milestones(self):
        now = datetime.now(dt.UTC)
        month = now.month
        year = now.year
        sent_tips = load_sent_tips(month, year)
        start_date, end_date = get_current_month_range()
        weighted_wager_data = fetch_weighted_wager(start_date, end_date)
        for entry in weighted_wager_data:
            user_id = entry.get("uid")
            username = entry.get("username", "Unknown")
            weighted_wagered = entry.get("weightedWagered", 0)
            if not isinstance(weighted_wagered, (int, float)) or weighted_wagered < 0:
                continue
            for milestone in MILESTONES:
                tier = milestone["tier"]
                threshold = milestone["threshold"]
                if weighted_wagered >= threshold and (user_id, tier) not in sent_tips:
                    await self.tip_queue.put((user_id, username, milestone, month, year))

    @check_wager_milestones.before_loop
    async def before_milestone_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Milestones(bot))
