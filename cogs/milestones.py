import discord
from discord.ext import commands, tasks
from utils import fetch_weighted_wager, send_tip
from db import get_db_connection, release_db_connection
import os
import logging
from datetime import datetime
import datetime as dt

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
        self.check_wager_milestones.start()

    @tasks.loop(minutes=15)
    async def check_wager_milestones(self):
        channel = self.bot.get_channel(MILESTONE_CHANNEL_ID)
        if not channel:
            logger.error("Milestone channel not found.")
            return
        start_date = "2025-06-01T00:00:00"
        end_date = "2025-06-30T23:59:59"
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
                if weighted_wagered >= threshold:
                    # Send tip and announce (simplified for brevity)
                    send_tip(user_id, username, user_id, milestone["tip"])
                    embed = discord.Embed(
                        title=f"{milestone['emoji']} {tier} Wager Milestone Achieved! {milestone['emoji']}",
                        description=(
                            f"🎉 **{username}** has conquered the **{tier} Milestone**!\n"
                            f"✨ **Weighted Wagered**: ${milestone['threshold']:,.2f}\n"
                            f"💸 **Tip Received**: **${milestone['tip']:.2f} USD**\n"
                            f"Keep rocking the slots! 🚀"
                        ),
                        color=milestone["color"]
                    )
                    embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
                    embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
                    await channel.send(embed=embed)

    @check_wager_milestones.before_loop
    async def before_milestone_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(Milestones(bot))
