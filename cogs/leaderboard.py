import discord
from discord.ext import commands, tasks
from utils import get_current_month_range
from db import get_leaderboard_message_id, save_leaderboard_message_id, save_announced_goals, load_announced_goals, load_sent_tips
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

# Milestone data from milestones.py
MILESTONES = [
    {"tier": "Rank 1", "threshold": 50, "tip": 1.00, "emoji": "<:rank1:1389367229417656543>"},
    {"tier": "Rank 2", "threshold": 100, "tip": 1.00, "emoji": "<:rank2:1389367231191715970>"},
    {"tier": "Rank 3", "threshold": 150, "tip": 1.00, "emoji": "<:rank3:1389367233507229776>"},
    {"tier": "Rank 4", "threshold": 250, "tip": 1.00, "emoji": "<:rank4:1389367235390472304>"},
    {"tier": "Rank 5", "threshold": 400, "tip": 1.00, "emoji": "<:rank5:1389367237407670394>"},
    {"tier": "Rank 6", "threshold": 600, "tip": 2.00, "emoji": "<:rank6:1389367239161155624>"},
    {"tier": "Rank 7", "threshold": 800, "tip": 2.00, "emoji": "<:rank7:1389367446196060210>"},
    {"tier": "Rank 8", "threshold": 1000, "tip": 2.00, "emoji": "<:rank8:1389367448133697647>"},
    {"tier": "Rank 9", "threshold": 1500, "tip": 3.00, "emoji": "<:rank9:1389367449974997062>"},
    {"tier": "Rank 10", "threshold": 2000, "tip": 3.00, "emoji": "<:rank10:1389367451770294386>"},
    {"tier": "Rank 11", "threshold": 2500, "tip": 3.00, "emoji": "<:rank11:1389367453766909992>"},
    {"tier": "Rank 12", "threshold": 3000, "tip": 3.00, "emoji": "<:rank12:1389367455788564613>"},
    {"tier": "Rank 13", "threshold": 5000, "tip": 11.00, "emoji": "<:rank13:1389367624273498202>"},
    {"tier": "Rank 14", "threshold": 7500, "tip": 14.00, "emoji": "<:rank14:1389367626102210721>"},
    {"tier": "Rank 15", "threshold": 10000, "tip": 14.00, "emoji": "<:rank15:1389367628149166151>"},
    {"tier": "Rank 16", "threshold": 15000, "tip": 27.00, "emoji": "<:rank16:1389367630078545930>"},
    {"tier": "Rank 17", "threshold": 20000, "tip": 27.00, "emoji": "<:rank17:1389367632309784606>"},
    {"tier": "Rank 18", "threshold": 25000, "tip": 27.00, "emoji": "<:rank18:1389367634189095023>"},
    {"tier": "Rank 19", "threshold": 35000, "tip": 54.00, "emoji": "<:rank19:1389367787972985026>"},
    {"tier": "Rank 20", "threshold": 50000, "tip": 81.00, "emoji": "<:rank20:1389367789894238340>"},
    {"tier": "Rank 21", "threshold": 75000, "tip": 135.00, "emoji": "<:rank21:1389367791852716073>"},
    {"tier": "Rank 22", "threshold": 100000, "tip": 149.00, "emoji": "<:rank22:1389367794444931193>"},
    {"tier": "Rank 23", "threshold": 150000, "tip": 270.00, "emoji": "<:rank23:1389367796646940772>"},
    {"tier": "Rank 24", "threshold": 200000, "tip": 270.00, "emoji": "<:rank24:1389367804523708576>"},
    {"tier": "Rank 25", "threshold": 250000, "tip": 270.00, "emoji": "<:rank25:1389367974036504688>"},
    {"tier": "Rank 26", "threshold": 350000, "tip": 540.00, "emoji": "<:rank26:1389367976104558692>"},
    {"tier": "Rank 27", "threshold": 500000, "tip": 810.00, "emoji": "<:rank27:1389367978419683442>"},
    {"tier": "Rank 28", "threshold": 650000, "tip": 810.00, "emoji": "<:rank28:1389367980793659432>"},
    {"tier": "Rank 29", "threshold": 800000, "tip": 810.00, "emoji": "<:rank29:1389367970026754170>"},
    {"tier": "Rank 30", "threshold": 1000000, "tip": 1080.00, "emoji": "<:rank30:1389367972090351626>"}
]

class Leaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        now = datetime.now(dt.UTC)
        year_month = f"{now.year}_{now.month:02d}"
        self.announced_goals = load_announced_goals(year_month)
        self.year_month = year_month
        self.auto_post_monthly_goal.start()
        self.update_roobet_leaderboard.start()
        logger.info("[Leaderboard] Initialized - leaderboard tasks started")

    def get_data_manager(self):
        """Get the DataManager cog"""
        return self.bot.get_cog('DataManager')
    
    def get_milestone_info(self, weighted_wagered):
        """Get milestone rank info for a given weighted wager amount"""
        current_rank = None
        current_rank_index = -1
        
        # Find the highest milestone achieved
        for j, milestone in enumerate(reversed(MILESTONES)):  # Check from highest to lowest
            if weighted_wagered >= milestone["threshold"]:
                current_rank = milestone
                current_rank_index = len(MILESTONES) - 1 - j  # Convert back to normal index
                break
        
        return current_rank, current_rank_index
    
    def calculate_total_tips_for_rank(self, current_rank_index):
        """Calculate the total cumulative tips earned up to a specific rank"""
        if current_rank_index == -1:
            return 0.0
        
        total = 0.0
        for i in range(current_rank_index + 1):  # Include the current rank
            total += MILESTONES[i]["tip"]
        return total
    
    def get_monthly_tips_earned(self, user_id, month, year):
        """Get the total tips earned by a user this month"""
        sent_tips = load_sent_tips(month, year)
        user_tips = [tip_info for (uid, tier), tip_info in sent_tips.items() if uid == user_id]
        
        total_tips = 0.0
        for tip_info in user_tips:
            milestone_tier = tip_info.get('tier', '')
            # Find the tip amount for this tier
            for milestone in MILESTONES:
                if milestone['tier'] == milestone_tier:
                    total_tips += milestone['tip']
                    break
        
        return total_tips

    @tasks.loop(minutes=10)
    async def update_roobet_leaderboard(self):
        logger.info("[Leaderboard] Starting leaderboard update cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        channel = self.bot.get_channel(LEADERBOARD_CHANNEL_ID)
        if not channel:
            logger.error("Leaderboard channel not found.")
            return
        
        # Get data from centralized data manager
        data_manager = self.get_data_manager()
        if not data_manager or not data_manager.is_data_fresh():
            logger.warning("DataManager not available or data not fresh, skipping leaderboard update")
            return
        
        cached_data = data_manager.get_cached_data()
        total_wager_data = cached_data.get('total_wager', [])
        weighted_wager_data = cached_data.get('weighted_wager', [])
        period = cached_data.get('period', {})
        start_date = period.get('start_date')
        end_date = period.get('end_date')
        start_unix = period.get('start_timestamp')
        end_unix = period.get('end_timestamp')
        
        if not weighted_wager_data:
            logger.error("No weighted wager data available from DataManager.")
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
        
        # Get current month for tip calculations
        now = datetime.now(dt.UTC)
        current_month = now.month
        current_year = now.year
        
        leaderboard_lines = []
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
                
                # Get milestone information
                current_rank, current_rank_index = self.get_milestone_info(weighted_wagered)
                monthly_tips = self.get_monthly_tips_earned(uid, current_month, current_year)
                
                if current_rank:
                    rank_emoji = current_rank["emoji"]
                    rank_name = current_rank["tier"]
                    leaderboard_lines.append(
                        f"**#{i + 1} - {rank_emoji} {username} - {rank_name}**\n"
                        f"✨ **Weighted Wagered:** `${weighted_wagered:,.2f}`\n"
                        f"💰 **Total Wagered:** `${total_wagered:,.2f}`\n"
                        f"💸 **Milestone Tips Earned ({current_year}-{current_month:02d}):** `${monthly_tips:.2f}`\n"
                        f"🎁 **Prize:** `${prize} USD`\n"
                    )
                else:
                    leaderboard_lines.append(
                        f"**#{i + 1} - 🎰 {username} - No Rank Yet**\n"
                        f"✨ **Weighted Wagered:** `${weighted_wagered:,.2f}`\n"
                        f"💰 **Total Wagered:** `${total_wagered:,.2f}`\n"
                        f"💸 **Milestone Tips Earned ({current_year}-{current_month:02d}):** `$0.00`\n"
                        f"🎁 **Prize:** `${prize} USD`\n"
                    )
            else:
                leaderboard_lines.append(
                    f"**#{i + 1} - N/A**\n"
                    f"✨ **Weighted Wagered:** `$0.00`\n"
                    f"💰 **Total Wagered:** `$0.00`\n"
                    f"💸 **Milestone Tips Earned ({current_year}-{current_month:02d}):** `$0.00`\n"
                    f"🎁 **Prize:** `${PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0} USD`\n"
                )
        leaderboard_block = '\n'.join(leaderboard_lines)
        embed = discord.Embed(
            title="🏆 **$1,500 USD Roobet Monthly Leaderboard** 🏆",
            description=(
                f"🗓️ **Leaderboard Period:**\n"
                f"From: <t:{start_unix}:F>\n"
                f"To: <t:{end_unix}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "📜 **Leaderboard Rules & Disclosure**:\n"
                "• Games with an RTP of **97% or less** contribute **100%** to your weighted wager.\n"
                "• Games with an RTP **above 97%** contribute **50%** to your weighted wager.\n"
                "• Games with an RTP **98% and above** contribute **10%** to your weighted wager.\n"
                "• **Only Slots and House Games count** (Dice is excluded).\n\n"
                "💵 **All amounts displayed are in USD.**\n\n"
                + leaderboard_block
            ),
            color=discord.Color.gold()
        )
        
        # Update Discord message
        message_id = get_leaderboard_message_id(key="leaderboard_message_id")
        logger.info(f"[Leaderboard] Retrieved leaderboard message ID: {message_id}")
        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
                logger.info("[Leaderboard] Leaderboard message updated.")
            except discord.errors.NotFound:
                logger.warning(f"Leaderboard message ID {message_id} not found, sending new message.")
                try:
                    message = await channel.send(embed=embed)
                    save_leaderboard_message_id(message.id, key="leaderboard_message_id")
                    logger.info("[Leaderboard] New leaderboard message sent.")
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in leaderboard channel.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit messages in leaderboard channel.")
        else:
            logger.info("[Leaderboard] No leaderboard message ID found, sending new message.")
            try:
                message = await channel.send(embed=embed)
                save_leaderboard_message_id(message.id, key="leaderboard_message_id")
                logger.info("[Leaderboard] New leaderboard message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in leaderboard channel.")

    @tasks.loop(minutes=10)
    async def auto_post_monthly_goal(self):
        logger.info("[Leaderboard] Starting monthly goal check cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        channel = self.bot.get_channel(MONTHLY_GOAL_CHANNEL_ID)
        if not channel:
            logger.error("Monthly goal channel not found.")
            return
        
        now = datetime.now(dt.UTC)
        year_month = f"{now.year}_{now.month:02d}"
        if year_month != self.year_month:
            self.announced_goals = set()
            self.year_month = year_month
        
        # Get data from centralized data manager
        data_manager = self.get_data_manager()
        if not data_manager or not data_manager.is_data_fresh():
            logger.warning("DataManager not available or data not fresh, skipping monthly goal update")
            return
        
        try:
            cached_data = data_manager.get_cached_data()
            total_wager_data = cached_data.get('total_wager', [])
            weighted_wager_data = cached_data.get('weighted_wager', [])
            
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
                save_announced_goals(self.announced_goals, self.year_month)
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

    def cog_unload(self):
        self.update_roobet_leaderboard.cancel()
        self.auto_post_monthly_goal.cancel()

async def setup(bot):
    await bot.add_cog(Leaderboard(bot))
