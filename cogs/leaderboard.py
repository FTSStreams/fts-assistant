import discord
from discord.ext import commands, tasks
from utils import get_current_month_range, get_month_range, fetch_total_wager, fetch_weighted_wager
from db import get_leaderboard_message_id, save_leaderboard_message_id, save_announced_goals, load_announced_goals, load_sent_tips, get_setting_value, save_setting_value
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio
import calendar
from milestones_config import MILESTONES

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
LEADERBOARD_CHANNEL_ID = int(os.getenv("LEADERBOARD_CHANNEL_ID"))
MONTHLY_GOAL_CHANNEL_ID = 1036310766300700752
WAGER_LEADERBOARD_LOGS_CHANNEL_ID = int(os.getenv("WAGER_LEADERBOARD_LOGS_CHANNEL_ID", "1439815084078792774"))
WAGER_LEADERBOARD_ROLE_CLAIM_CHANNEL_ID = int(os.getenv("WAGER_LEADERBOARD_ROLE_CLAIM_CHANNEL_ID", "1440843895360590028"))
WAGER_LEADERBOARD_PING_ROLE_ID = int(os.getenv("WAGER_LEADERBOARD_PING_ROLE_ID", "1501622029848150178"))
PRIZE_DISTRIBUTION = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]
GOAL_THRESHOLDS = [
    50000, 100000, 150000, 200000, 250000, 300000, 350000, 400000, 450000, 500000,
    550000, 600000, 650000, 700000, 750000, 800000, 850000, 900000, 950000, 1000000
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

    def _mask_public_username(self, username):
        if len(username) > 3:
            return username[:3] + "•••"
        return "•••"

    def _build_monthly_winner_embed(self, winners_data, period_start_unix, period_end_unix, month_label):
        winners_lines = []
        medals = ["🥇", "🥈", "🥉", ":four:", ":five:", ":six:", ":seven:", ":eight:", ":nine:", ":one::zero:"]

        for winner in winners_data:
            medal = medals[winner["rank"] - 1] if winner["rank"] <= len(medals) else f"#{winner['rank']}"
            winners_lines.append(
                f"{medal} **#{winner['rank']}** @{self._mask_public_username(winner['username'])}\n"
                f"⚖️ **Weighted:** ${winner['weighted_wagered']:,.2f} | 💰 **Total:** ${winner['total_wagered']:,.2f}\n"
                f"🎁 **Prize:** ${winner['prize']:,.2f}"
            )

        description = (
            f"**Leaderboard Period:** <t:{period_start_unix}:F> → <t:{period_end_unix}:F>\n"
            f"**Month Closed:** {month_label}\n\n"
            "**Winners:**\n\n"
            + "\n\n".join(winners_lines)
            + f"\n\n📍 **View LB Logs:** <#{WAGER_LEADERBOARD_LOGS_CHANNEL_ID}>\n"
            f"🎭 **Claim your Wager Leaderboard role:** <#{WAGER_LEADERBOARD_ROLE_CLAIM_CHANNEL_ID}>"
        )

        embed = discord.Embed(
            title="🏆 Monthly Wager Leaderboard Results",
            description=description,
            color=discord.Color.gold(),
        )
        embed.set_footer(text="AutoTip Engine • Monthly results snapshot")
        return embed

    async def post_monthly_winner_logs_for_month(self, target_year, target_month, force=False):
        """Post a specific month's winner logs to the configured logs channel."""
        if not WAGER_LEADERBOARD_LOGS_CHANNEL_ID:
            logger.warning("[Leaderboard] WAGER_LEADERBOARD_LOGS_CHANNEL_ID not configured")
            return False

        logs_channel = self.bot.get_channel(WAGER_LEADERBOARD_LOGS_CHANNEL_ID)
        if not logs_channel:
            logger.warning(f"[Leaderboard] Wager logs channel {WAGER_LEADERBOARD_LOGS_CHANNEL_ID} not found")
            return False

        target_key = f"{target_year}-{target_month:02d}"

        if not force:
            last_posted = get_setting_value("wager_lb_last_logged_month", default="")
            if last_posted == target_key:
                return False

        start_date, end_date = get_month_range(target_year, target_month)
        logger.info(f"[Leaderboard] Building monthly winner logs for {target_key}: {start_date} -> {end_date}")

        try:
            total_wager_data = await asyncio.to_thread(fetch_total_wager, start_date, end_date)
            weighted_wager_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
        except Exception as e:
            logger.error(f"[Leaderboard] Failed to fetch monthly winner log data for {target_key}: {e}")
            return False

        if not weighted_wager_data:
            logger.warning(f"[Leaderboard] No weighted data for {target_key}, skipping logs post")
            return False

        total_wager_dict = {entry.get("uid"): entry.get("wagered", 0) for entry in total_wager_data}
        weighted_wager_data.sort(
            key=lambda x: x.get("weightedWagered", 0) if isinstance(x.get("weightedWagered"), (int, float)) and x.get("weightedWagered") >= 0 else 0,
            reverse=True
        )

        winners_data = []
        for i in range(min(10, len(weighted_wager_data))):
            entry = weighted_wager_data[i]
            uid = entry.get("uid")
            winners_data.append({
                "rank": i + 1,
                "username": entry.get("username", "Unknown"),
                "weighted_wagered": entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) else 0,
                "total_wagered": total_wager_dict.get(uid, 0) if uid in total_wager_dict else 0,
                "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0,
            })

        period_start_unix = int(datetime.fromisoformat(start_date.replace("Z", "+00:00")).timestamp())
        period_end_unix = int(datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp())
        month_label = f"{calendar.month_name[target_month]} {target_year}"

        embed = self._build_monthly_winner_embed(
            winners_data,
            period_start_unix=period_start_unix,
            period_end_unix=period_end_unix,
            month_label=month_label,
        )

        ping_content = f"<@&{WAGER_LEADERBOARD_PING_ROLE_ID}>" if WAGER_LEADERBOARD_PING_ROLE_ID else None
        await logs_channel.send(content=ping_content, embed=embed)
        save_setting_value("wager_lb_last_logged_month", target_key)
        logger.info(f"[Leaderboard] Posted monthly winner logs for {target_key} to channel {WAGER_LEADERBOARD_LOGS_CHANNEL_ID}")
        return True

    async def maybe_post_monthly_winner_logs(self):
        """Post previous month's top 10 leaderboard winners once per month."""
        now = datetime.now(dt.UTC)
        prev_month_anchor = now.replace(day=1) - dt.timedelta(days=1)
        target_year = prev_month_anchor.year
        target_month = prev_month_anchor.month
        await self.post_monthly_winner_logs_for_month(target_year, target_month, force=False)
    
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
        # sent_tips is a set of (user_id, tier) tuples
        user_tips = [tier for (uid, tier) in sent_tips if uid == user_id]
        
        total_tips = 0.0
        for tier in user_tips:
            # Find the tip amount for this tier
            for milestone in MILESTONES:
                if milestone['tier'] == tier:
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

        # Post closed-month winner summary once per month to logs channel.
        await self.maybe_post_monthly_winner_logs()
        
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
        position_markers = [
            "🥇", "🥈", "🥉", ":four:", ":five:",
            ":six:", ":seven:", ":eight:", ":nine:", ":one::zero:",
        ]
        for i in range(10):
            position_marker = position_markers[i] if i < len(position_markers) else f"#{i + 1}"
            if i < len(weighted_wager_data):
                entry = weighted_wager_data[i]
                username = entry.get("username", "Unknown")
                # Censor username using bullet characters for consistency.
                if len(username) > 3:
                    username = username[:-3] + "•••"
                else:
                    username = "•••"
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
                        f"{position_marker} — ***__{username}__*** — **Milestone Rank:** {rank_name} {rank_emoji}\n"
                        f"⚖️ **Weighted Wagered:** `${weighted_wagered:,.2f}`\n"
                        f"💰 **Total Wagered:** `${total_wagered:,.2f}`\n"
                        f"💸 **Milestone Tips Earned ({current_year}-{current_month:02d}):** `${monthly_tips:.2f}`\n"
                        f"🎁 **Prize:** `${prize:.2f}`"
                    )
                else:
                    leaderboard_lines.append(
                        f"{position_marker} — ***__{username}__*** — **Milestone Rank:** No Rank Yet\n"
                        f"⚖️ **Weighted Wagered:** `${weighted_wagered:,.2f}`\n"
                        f"💰 **Total Wagered:** `${total_wagered:,.2f}`\n"
                        f"💸 **Milestone Tips Earned ({current_year}-{current_month:02d}):** `$0.00`\n"
                        f"🎁 **Prize:** `${prize:.2f}`"
                    )
            else:
                leaderboard_lines.append(
                    f"{position_marker} — ***__N/A__*** — **Milestone Rank:** N/A\n"
                    f"⚖️ **Weighted Wagered:** `$0.00`\n"
                    f"💰 **Total Wagered:** `$0.00`\n"
                    f"💸 **Milestone Tips Earned ({current_year}-{current_month:02d}):** `$0.00`\n"
                    f"🎁 **Prize:** `${PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0:.2f}`"
                )
        leaderboard_block = '\n\n'.join(leaderboard_lines)
        embed = discord.Embed(
            title="🏆 **$1,500 USD Roobet Monthly Leaderboard** 🏆",
            description=(
                f"🗓️ **Leaderboard Period:**\n"
                f"From: <t:{start_unix}:F>\n"
                f"To: <t:{end_unix}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "📜 **Rules & Disclosure:**\n"
                "• Games with an RTP of **97% or less** contribute **100%** to your weighted wager.\n"
                "• Games with an RTP **above 97%** contribute **50%** to your weighted wager.\n"
                "• Games with an RTP **98% and above** contribute **10%** to your weighted wager.\n"
                "• **Only Slots and House Games count** (Dice is excluded).\n\n"
                "💵 **All amounts displayed are in USD.**\n\n"
                + leaderboard_block
                + f"\n\n📍 **View LB Logs:** <#{WAGER_LEADERBOARD_LOGS_CHANNEL_ID}>\n"
                f"🎭 **Claim your Wager Leaderboard role:** <#{WAGER_LEADERBOARD_ROLE_CLAIM_CHANNEL_ID}>"
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
                embed.set_footer(text="AutoTip Engine • Auto-pays during stream on the 1st of each month.")
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
