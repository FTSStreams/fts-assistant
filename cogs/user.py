import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import send_tip, get_current_month_range, get_current_week_range, fetch_weighted_wager
from db import get_db_connection, release_db_connection, save_tip_log, get_monthly_totals, get_user_slot_challenge_stats, get_roovsflip_queue, get_roovsflip_event_start
import os
from datetime import datetime
import datetime as dt
import logging
import asyncio
import re
from milestones_config import MILESTONES

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
MONTHTOMONTH_AUTOPOST_CHANNEL_ID = int(os.getenv("MONTHTOMONTH_AUTOPOST_CHANNEL_ID", "0"))
MILESTONE_PRIZES_CHANNEL_ID = int(os.getenv("MILESTONE_PRIZES_CHANNEL_ID", "1362517492651790416"))
MONTHLY_LEADERBOARD_PRIZES = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]
WAGER_LEADERBOARD_CHANNEL_ID = int(os.getenv("WAGER_LEADERBOARD_CHANNEL_ID", "1324462489404051487"))
SLOT_CHALLENGES_CHANNEL_ID = int(os.getenv("SLOT_CHALLENGES_CHANNEL_ID", "1385820512529158226"))
MULTI_LEADERBOARD_CHANNEL_ID = int(os.getenv("MULTI_LEADERBOARD_CHANNEL_ID", "1352322188102991932"))
ROO_VS_FLIP_CHANNEL_ID = int(os.getenv("ROO_VS_FLIP_CHANNEL_ID", "1486202172378189925"))
ROO_VS_FLIP_PRIZE_POOL = 250.00
MULTI_LEADERBOARD_PRIZES = [25, 15, 10]
TIP_TYPE_DISPLAY_ORDER = [
    "monthly_leaderboard",
    "milestone",
    "weekly_multiplier",
    "slot_challenge",
    "roo_vs_flip",
    "manual",
]

TIP_TYPE_DISPLAY_NAMES = {
    "monthly_leaderboard": "Monthly Leaderboard",
    "milestone": "Milestones",
    "weekly_multiplier": "Weekly Multi Leaderboard",
    "slot_challenge": "Slot Challenges",
    "roo_vs_flip": "Roo vs Flip",
    "manual": "Manual",
}

class User(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.last_monthtomonth_autopost_slot = None
        self.last_tipstats_autopost_slot = None
        self.auto_post_monthtomonth.start()
        self.auto_post_tipstats.start()
    
    def get_data_manager(self):
        """Helper to get DataManager cog"""
        return self.bot.get_cog('DataManager')

    async def _generate_monthtomonth_embed_file(self):
        import matplotlib.pyplot as plt
        import io
        import calendar

        # Get historical monthly data from database
        monthly_data = get_monthly_totals()

        # Force fresh data fetch for current month
        current_total = 0
        current_weighted = 0

        try:
            # Always fetch fresh data for current month - no caching
            from utils import get_current_month_range, fetch_total_wager, fetch_weighted_wager

            logger.info("[monthtomonth] Fetching fresh current month data...")

            # Get current month date range
            start_date, end_date = get_current_month_range()

            # Fetch fresh data for current month
            fresh_total_data = fetch_total_wager(start_date, end_date)
            fresh_weighted_data = fetch_weighted_wager(start_date, end_date)

            # Calculate totals from fresh data
            current_total = sum(
                entry.get("wagered", 0)
                for entry in fresh_total_data
                if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
            )
            current_weighted = sum(
                entry.get("weightedWagered", 0)
                for entry in fresh_weighted_data
                if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
            )

            logger.info(f"[monthtomonth] Fresh data: Total=${current_total:,.2f}, Weighted=${current_weighted:,.2f}")

        except Exception as e:
            logger.error(f"[monthtomonth] Failed to fetch fresh data: {e}")

            # Fallback to cached data if fresh fetch fails
            data_manager = self.get_data_manager()
            if data_manager:
                cached_data = data_manager.get_cached_data()
                if cached_data:
                    weighted_wager_data = cached_data.get('weighted_wager', [])
                    total_wager_data = cached_data.get('total_wager', [])

                    current_weighted = sum(
                        entry.get("weightedWagered", 0)
                        for entry in weighted_wager_data
                        if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
                    )
                    current_total = sum(
                        entry.get("wagered", 0)
                        for entry in total_wager_data
                        if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
                    )

                    logger.info(f"[monthtomonth] Fallback cached data: Total=${current_total:,.2f}, Weighted=${current_weighted:,.2f}")

        # Add current month to the data or update if already present with fresh data
        now = datetime.now()

        # Check if current month is already in the data and update/add accordingly
        current_month_found = False
        for i, data in enumerate(monthly_data):
            if data['year'] == now.year and data['month'] == now.month:
                monthly_data[i]['total_wager'] = current_total
                monthly_data[i]['weighted_wager'] = current_weighted
                current_month_found = True
                logger.info(f"[monthtomonth] Updated current month data in list: Total=${current_total:,.2f}, Weighted=${current_weighted:,.2f}")
                break

        if not current_month_found:
            monthly_data.append({
                'year': now.year,
                'month': now.month,
                'total_wager': current_total,
                'weighted_wager': current_weighted
            })
            logger.info(f"[monthtomonth] Added current month data to list: Total=${current_total:,.2f}, Weighted=${current_weighted:,.2f}")

        projected_total = None
        projected_weighted = None

        if monthly_data and monthly_data[-1]['year'] == now.year and monthly_data[-1]['month'] == now.month:
            import calendar as cal

            days_in_month = cal.monthrange(now.year, now.month)[1]
            days_elapsed = now.day

            if days_elapsed > 0:
                current_total = monthly_data[-1]['total_wager']
                current_weighted = monthly_data[-1]['weighted_wager']

                daily_avg_total = current_total / days_elapsed
                daily_avg_weighted = current_weighted / days_elapsed

                projected_total = daily_avg_total * days_in_month
                projected_weighted = daily_avg_weighted * days_in_month

                logger.info(
                    f"[monthtomonth] Projection calculated: Days {days_elapsed}/{days_in_month}, "
                    f"Projected Total=${projected_total:,.2f}, Projected Weighted=${projected_weighted:,.2f}"
                )

        if not monthly_data:
            embed = discord.Embed(
                title="📈 Month-to-Month Wager Totals",
                description="No monthly data available yet. Please try again later.",
                color=discord.Color.orange()
            )
            return embed, None

        monthly_data = monthly_data[-12:]

        months = []
        weighted_wagers = []
        total_wagers = []

        for data in monthly_data:
            month_name = calendar.month_name[data['month']]
            year_suffix = f" {data['year']}" if data['year'] != now.year else ""
            months.append(f"{month_name[:3]}{year_suffix}")
            weighted_wagers.append(data['weighted_wager'])
            total_wagers.append(data['total_wager'])

        plt.figure(figsize=(12, 6))
        plt.plot(months, weighted_wagers, marker='o', color='b', label='Weighted Wager', linewidth=2, markersize=6)
        plt.plot(months, total_wagers, marker='s', color='r', label='Total Wager', linewidth=2, markersize=6)

        if projected_total is not None:
            current_month_idx = len(months) - 1
            plt.plot(current_month_idx, projected_total, marker='^', color='red',
                     markersize=8, markerfacecolor='none', markeredgewidth=2,
                     label='Projected Total', linestyle='none')
            plt.plot(current_month_idx, projected_weighted, marker='^', color='blue',
                     markersize=8, markerfacecolor='none', markeredgewidth=2,
                     label='Projected Weighted', linestyle='none')

        plt.title('Month-to-Month Wager Totals', fontsize=16, fontweight='bold')
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Wager (USD)', fontsize=12)
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plt.close()

        file = discord.File(buf, filename="monthtomonth.png")
        embed = discord.Embed(title="📈 Month-to-Month Wager Totals", color=discord.Color.green())

        latest = monthly_data[-1]
        embed.add_field(
            name="Current Month",
            value=f"**Total:** ${latest['total_wager']:,.2f}\n**Weighted:** ${latest['weighted_wager']:,.2f}",
            inline=True
        )

        if projected_total is not None and projected_weighted is not None:
            embed.add_field(
                name="Month-End Projection",
                value=f"**Total:** ${projected_total:,.2f}\n**Weighted:** ${projected_weighted:,.2f}",
                inline=True
            )

        if len(monthly_data) > 1:
            previous = monthly_data[-2]
            total_change = latest['total_wager'] - previous['total_wager']
            weighted_change = latest['weighted_wager'] - previous['weighted_wager']

            total_emoji = "📈" if total_change >= 0 else "📉"
            weighted_emoji = "📈" if weighted_change >= 0 else "📉"

            embed.add_field(
                name="Month-over-Month Change",
                value=f"**Total:** {total_emoji} ${total_change:+,.2f}\n**Weighted:** {weighted_emoji} ${weighted_change:+,.2f}",
                inline=True
            )

        embed.set_image(url="attachment://monthtomonth.png")

        footer_text = f"Showing last {len(monthly_data)} months • Data auto-updates monthly"
        if projected_total is not None:
            footer_text += f" • Projections based on {now.day} days elapsed"
        embed.set_footer(text=footer_text)

        return embed, file

    @tasks.loop(minutes=1)
    async def auto_post_monthtomonth(self):
        if not MONTHTOMONTH_AUTOPOST_CHANNEL_ID:
            return

        now = datetime.now(dt.UTC)
        if now.minute != 0 or now.hour not in (0, 6, 12, 18):
            return

        slot_key = now.strftime("%Y-%m-%d-%H")
        if self.last_monthtomonth_autopost_slot == slot_key:
            return

        channel = self.bot.get_channel(MONTHTOMONTH_AUTOPOST_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(MONTHTOMONTH_AUTOPOST_CHANNEL_ID)
            except Exception as e:
                logger.error(f"[monthtomonth] Auto-post channel fetch failed: {e}")
                return

        try:
            embed, file = await self._generate_monthtomonth_embed_file()
            if file is not None:
                await channel.send(embed=embed, file=file)
            else:
                await channel.send(embed=embed)
            self.last_monthtomonth_autopost_slot = slot_key
            logger.info(f"[monthtomonth] Auto-posted chart for UTC slot {slot_key}")
        except Exception as e:
            logger.error(f"[monthtomonth] Auto-post failed: {e}")

    @auto_post_monthtomonth.before_loop
    async def before_auto_post_monthtomonth(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=1)
    async def auto_post_tipstats(self):
        if not MONTHTOMONTH_AUTOPOST_CHANNEL_ID:
            return

        now = datetime.now(dt.UTC)
        if now.minute != 1 or now.hour not in (0, 6, 12, 18):
            return

        slot_key = now.strftime("%Y-%m-%d-%H")
        if self.last_tipstats_autopost_slot == slot_key:
            return

        channel = self.bot.get_channel(MONTHTOMONTH_AUTOPOST_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(MONTHTOMONTH_AUTOPOST_CHANNEL_ID)
            except Exception as e:
                logger.error(f"[tipstats] Auto-post channel fetch failed: {e}")
                return

        try:
            summary_embed, by_type_embed = await self._generate_tipstats_embeds()
            await channel.send(embeds=[summary_embed, by_type_embed])
            self.last_tipstats_autopost_slot = slot_key
            logger.info(f"[tipstats] Auto-posted tip stats for UTC slot {slot_key}")
        except Exception as e:
            logger.error(f"[tipstats] Auto-post failed: {e}")

    @auto_post_tipstats.before_loop
    async def before_auto_post_tipstats(self):
        await self.bot.wait_until_ready()

    async def _generate_tipstats_embeds(self):
        """Build and return (summary_embed, by_type_embed) from the manualtips table."""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                now = datetime.now(dt.UTC)
                last_24h = now - dt.timedelta(hours=24)
                last_7d = now - dt.timedelta(days=7)
                last_30d = now - dt.timedelta(days=30)
                current_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                since_jan1 = datetime(2025, 1, 1, tzinfo=dt.UTC)
                cur.execute("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_24h,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_7d,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_30d,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS current_month,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS since_jan1
                    FROM manualtips;
                """, (last_24h, last_7d, last_30d, current_month_start, since_jan1))
                result = cur.fetchone()

                cur.execute("""
                    SELECT
                        tip_type,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_24h,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_7d,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_30d,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS current_month,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS lifetime
                    FROM manualtips
                    GROUP BY tip_type;
                """, (last_24h, last_7d, last_30d, current_month_start, since_jan1))
                by_type_rows = cur.fetchall()

                by_type_stats = {}
                for row in by_type_rows:
                    tip_type = row[0] or "unknown"
                    by_type_stats[tip_type] = {
                        "last_24h": float(row[1]),
                        "last_7d": float(row[2]),
                        "last_30d": float(row[3]),
                        "current_month": float(row[4]),
                        "lifetime": float(row[5]),
                    }

                def format_by_type(window_key):
                    lines = []
                    for tip_type in TIP_TYPE_DISPLAY_ORDER:
                        window_amount = by_type_stats.get(tip_type, {}).get(window_key, 0.0)
                        display_name = TIP_TYPE_DISPLAY_NAMES.get(tip_type, tip_type.replace("_", " ").title())
                        lines.append(f"• **{display_name}:** `${window_amount:,.2f}`")
                    remaining_types = sorted(
                        [tip_type for tip_type in by_type_stats.keys() if tip_type not in TIP_TYPE_DISPLAY_ORDER]
                    )
                    for tip_type in remaining_types:
                        window_amount = by_type_stats.get(tip_type, {}).get(window_key, 0.0)
                        display_name = TIP_TYPE_DISPLAY_NAMES.get(tip_type, tip_type.replace("_", " ").title())
                        lines.append(f"• **{display_name}:** `${window_amount:,.2f}`")
                    return "\n".join(lines)

                stats = {
                    "last_24h": float(result[0]),
                    "last_7d": float(result[1]),
                    "last_30d": float(result[2]),
                    "current_month": float(result[3]),
                    "since_jan1": float(result[4]) + 11295.53,
                    "legacy_adjustment": 11295.53,
                }

            summary_embed = discord.Embed(
                title="📊 Tip Statistics",
                description=(
                    f"**Past 24 Hours**: ${stats['last_24h']:.2f} USD\n"
                    f"**Past 7 Days**: ${stats['last_7d']:.2f} USD\n"
                    f"**Past 30 Days**: ${stats['last_30d']:.2f} USD\n"
                    f"**Current Month**: ${stats['current_month']:.2f} USD\n"
                    f"**Lifetime (Since Jan. 1st 2025)**: ${stats['since_jan1']:.2f} USD"
                ),
                color=discord.Color.blue()
            )
            summary_embed.add_field(
                name="Lifetime Adjustment",
                value=f"Legacy baseline included: ${stats['legacy_adjustment']:.2f}",
                inline=False,
            )
            summary_embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")

            by_type_embed = discord.Embed(
                title="📊 Tip Statistics by Type",
                color=discord.Color.blurple(),
            )
            by_type_embed.add_field(name="By Type • Past 24 Hours", value=format_by_type("last_24h"), inline=False)
            by_type_embed.add_field(name="By Type • Past 7 Days", value=format_by_type("last_7d"), inline=False)
            by_type_embed.add_field(name="By Type • Past 30 Days", value=format_by_type("last_30d"), inline=False)
            by_type_embed.add_field(name="By Type • Current Month", value=format_by_type("current_month"), inline=False)
            by_type_embed.add_field(name="By Type • Lifetime", value=format_by_type("lifetime"), inline=False)
            by_type_embed.set_footer(text="Type totals come from stored tip_type values in manualtips")

            return summary_embed, by_type_embed
        finally:
            release_db_connection(conn)

    async def _send_logged_tip(self, interaction: discord.Interaction, username: str, amount: float, tip_type: str, success_title: str):
        if amount <= 0:
            await interaction.response.send_message("❌ Tip amount must be greater than 0.", ephemeral=True)
            logger.error(f"Invalid tip amount: {amount} by {interaction.user}")
            return

        await interaction.response.defer()

        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return

        from utils import fetch_weighted_wager

        try:
            current_year = datetime.now(dt.UTC).year
            start_date = f"{current_year}-01-01T00:00:00Z"
            end_date = datetime.now(dt.UTC).isoformat()

            logger.info(f"[{tip_type}] Searching for {username} in yearly data from {start_date} to {end_date}")
            yearly_wager_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)

            username_lower = username.lower()
            roobet_uid = None

            for entry in yearly_wager_data:
                entry_username = entry.get("username", "").lower()
                if username_lower == entry_username:
                    roobet_uid = entry.get("uid")
                    username = entry.get("username")
                    logger.info(f"[{tip_type}] Found {username} (UID: {roobet_uid}) in yearly data")
                    break

            if not roobet_uid:
                cached_data = data_manager.get_cached_data()
                if cached_data:
                    weighted_wager_data = cached_data.get('weighted_wager', [])
                    for entry in weighted_wager_data:
                        entry_username = entry.get("username", "").lower()
                        if username_lower == entry_username:
                            roobet_uid = entry.get("uid")
                            username = entry.get("username")
                            logger.info(f"[{tip_type}] Found {username} (UID: {roobet_uid}) in current month data")
                            break

        except Exception as e:
            logger.error(f"Error fetching yearly data for {tip_type}: {e}")
            cached_data = data_manager.get_cached_data()
            if not cached_data:
                await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
                return

            weighted_wager_data = cached_data.get('weighted_wager', [])
            username_lower = username.lower()
            roobet_uid = None

            for entry in weighted_wager_data:
                entry_username = entry.get("username", "").lower()
                if username_lower == entry_username:
                    roobet_uid = entry.get("uid")
                    username = entry.get("username")
                    break

        if not roobet_uid:
            await interaction.followup.send(f"❌ No user found with username '{username}' in {datetime.now(dt.UTC).year} wager data.", ephemeral=True)
            logger.error(f"No UID found for username {username} in /{tip_type} by {interaction.user}")
            return

        logger.info(f"Attempting to send {tip_type} tip of ${amount} to {username} (UID: {roobet_uid})")
        response = await send_tip(
            user_id=os.getenv("ROOBET_USER_ID"),
            to_username=username,
            to_user_id=roobet_uid,
            amount=amount,
            show_in_chat=True,
            balance_type="crypto"
        )

        masked_username = username[:-3] + "\\*\\*\\*" if len(username) > 3 else "\\*\\*\\*"
        if response.get("success"):
            save_tip_log(roobet_uid, username, amount, tip_type, month=datetime.now(dt.UTC).month, year=datetime.now(dt.UTC).year)
            embed = discord.Embed(
                title=success_title,
                description=(
                    f"**{masked_username}** received a tip of **${amount:.2f} USD**!\n"
                    f"Sent by: **{interaction.user.display_name}**"
                ),
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
            await interaction.followup.send(embed=embed)
            logger.info(f"{tip_type} tip of ${amount} sent to {username} (UID: {roobet_uid})")
        else:
            error_message = response.get("message", "Unknown error")
            await interaction.followup.send(
                f"❌ Failed to send tip to {username}: {error_message}", ephemeral=True
            )
            logger.error(f"Failed to send {tip_type} tip to {username} (UID: {roobet_uid}): {error_message}")

    @app_commands.command(name="mywager", description="Check your personal wager stats for the current month using your Roobet username")
    @app_commands.describe(username="Your Roobet username")
    async def mywager(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()
        
        # Input validation - only allow alphanumeric characters and underscores
        if not re.match(r'^[a-zA-Z0-9_]+$', username):
            await interaction.followup.send("❌ Username can only contain letters, numbers, and underscores.", ephemeral=True)
            return
        
        if len(username) > 50:  # Reasonable length limit
            await interaction.followup.send("❌ Username is too long (max 50 characters).", ephemeral=True)
            return
        
        # Get data from DataManager
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
            
        weighted_wager_data = cached_data.get('weighted_wager', [])
        total_wager_data = cached_data.get('total_wager', [])
        
        username_lower = username.lower()
        roobet_uid = None
        
        # Find user in weighted wager data
        for entry in weighted_wager_data:
            entry_username = entry.get("username", "").lower()
            if username_lower == entry_username:
                roobet_uid = entry.get("uid")
                username = entry.get("username")
                break
                
        if not roobet_uid:
            await interaction.followup.send(f"❌ No user found with username '{username}' who wagered this month.", ephemeral=True)
            return
            
        # Find user's total wager
        total_wager = 0
        for entry in total_wager_data:
            if entry.get("uid") == roobet_uid:
                total_wager = entry.get("wagered", 0) if isinstance(entry.get("wagered"), (int, float)) else 0
                break
                
        # Find user's weighted wager
        weighted_wager = 0
        for entry in weighted_wager_data:
            if entry.get("uid") == roobet_uid:
                weighted_wager = entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) else 0
                break

        sorted_weighted_wager_data = sorted(
            weighted_wager_data,
            key=lambda entry: entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0 else 0,
            reverse=True
        )

        leaderboard_rank = None
        for index, entry in enumerate(sorted_weighted_wager_data, start=1):
            if entry.get("uid") == roobet_uid:
                leaderboard_rank = index
                break

        current_rank = None
        next_rank = None
        for milestone in MILESTONES:
            if weighted_wager >= milestone["threshold"]:
                current_rank = milestone
            elif next_rank is None:
                next_rank = milestone
                break

        if current_rank and next_rank:
            progress_start = float(current_rank["threshold"])
            progress_end = float(next_rank["threshold"])
            progress_ratio = (weighted_wager - progress_start) / (progress_end - progress_start)
        elif next_rank:
            progress_start = 0.0
            progress_end = float(next_rank["threshold"])
            progress_ratio = weighted_wager / progress_end if progress_end > 0 else 0.0
        else:
            progress_start = float(current_rank["threshold"]) if current_rank else 0.0
            progress_end = progress_start
            progress_ratio = 1.0

        progress_ratio = max(0.0, min(progress_ratio, 1.0))
        bar_length = 10
        filled_blocks = round(progress_ratio * bar_length)
        progress_bar = "█" * filled_blocks + "░" * (bar_length - filled_blocks)
        progress_percent = progress_ratio * 100

        current_rank_label = current_rank["tier"] if current_rank else "Unranked"
        current_rank_emoji = current_rank["emoji"] if current_rank else ""

        if next_rank:
            next_rank_progress_line = (
                f"📈 **Next Milestone Progress**: **${weighted_wager:,.2f} / ${float(next_rank['threshold']):,.2f}**\n"
                f"🧱 **Progress Bar**: **{progress_bar} {progress_percent:.1f}%**\n"
                f"💵 **Remaining**: **${float(next_rank['threshold']) - weighted_wager:,.2f}** to next milestone rank"
            )
        else:
            next_rank_progress_line = (
                f"📈 **Next Milestone Progress**: **MAX RANK REACHED**\n"
                f"🧱 **Progress Bar**: **{progress_bar} {progress_percent:.1f}%**\n"
                f"💵 **Remaining**: **$0.00** to next milestone rank"
            )

        leaderboard_status_lines = []
        current_lb_prize = 0.0
        if leaderboard_rank is not None and leaderboard_rank <= 10:
            current_lb_prize = MONTHLY_LEADERBOARD_PRIZES[leaderboard_rank - 1]
            leaderboard_status_lines.extend([
                f"🏆 **Wager Leaderboard Rank**: **#{leaderboard_rank}**",
                f"🎁 **Current LB Prize**: **${current_lb_prize:,.2f} USD**"
            ])

            if leaderboard_rank == 1:
                leaderboard_status_lines.append("👑 **Status**: **Holding first place**")
            else:
                next_lb_position = leaderboard_rank - 1
                next_lb_entry = sorted_weighted_wager_data[next_lb_position - 1]
                next_lb_weighted = next_lb_entry.get("weightedWagered", 0) if isinstance(next_lb_entry.get("weightedWagered"), (int, float)) else 0
                next_lb_gap = max(0.0, next_lb_weighted - weighted_wager)
                next_lb_prize = MONTHLY_LEADERBOARD_PRIZES[next_lb_position - 1]
                leaderboard_status_lines.extend([
                    f"⬆️ **Needed for #{next_lb_position}**: **${next_lb_gap:,.2f}** weighted wager",
                    f"🎯 **Next Tier Prize**: **${next_lb_prize:,.2f} USD**"
                ])
        else:
            tenth_place_weighted = 0.0
            if len(sorted_weighted_wager_data) >= 10:
                tenth_place_entry = sorted_weighted_wager_data[9]
                tenth_place_weighted = tenth_place_entry.get("weightedWagered", 0) if isinstance(tenth_place_entry.get("weightedWagered"), (int, float)) else 0

            leaderboard_gap = max(0.0, tenth_place_weighted - weighted_wager)
            leaderboard_status_lines.extend([
                "🏆 **Wager Leaderboard Status**: **Not placed**",
                f"🔟 **Top 10 Cutoff**: **${tenth_place_weighted:,.2f}** weighted",
                f"📌 **Needed for #10**: **${leaderboard_gap:,.2f}** weighted wager",
                f"🎁 **Prize at #10**: **${MONTHLY_LEADERBOARD_PRIZES[9]:,.2f} USD**"
            ])

        leaderboard_status_lines.append(f"📣 **Wager Leaderboard**: <#{WAGER_LEADERBOARD_CHANNEL_ID}>")

        leaderboard_status_block = "\n".join(leaderboard_status_lines)

        now_utc = datetime.now(dt.UTC)
        slot_stats = get_user_slot_challenge_stats(roobet_uid, month=now_utc.month, year=now_utc.year)
        slot_challenge_status_block = (
            f"🎯 **Slot Challenges Completed (All-Time)**: **{slot_stats['completed_all_time']}**\n"
            f"🎯 **Slot Challenges Completed (Current Month)**: **{slot_stats['completed_current_month']}**\n"
            f"💵 **Slot Challenges Money Earned (All-Time)**: **${slot_stats['earned_all_time']:,.2f} USD**\n"
            f"💵 **Slot Challenges Money Earned (Current Month)**: **${slot_stats['earned_current_month']:,.2f} USD**\n"
            f"📣 **Slot Challenges**: <#{SLOT_CHALLENGES_CHANNEL_ID}>"
        )

        # Weekly biggest single multiplier hit + placement details (sync with multi leaderboard period).
        weekly_multi_lines = ["🔥 **Biggest Multi This Week**: **No qualifying multi hit yet**"]
        weekly_rank = None
        current_multi_prize = 0.0
        try:
            week_start, week_end = get_current_week_range()
            weekly_weighted_data = await asyncio.to_thread(fetch_weighted_wager, week_start, week_end)
            weekly_candidates = []
            for entry in weekly_weighted_data:
                if not isinstance(entry, dict):
                    continue
                highest = entry.get("highestMultiplier")
                if not isinstance(highest, dict):
                    continue
                multiplier = highest.get("multiplier")
                if isinstance(multiplier, (int, float)) and multiplier > 0:
                    weekly_candidates.append(entry)

            weekly_candidates.sort(
                key=lambda entry: entry.get("highestMultiplier", {}).get("multiplier", 0),
                reverse=True
            )

            user_week_entry = next(
                (entry for entry in weekly_candidates if str(entry.get("uid")) == str(roobet_uid)),
                None
            )

            if user_week_entry:
                highest_multi = user_week_entry.get("highestMultiplier", {})
                weekly_multiplier = float(highest_multi.get("multiplier", 0))
                weekly_game = highest_multi.get("gameTitle", "Unknown")
                weekly_payout = float(highest_multi.get("payout", 0)) if isinstance(highest_multi.get("payout"), (int, float)) else 0.0
                weekly_wagered = float(highest_multi.get("wagered", 0)) if isinstance(highest_multi.get("wagered"), (int, float)) else 0.0
                weekly_rank = next(
                    (idx + 1 for idx, entry in enumerate(weekly_candidates) if str(entry.get("uid")) == str(roobet_uid)),
                    None
                )

                weekly_multi_lines = [
                    f"💥 **Biggest Win This Week (Multi LB)**: **x{weekly_multiplier:,.2f}** on **{weekly_game}**",
                    f"💰 **Payout**: **${weekly_payout:,.2f}** (**${weekly_wagered:,.2f}** base bet)",
                ]

                if weekly_rank is not None and weekly_rank <= 3:
                    current_multi_prize = MULTI_LEADERBOARD_PRIZES[weekly_rank - 1]
                    weekly_multi_lines = [
                        f"🏆 **Multi Leaderboard Rank**: **#{weekly_rank}**",
                        f"🔥 **Biggest Multi This Week**: **x{weekly_multiplier:,.2f}** on **{weekly_game}**",
                        f"💰 **Payout**: **${weekly_payout:,.2f}** (**${weekly_wagered:,.2f}** base bet)",
                        f"🎁 **Current Multi Prize**: **${current_multi_prize:,.2f} USD**",
                    ]
                    if weekly_rank == 1:
                        weekly_multi_lines.append("👑 **Status**: **Holding first place**")
                    else:
                        target_rank = weekly_rank - 1
                        target_entry = weekly_candidates[target_rank - 1]
                        target_multiplier = float(target_entry.get("highestMultiplier", {}).get("multiplier", 0))
                        required_multi_gap = max(0.0, target_multiplier - weekly_multiplier)
                        target_prize = MULTI_LEADERBOARD_PRIZES[target_rank - 1]
                        weekly_multi_lines.extend([
                            f"⬆️ **Needed for #{target_rank}**: **x{required_multi_gap:,.2f}** more multiplier",
                            f"🎯 **Next Tier Prize**: **${target_prize:,.2f} USD**",
                        ])
                else:
                    third_multiplier = 0.0
                    if len(weekly_candidates) >= 3:
                        third_multiplier = float(weekly_candidates[2].get("highestMultiplier", {}).get("multiplier", 0))
                    needed_for_top3 = max(0.0, third_multiplier - weekly_multiplier)
                    weekly_multi_lines = [
                        "🏆 **Multi Leaderboard Status**: **Not placed**",
                        f"🔥 **Biggest Multi This Week**: **x{weekly_multiplier:,.2f}** on **{weekly_game}**",
                        f"💰 **Payout**: **${weekly_payout:,.2f}** (**${weekly_wagered:,.2f}** base bet)",
                        f"🥉 **Top 3 Cutoff**: **x{third_multiplier:,.2f}**",
                        f"📌 **Needed for #3**: **x{needed_for_top3:,.2f}** more multiplier",
                        f"🎁 **Prize at #3**: **${MULTI_LEADERBOARD_PRIZES[2]:,.2f} USD**",
                    ]
            else:
                third_multiplier = 0.0
                if len(weekly_candidates) >= 3:
                    third_multiplier = float(weekly_candidates[2].get("highestMultiplier", {}).get("multiplier", 0))
                weekly_multi_lines.extend([
                    "🏆 **Multi Leaderboard Status**: **Not placed**",
                    f"🥉 **Top 3 Cutoff**: **x{third_multiplier:,.2f}**",
                    f"📌 **Needed for #3**: **x{third_multiplier:,.2f}** more multiplier",
                    f"🎁 **Prize at #3**: **${MULTI_LEADERBOARD_PRIZES[2]:,.2f} USD**",
                ])
        except Exception as e:
            logger.warning(f"Error loading weekly biggest win for /mywager: {e}")
        weekly_multi_lines.append(f"📣 **Multi Leaderboard**: <#{MULTI_LEADERBOARD_CHANNEL_ID}>")
        weekly_multi_block = "\n".join(weekly_multi_lines)

        # Roo vs Flip progress for current event.
        rvf_block = (
            "🆚 **Roo vs Flip Status**: **No active event**\n"
            f"💰 **Current Prize Pool**: **${ROO_VS_FLIP_PRIZE_POOL:,.2f} USD**\n"
            f"📣 **Roo vs Flip**: <#{ROO_VS_FLIP_CHANNEL_ID}>"
        )
        rvf_completed = False
        rvf_estimated_prize = 0.0
        rvf_period_end = None
        try:
            rvf_queue = get_roovsflip_queue()
            if rvf_queue:
                event_start = get_roovsflip_event_start()
                completed_games = 0
                total_games = len(rvf_queue)
                participant_completion_counts = {}

                event_start_dt = datetime.fromisoformat(str(event_start).replace("Z", "+00:00"))
                months_ahead = 2 if event_start_dt.day > 1 else 1
                target_month = event_start_dt.month + months_ahead
                target_year = event_start_dt.year + (target_month - 1) // 12
                target_month = ((target_month - 1) % 12) + 1
                rvf_period_end = datetime(target_year, target_month, 1, tzinfo=dt.UTC)

                for game in rvf_queue:
                    game_identifier = game.get("game_identifier")
                    game_entries = await asyncio.to_thread(fetch_weighted_wager, event_start, None, game_identifier)

                    req_multi = float(game.get("req_multi", 0)) if isinstance(game.get("req_multi"), (int, float)) else 0.0
                    if req_multi <= 0:
                        continue

                    for entry in game_entries:
                        uid = str(entry.get("uid"))
                        highest = entry.get("highestMultiplier") if isinstance(entry, dict) else None
                        multi_value = 0.0
                        if isinstance(highest, dict) and isinstance(highest.get("multiplier"), (int, float)):
                            multi_value = float(highest.get("multiplier", 0))
                        if multi_value >= req_multi:
                            participant_completion_counts[uid] = participant_completion_counts.get(uid, 0) + 1

                    user_game_entry = next(
                        (entry for entry in game_entries if str(entry.get("uid")) == str(roobet_uid)),
                        None
                    )
                    highest = user_game_entry.get("highestMultiplier") if isinstance(user_game_entry, dict) else None
                    multi_value = 0.0
                    if isinstance(highest, dict) and isinstance(highest.get("multiplier"), (int, float)):
                        multi_value = float(highest.get("multiplier", 0))
                    if multi_value >= req_multi and req_multi > 0:
                        completed_games += 1

                rvf_completed = completed_games == total_games and total_games > 0
                winner_count = sum(1 for count in participant_completion_counts.values() if count == total_games)
                if rvf_completed and winner_count > 0:
                    rvf_estimated_prize = round(ROO_VS_FLIP_PRIZE_POOL / winner_count, 2)
                rvf_status = "✅ Completed" if rvf_completed else "❌ Not completed"
                rvf_block = (
                    f"🆚 **Roo vs Flip Status**: **{rvf_status}**\n"
                    f"💰 **Current Prize Pool**: **${ROO_VS_FLIP_PRIZE_POOL:,.2f} USD**\n"
                    f"🎯 **Progress**: **{completed_games}/{total_games}** games completed\n"
                    f"📣 **Roo vs Flip**: <#{ROO_VS_FLIP_CHANNEL_ID}>"
                )
        except Exception as e:
            logger.warning(f"Error loading Roo vs Flip status for /mywager: {e}")

        # Payout summary (paid + pending).
        milestone_paid_all_time = 0.0
        milestone_paid_current_month = 0.0
        rvf_paid_for_cycle = False
        try:
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(amount), 0)
                        FROM manualtips
                        WHERE user_id = %s AND tip_type = 'milestone';
                        """,
                        (str(roobet_uid),)
                    )
                    milestone_paid_all_time = float((cur.fetchone() or [0])[0] or 0)

                    cur.execute(
                        """
                        SELECT COALESCE(SUM(amount), 0)
                        FROM manualtips
                        WHERE user_id = %s
                            AND tip_type = 'milestone'
                            AND month = %s
                            AND year = %s;
                        """,
                        (str(roobet_uid), now_utc.month, now_utc.year)
                    )
                    milestone_paid_current_month = float((cur.fetchone() or [0])[0] or 0)

                    if rvf_period_end:
                        if rvf_period_end.month == 1:
                            payout_year = rvf_period_end.year - 1
                            payout_month = 12
                        else:
                            payout_year = rvf_period_end.year
                            payout_month = rvf_period_end.month - 1

                        cur.execute(
                            """
                            SELECT COUNT(*)
                            FROM roovsflip_payouts
                            WHERE year = %s AND month = %s AND winner_uid = %s;
                            """,
                            (payout_year, payout_month, str(roobet_uid))
                        )
                        rvf_paid_for_cycle = int((cur.fetchone() or [0])[0] or 0) > 0
            finally:
                release_db_connection(conn)
        except Exception as e:
            logger.warning(f"Error building payout summary for /mywager: {e}")

        wager_expected = current_lb_prize if (leaderboard_rank is not None and leaderboard_rank <= 10) else 0.0
        wager_pending = wager_expected  # Current month's LB prize is always pending; it pays out on the 1st of next month
        multi_pending = current_multi_prize if (weekly_rank is not None and weekly_rank <= 3) else 0.0
        rvf_pending = rvf_estimated_prize if (rvf_completed and not rvf_paid_for_cycle) else 0.0

        if now_utc.month == 12:
            next_month_payout = datetime(now_utc.year + 1, 1, 1, 0, 15, 0, tzinfo=dt.UTC)
        else:
            next_month_payout = datetime(now_utc.year, now_utc.month + 1, 1, 0, 15, 0, tzinfo=dt.UTC)

        days_until_friday = (4 - now_utc.weekday()) % 7
        next_multi_payout = (now_utc + dt.timedelta(days=days_until_friday)).replace(hour=0, minute=15, second=0, microsecond=0)
        if next_multi_payout <= now_utc:
            next_multi_payout = next_multi_payout + dt.timedelta(days=7)

        payout_lines = [
            "💸 **Payout Summary**",
            "",
            "✅ **Paid:**",
            f"• Milestone Tips Earned (All-Time): **${milestone_paid_all_time:,.2f}**",
            f"• Milestone Tips Earned (Current Month): **${milestone_paid_current_month:,.2f}**",
            f"• Slot Challenges (All-Time): **${slot_stats['earned_all_time']:,.2f}**",
            f"• Slot Challenges (Current Month): **${slot_stats['earned_current_month']:,.2f}**",
            "",
            "⏳ **Pending:**",
            f"• Wager Leaderboard: **${wager_pending:,.2f}** (Expected <t:{int(next_month_payout.timestamp())}:R>)",
            f"• Multi Leaderboard: **${multi_pending:,.2f}** (Expected <t:{int(next_multi_payout.timestamp())}:R>)",
        ]

        if rvf_period_end:
            payout_lines.append(
                f"• Roo vs Flip: **${rvf_pending:,.2f}** (Expected <t:{int(rvf_period_end.timestamp())}:R>)"
            )
        else:
            payout_lines.append("• Roo vs Flip: **$0.00** (Expected N/A)")

        total_paid_out_all_time = milestone_paid_all_time + float(slot_stats['earned_all_time'])
        total_paid_out_current_month = milestone_paid_current_month + float(slot_stats['earned_current_month'])
        total_pending = wager_pending + multi_pending + rvf_pending
        grand_total = total_paid_out_all_time + total_pending

        payout_lines.extend([
            "",
            "📊 **Totals:**",
            f"• Total Paid Out (All-Time): **${total_paid_out_all_time:,.2f}**",
            f"• Total Paid Out (Current Month): **${total_paid_out_current_month:,.2f}**",
            f"• Total Pending: **${total_pending:,.2f}**",
            f"• Grand Total: **${grand_total:,.2f}**",
        ])
        payout_summary_block = "\n".join(payout_lines)

        divider = "────────────────────────"
                
        embed = discord.Embed(
            title=f"🎰 Your Wager Stats, {username}! 🎰",
            description=(
                f"💰 **Total Wager**: **${total_wager:,.2f} USD**\n"
                f"⚖️ **Weighted Wager**: **${weighted_wager:,.2f} USD**\n"
                f"\n{divider}\n"
                f"\n🏅**Current Milestone Rank**: **{current_rank_label}** {current_rank_emoji}\n"
                f"{next_rank_progress_line}\n"
                f"🎁 **Milestone Prizes**: <#{MILESTONE_PRIZES_CHANNEL_ID}>\n"
                f"\n{divider}\n"
                f"\n{leaderboard_status_block}\n"
                f"\n{divider}\n"
                f"\n{weekly_multi_block}\n"
                f"\n{divider}\n"
                f"\n{slot_challenge_status_block}\n"
                f"\n{divider}\n"
                f"\n{rvf_block}\n"
                f"\n{divider}\n"
                f"\n{payout_summary_block}"
            ),
            color=discord.Color.gold()
        )
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        embed.set_footer(text=f"🕒 Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="monthlygoal", description="Display total wager and weighted wager for the current month")
    async def monthlygoal(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        # Get data from DataManager
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
            
        try:
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
            embed = discord.Embed(
                title="📈 Monthly Wager Stats",
                description=(
                    f"**TOTAL WAGER THIS MONTH**: ${total_wager:,.2f} USD\n"
                    f"**TOTAL WEIGHTED WAGER THIS MONTH**: ${total_weighted_wager:,.2f} USD"
                ),
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Error retrieving monthly stats: {str(e)}", ephemeral=True)
            logger.error(f"Error in /monthlygoal: {str(e)}")

    @app_commands.command(name="tipstats", description="Display tip statistics (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def tipstats(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            summary_embed, by_type_embed = await self._generate_tipstats_embeds()
            await interaction.followup.send(embeds=[summary_embed, by_type_embed])
        except Exception as e:
            await interaction.followup.send(f"❌ Error retrieving tip stats: {str(e)}", ephemeral=True)
            logger.error(f"Error in /tipstats: {str(e)}")

    @app_commands.command(name="tipuser", description="Manually tip a Roobet user by username (admin only)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The Roobet username of the player",
        amount="The tip amount in USD (e.g., 5.00)"
    )
    async def tipuser(self, interaction: discord.Interaction, username: str, amount: float):
        await self._send_logged_tip(interaction, username, amount, "manual", "🎉 Manual Tip Sent!")

    @app_commands.command(name="tipmonthly", description="Send a monthly leaderboard tip to a Roobet user (admin only)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The Roobet username of the player",
        amount="The tip amount in USD (e.g., 5.00)"
    )
    async def tipmonthly(self, interaction: discord.Interaction, username: str, amount: float):
        await self._send_logged_tip(interaction, username, amount, "monthly_leaderboard", "🏆 Monthly Leaderboard Tip Sent!")

    @app_commands.command(name="monthtomonth", description="Show a month-to-month wager line chart")
    async def monthtomonth(self, interaction: discord.Interaction):
        await interaction.response.defer()
        embed, file = await self._generate_monthtomonth_embed_file()
        if file is not None:
            await interaction.followup.send(embed=embed, file=file)
        else:
            await interaction.followup.send(embed=embed)

    @app_commands.command(name="lifetimestats", description="Show total wager and weighted wager for the current month")
    async def lifetimestats(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        # Get data from DataManager (current month data)
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
        
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
        
        now = datetime.now(dt.UTC)
        
        embed = discord.Embed(
            title="🏆 Current Month Wager Stats",
            description=(
                f"**TOTAL WAGER (This Month):** ${total_wager:,.2f} USD\n"
                f"**TOTAL WEIGHTED WAGER (This Month):** ${total_weighted_wager:,.2f} USD"
            ),
            color=discord.Color.purple()
        )
        embed.set_footer(text=f"Generated on {now.strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed)

    def cog_unload(self):
        self.auto_post_monthtomonth.cancel()
        self.auto_post_tipstats.cancel()

async def setup(bot):
    await bot.add_cog(User(bot))
