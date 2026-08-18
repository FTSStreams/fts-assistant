import discord
from discord.ext import commands, tasks
from discord import app_commands
from utils import send_tip, get_current_month_range
from db import get_db_connection, release_db_connection, save_tip_log, load_sent_tips, save_tip, get_leaderboard_message_id, save_leaderboard_message_id, get_setting_value
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio
from milestones_config import MILESTONES
from collections import deque

logger = logging.getLogger(__name__)
MILESTONE_PRIZES_CHANNEL_ID = 1362517492651790416
MILESTONE_BLOCKED_USER_IDS_KEY = "milestone_blocked_user_ids"
MILESTONE_BADGE_URLS = {
    "g15": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g15.png",
    "g14": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g14.png",
    "g13": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g13.png",
    "g12": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g12.png",
    "g11": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g11.png",
    "g10": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g10.png",
    "g9": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g9.png",
    "g8": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g8.png",
    "g7": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g7.png",
    "g6": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g6.png",
    "g5": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g5.png",
    "g4": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g4.png",
    "g3": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g3.png",
    "g2": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g2.png",
    "g1": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/g1.png",
    "s15": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s15.png",
    "s14": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s14.png",
    "s13": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s13.png",
    "s12": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s12.png",
    "s11": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s11.png",
    "s10": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s10.png",
    "s9": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s9.png",
    "s8": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s8.png",
    "s7": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s7.png",
    "s6": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s6.png",
    "s5": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s5.png",
    "s4": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s4.png",
    "s3": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s3.png",
    "s2": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s2.png",
    "s1": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/s1.png",
    "b15": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b15.png",
    "b14": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b14.png",
    "b13": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b13.png",
    "b12": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b12.png",
    "b11": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b11.png",
    "b10": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b10.png",
    "b9": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b9.png",
    "b8": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b8.png",
    "b7": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b7.png",
    "b6": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b6.png",
    "b5": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b5.png",
    "b4": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b4.png",
    "b3": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b3.png",
    "b2": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b2.png",
    "b1": "https://raw.githubusercontent.com/FTSStreams/fts-assistant/main/assets/images/MilestoneRanks/b1.png",
}

# Environment variable validation with proper error handling
try:
    GUILD_ID = int(os.getenv("GUILD_ID", "0"))
    MILESTONE_CHANNEL_ID = int(os.getenv("MILESTONE_CHANNEL_ID", "0"))
    TIP_CONFIRMATION_CHANNEL_ID = int(os.getenv("TIP_CONFIRMATION_CHANNEL_ID", "0"))
    
    if not all([GUILD_ID, MILESTONE_CHANNEL_ID, TIP_CONFIRMATION_CHANNEL_ID]):
        raise ValueError("Missing required environment variables: GUILD_ID, MILESTONE_CHANNEL_ID, TIP_CONFIRMATION_CHANNEL_ID")
except (ValueError, TypeError) as e:
    logger.critical(f"Environment variable error in milestones.py: {e}")
    raise SystemExit("Bot cannot start due to missing or invalid environment variables")

class Milestones(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tip_queue = asyncio.Queue()
        
        # Initialize month/year state tracking
        now = datetime.now(dt.UTC)
        self.current_month = now.month
        self.current_year = now.year
        logger.info(f"[Milestones] Initialized with month/year: {self.current_year}-{self.current_month:02d}")
        
        self.check_wager_milestones.start()
        # process_tip_queue_task will be started in cog_load
    
    def get_data_manager(self):
        """Helper to get DataManager cog"""
        return self.bot.get_cog('DataManager')

    @staticmethod
    def _normalize_roobet_username(username):
        if not isinstance(username, str):
            return ""
        cleaned = username.strip()
        if cleaned.startswith("@"):
            cleaned = cleaned[1:]
        return cleaned.lower()

    def _load_blocked_identities(self):
        raw_value = get_setting_value(MILESTONE_BLOCKED_USER_IDS_KEY, default="{}")
        usernames = set()
        uids = set()

        try:
            import json

            parsed = json.loads(raw_value)
            if isinstance(parsed, dict):
                usernames = {
                    self._normalize_roobet_username(entry)
                    for entry in parsed.get("usernames", [])
                    if self._normalize_roobet_username(entry)
                }
                uids = {
                    str(entry).strip()
                    for entry in parsed.get("uids", [])
                    if str(entry).strip()
                }
                return {"usernames": usernames, "uids": uids}

            if isinstance(parsed, list):
                # Legacy format fallback.
                uids = {str(entry).strip() for entry in parsed if str(entry).strip()}
                return {"usernames": set(), "uids": uids}
        except Exception:
            pass

        # Legacy CSV fallback.
        tokens = {entry.strip() for entry in str(raw_value).split(",") if entry and entry.strip()}
        return {"usernames": set(), "uids": tokens}

    def is_user_blocked_from_milestones(self, user_id, username=None):
        identities = self._load_blocked_identities()
        blocked_uids = identities.get("uids", set())
        blocked_usernames = identities.get("usernames", set())

        uid_match = str(user_id) in blocked_uids if user_id is not None else False
        username_match = self._normalize_roobet_username(username) in blocked_usernames if username else False
        return uid_match or username_match

    def _find_milestone_by_tier(self, tier_name):
        """Return the milestone definition for a stored tier name, such as 'Rank 7'."""
        tier_name = str(tier_name).strip()
        for milestone in MILESTONES:
            if milestone.get("tier") == tier_name:
                return milestone
        return None

    def _build_milestone_embed(self, username, milestone, tip_amount, footer_text="AutoTip Engine Live • Payout Sent Successfully"):
        display_username = username
        if len(username) > 3:
            display_username = username[:-3] + "•••"
        else:
            display_username = "•••"

        embed = discord.Embed(
            title=f"{milestone['emoji']} {milestone['tier']} Wager Milestone Achieved! {milestone['emoji']}",
            description=(
                f"🆔 **ID:** {display_username}\n"
                f"✨ **Weighted Wager:** ${milestone['threshold']:,.2f}\n"
                f"💸 **Tip Received:** ${float(tip_amount):.2f} USD\n"
                f"See Milestone Prizes -> <#{MILESTONE_PRIZES_CHANNEL_ID}>"
            ),
            color=milestone["color"]
        )
        badge_name = milestone['emoji'].split(':')[1] if ':' in milestone['emoji'] else None
        thumbnail_url = MILESTONE_BADGE_URLS.get(badge_name) if badge_name else None
        if thumbnail_url:
            embed.set_thumbnail(url=thumbnail_url)
        embed.set_footer(text=footer_text)
        return embed

    def purge_user_from_tip_queue(self, roobet_username=None, roobet_uid=None):
        """Remove queued milestone tips for a specific Roobet user and return removed count."""
        username_key = self._normalize_roobet_username(roobet_username)
        uid_key = str(roobet_uid).strip() if roobet_uid is not None and str(roobet_uid).strip() else ""

        if not username_key and not uid_key:
            return 0

        original_items = list(self.tip_queue._queue)
        kept_items = []
        removed_count = 0

        for item in original_items:
            queued_user_id = str(item[0]) if isinstance(item, tuple) and len(item) > 0 else ""
            queued_username = self._normalize_roobet_username(item[1]) if isinstance(item, tuple) and len(item) > 1 else ""

            uid_match = uid_key and queued_user_id == uid_key
            username_match = username_key and queued_username == username_key

            if uid_match or username_match:
                removed_count += 1
            else:
                kept_items.append(item)

        if removed_count <= 0:
            return 0

        self.tip_queue._queue = deque(kept_items)
        self.tip_queue._unfinished_tasks = max(0, self.tip_queue._unfinished_tasks - removed_count)
        if self.tip_queue._unfinished_tasks == 0:
            self.tip_queue._finished.set()

        return removed_count

    async def cog_load(self):
        self.process_tip_queue_task = asyncio.create_task(self.process_tip_queue())

    def cog_unload(self):
        self.check_wager_milestones.cancel()
        if hasattr(self, 'process_tip_queue_task'):
            self.process_tip_queue_task.cancel()

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
                # ❌ BUG FIX: Don't call task_done() here - we haven't called get() yet
                await asyncio.sleep(5)
                continue
            
            try:
                user_id, username, milestone, month, year = await self.tip_queue.get()
                if self.is_user_blocked_from_milestones(user_id, username):
                    logger.info(
                        f"[Milestones] Skipping blocked user {username} ({user_id}) for {milestone['tier']}"
                    )
                    continue
                logger.info(f"[Milestones] Processing tip for {username} - {milestone['tier']} (month={month}, year={year})")
                bot_user_id = os.getenv("ROOBET_USER_ID")
                tip_response = await send_tip(bot_user_id, username, user_id, milestone["tip"])
                if tip_response.get("success"):
                    save_tip(user_id, milestone["tier"], month, year)
                    save_tip_log(user_id, username, milestone["tip"], "milestone", month, year)
                    logger.info(f"[Milestones] Successfully saved tip for {username} - {milestone['tier']} in database (month={month}, year={year})")
                    embed = self._build_milestone_embed(username, milestone, milestone['tip'])
                    await channel.send(embed=embed)
                else:
                    logger.error(f"Failed to send milestone tip to {username}: {tip_response.get('message')}")
                    
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Error processing tip queue item: {e}")
            finally:
                # Always call task_done() after get(), even if there was an error
                self.tip_queue.task_done()

    @tasks.loop(minutes=10)
    async def check_wager_milestones(self):
        logger.info("[Milestones] Starting milestone check cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        
        now = datetime.now(dt.UTC)
        month = now.month
        year = now.year
        
        # Check for month transition and update state
        if month != self.current_month or year != self.current_year:
            logger.info(f"[Milestones] Month transition detected: {self.current_year}-{self.current_month:02d} → {year}-{month:02d}")
            self.current_month = month
            self.current_year = year
            logger.info("[Milestones] Month/year state updated for new period")
        
        sent_tips = load_sent_tips(month, year)
        logger.info(f"[Milestones] Loaded {len(sent_tips)} existing tips for {year}-{month:02d}")
        
        # Get data from DataManager
        data_manager = self.get_data_manager()
        if not data_manager:
            logger.error("[Milestones] DataManager not available")
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            logger.error("[Milestones] No cached data available")
            return
            
        weighted_wager_data = cached_data.get('weighted_wager', [])
        logger.info(f"[Milestones] Checking {len(weighted_wager_data)} users for milestones")
        
        # Track what we're queuing in this cycle to prevent duplicates
        queued_this_cycle = set()
        
        for entry in weighted_wager_data:
            user_id = entry.get("uid")
            username = entry.get("username", "Unknown")
            if self.is_user_blocked_from_milestones(user_id, username):
                logger.info(f"[Milestones] Skipping queue for blocked user {username} ({user_id})")
                continue
            weighted_wagered = entry.get("weightedWagered", 0)
            if not isinstance(weighted_wagered, (int, float)) or weighted_wagered < 0:
                continue
            for milestone in MILESTONES:
                tier = milestone["tier"]
                threshold = milestone["threshold"]
                milestone_key = (user_id, tier)
                
                # Check both database and this cycle's queue to prevent duplicates
                if (weighted_wagered >= threshold and 
                    milestone_key not in sent_tips and 
                    milestone_key not in queued_this_cycle):
                    
                    logger.info(f"[Milestones] Queuing milestone {tier} for {username} (${weighted_wagered:,.2f})")
                    await self.tip_queue.put((user_id, username, milestone, month, year))
                    queued_this_cycle.add(milestone_key)

    @check_wager_milestones.before_loop
    async def before_milestone_loop(self):
        await self.bot.wait_until_ready()

    def calculate_total_tips_for_rank(self, current_rank_index):
        """Calculate the total cumulative tips earned up to a specific rank"""
        if current_rank_index == -1:
            return 0.0
        
        total = 0.0
        for i in range(current_rank_index + 1):  # Include the current rank
            total += MILESTONES[i]["tip"]
        return total

    @app_commands.command(name="restoremilestones", description="Restore recent milestone log posts to the milestone channel")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(amount="Number of recent milestone logs to restore")
    async def restore_milestones(self, interaction: discord.Interaction, amount: int):
        """Replay recent milestone payouts into the milestone channel."""
        if amount <= 0:
            await interaction.response.send_message("❌ Amount must be greater than 0.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        channel = self.bot.get_channel(MILESTONE_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(MILESTONE_CHANNEL_ID)
            except Exception as e:
                logger.error(f"[Milestones] Failed to fetch milestone channel for restore: {e}")
                await interaction.followup.send("❌ Could not find the milestone channel.", ephemeral=True)
                return

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, tier, month, year, tipped_at
                    FROM milestonetips
                    ORDER BY tipped_at DESC NULLS LAST, year DESC, month DESC
                    LIMIT %s;
                    """,
                    (amount,),
                )
                rows = cur.fetchall()
        except Exception as e:
            logger.error(f"[Milestones] Failed to load milestone rows for restore: {e}")
            await interaction.followup.send("❌ Failed to load recent milestone records from the database.", ephemeral=True)
            return
        finally:
            release_db_connection(conn)

        if not rows:
            await interaction.followup.send("ℹ️ No milestone records were found to restore.", ephemeral=True)
            return

        restored = 0
        failed = 0
        for row in reversed(rows):
            user_id, tier_name, month, year, tipped_at = row
            try:
                milestone = self._find_milestone_by_tier(tier_name)
                if milestone is None:
                    logger.warning(f"[Milestones] Could not match stored milestone tier '{tier_name}' to a known milestone definition; skipping restore for user_id={user_id}")
                    failed += 1
                    continue

                username = None
                conn2 = get_db_connection()
                try:
                    with conn2.cursor() as cur2:
                        cur2.execute(
                            "SELECT username FROM manualtips WHERE user_id = %s AND tip_type = 'milestone' AND month = %s AND year = %s AND amount = %s ORDER BY tipped_at DESC LIMIT 1;",
                            (user_id, month, year, milestone['tip'])
                        )
                        result = cur2.fetchone()
                        if result:
                            username = result[0]
                except Exception as e:
                    logger.error(f"[Milestones] Failed to resolve username for restore row user_id={user_id}: {e}")
                finally:
                    release_db_connection(conn2)

                if not username:
                    username = "Unknown"

                embed = self._build_milestone_embed(username, milestone, milestone['tip'])
                await channel.send(embed=embed)
                restored += 1
            except Exception as e:
                logger.error(f"[Milestones] Failed to restore milestone log for user_id={user_id}, tier={tier_name}: {e}")
                failed += 1

        await interaction.followup.send(
            f"✅ Restored **{restored}** milestone log(s) to <#{MILESTONE_CHANNEL_ID}>. Failed: **{failed}**.",
            ephemeral=True,
        )

    @app_commands.command(name="milestonerules", description="Display milestone reward tiers and rules")
    async def milestonerules(self, interaction: discord.Interaction):
        """Post the milestone rules and all tiers in four embeds"""
        # Build rules field
        rules_field = (
            ":white_check_mark: **AutoTip Engine**\n"
            "*Reach a Wager Milestone -> Get Paid Instantly!*\n\n"
            ":bar_chart: Track your progress with **`/mywager [Username]`**\n"
            ":moneybag: View payout records in <#1339413771000614982>\n"
            ":arrows_counterclockwise: Progress resets every month.\n"
            ":dollar: All rewards are displayed in USD."
        )
        
        def build_tier_lines(start, end):
            return [
                f"{MILESTONES[i]['emoji']} **Rank {i+1}**: `${MILESTONES[i]['threshold']:,}` → `${MILESTONES[i]['tip']:.2f} USD`"
                for i in range(start, end)
            ]

        def add_tier_fields(embed, tier_name, tier_lines):
            field_lines = []
            field_length = 0
            for line in tier_lines:
                line_length = len(line) + (1 if field_lines else 0)
                if field_lines and field_length + line_length > 1024:
                    embed.add_field(name="\u200b", value="\n".join(field_lines), inline=False)
                    field_lines = []
                    field_length = 0
                field_lines.append(line)
                field_length += len(line) + (1 if len(field_lines) > 1 else 0)
            if field_lines:
                embed.add_field(name="\u200b", value="\n".join(field_lines), inline=False)

        rules_embed = discord.Embed(
            title="🎯 **WAGER MILESTONES - AUTOMATIC TIPS!** 🎯",
            description="**Climb 45 ranks and earn instant cash rewards!** 💰",
            color=discord.Color.blue()
        )
        rules_embed.add_field(name="⚡ **HOW IT WORKS**", value=rules_field, inline=False)
        rules_embed.set_footer(text="AutoTip Engine is Live")

        bronze_embed = discord.Embed(title="🥉 **BRONZE TIER (Ranks 1-15)**", color=discord.Color.from_rgb(205, 127, 50))
        add_tier_fields(bronze_embed, "🥉 **BRONZE TIER (Ranks 1-15)**", build_tier_lines(0, 15))
        bronze_embed.set_footer(text="AutoTip Engine is Live")

        silver_embed = discord.Embed(title="🥈 **SILVER TIER (Ranks 16-30)**", color=discord.Color.from_rgb(192, 192, 192))
        add_tier_fields(silver_embed, "🥈 **SILVER TIER (Ranks 16-30)**", build_tier_lines(15, 30))
        silver_embed.set_footer(text="AutoTip Engine is Live")

        gold_embed = discord.Embed(title="🥇 **GOLD TIER (Ranks 31-45)**", color=discord.Color.from_rgb(255, 215, 0))
        add_tier_fields(gold_embed, "🥇 **GOLD TIER (Ranks 31-45)**", build_tier_lines(30, 45))
        gold_embed.set_footer(text="AutoTip Engine is Live")

        await interaction.response.send_message(embeds=[rules_embed, bronze_embed, silver_embed, gold_embed])
        logger.info("[Milestones] Posted milestone rules embed")

async def setup(bot):
    await bot.add_cog(Milestones(bot))
