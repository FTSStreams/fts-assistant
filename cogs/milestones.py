import discord
from discord.ext import commands, tasks
from discord import app_commands
from utils import send_tip, get_current_month_range
from db import get_db_connection, release_db_connection, save_tip_log, load_sent_tips, save_tip, get_leaderboard_message_id, save_leaderboard_message_id
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio
from milestones_config import MILESTONES

logger = logging.getLogger(__name__)
MILESTONE_PRIZES_CHANNEL_ID = 1362517492651790416

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
                logger.info(f"[Milestones] Processing tip for {username} - {milestone['tier']} (month={month}, year={year})")
                bot_user_id = os.getenv("ROOBET_USER_ID")
                tip_response = await send_tip(bot_user_id, username, user_id, milestone["tip"])
                if tip_response.get("success"):
                    save_tip(user_id, milestone["tier"], month, year)
                    save_tip_log(user_id, username, milestone["tip"], "milestone", month, year)
                    logger.info(f"[Milestones] Successfully saved tip for {username} - {milestone['tier']} in database (month={month}, year={year})")
                    # Censor username for public display using bullet characters for consistency.
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
                            f"💸 **Tip Received:** ${milestone['tip']:.2f} USD\n"
                            f"See Milestone Prizes -> <#{MILESTONE_PRIZES_CHANNEL_ID}>"
                        ),
                        color=milestone["color"]
                    )
                    embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
                    embed.set_footer(text="AutoTip Engine Live • Payout Sent Successfully")
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

async def setup(bot):
    await bot.add_cog(Milestones(bot))
