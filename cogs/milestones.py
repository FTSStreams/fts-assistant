import discord
from discord.ext import commands, tasks
from discord import app_commands
from utils import send_tip, get_current_month_range
from db import get_db_connection, release_db_connection, save_tip_log, load_sent_tips, save_tip
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)

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
MILESTONES = [
    {"tier": "Rank 1", "threshold": 50, "tip": 1.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:rank1:1389367229417656543>"},
    {"tier": "Rank 2", "threshold": 100, "tip": 1.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:rank2:1389367231191715970>"},
    {"tier": "Rank 3", "threshold": 150, "tip": 1.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:rank3:1389367233507229776>"},
    {"tier": "Rank 4", "threshold": 250, "tip": 1.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:rank4:1389367235390472304>"},
    {"tier": "Rank 5", "threshold": 400, "tip": 1.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:rank5:1389367237407670394>"},
    {"tier": "Rank 6", "threshold": 600, "tip": 2.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:rank6:1389367239161155624>"},
    {"tier": "Rank 7", "threshold": 800, "tip": 2.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:rank7:1389367446196060210>"},
    {"tier": "Rank 8", "threshold": 1000, "tip": 2.00, "color": discord.Color.from_rgb(192, 192, 192), "emoji": "<:rank8:1389367448133697647>"},
    {"tier": "Rank 9", "threshold": 1500, "tip": 3.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:rank9:1389367449974997062>"},
    {"tier": "Rank 10", "threshold": 2000, "tip": 3.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:rank10:1389367451770294386>"},
    {"tier": "Rank 11", "threshold": 2500, "tip": 3.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:rank11:1389367453766909992>"},
    {"tier": "Rank 12", "threshold": 3000, "tip": 3.00, "color": discord.Color.from_rgb(205, 127, 50), "emoji": "<:rank12:1389367455788564613>"},
    {"tier": "Rank 13", "threshold": 5000, "tip": 11.00, "color": discord.Color.from_rgb(0, 255, 127), "emoji": "<:rank13:1389367624273498202>"},
    {"tier": "Rank 14", "threshold": 7500, "tip": 14.00, "color": discord.Color.from_rgb(0, 255, 127), "emoji": "<:rank14:1389367626102210721>"},
    {"tier": "Rank 15", "threshold": 10000, "tip": 14.00, "color": discord.Color.from_rgb(0, 255, 127), "emoji": "<:rank15:1389367628149166151>"},
    {"tier": "Rank 16", "threshold": 15000, "tip": 27.00, "color": discord.Color.from_rgb(0, 191, 255), "emoji": "<:rank16:1389367630078545930>"},
    {"tier": "Rank 17", "threshold": 20000, "tip": 27.00, "color": discord.Color.from_rgb(0, 191, 255), "emoji": "<:rank17:1389367632309784606>"},
    {"tier": "Rank 18", "threshold": 25000, "tip": 27.00, "color": discord.Color.from_rgb(0, 191, 255), "emoji": "<:rank18:1389367634189095023>"},
    {"tier": "Rank 19", "threshold": 35000, "tip": 54.00, "color": discord.Color.from_rgb(138, 43, 226), "emoji": "<:rank19:1389367787972985026>"},
    {"tier": "Rank 20", "threshold": 50000, "tip": 81.00, "color": discord.Color.from_rgb(138, 43, 226), "emoji": "<:rank20:1389367789894238340>"},
    {"tier": "Rank 21", "threshold": 75000, "tip": 135.00, "color": discord.Color.from_rgb(255, 20, 147), "emoji": "<:rank21:1389367791852716073>"},
    {"tier": "Rank 22", "threshold": 100000, "tip": 149.00, "color": discord.Color.from_rgb(255, 20, 147), "emoji": "<:rank22:1389367794444931193>"},
    {"tier": "Rank 23", "threshold": 150000, "tip": 270.00, "color": discord.Color.from_rgb(255, 69, 0), "emoji": "<:rank23:1389367796646940772>"},
    {"tier": "Rank 24", "threshold": 200000, "tip": 270.00, "color": discord.Color.from_rgb(255, 69, 0), "emoji": "<:rank24:1389367804523708576>"},
    {"tier": "Rank 25", "threshold": 250000, "tip": 270.00, "color": discord.Color.from_rgb(255, 69, 0), "emoji": "<:rank25:1389367974036504688>"},
    {"tier": "Rank 26", "threshold": 350000, "tip": 540.00, "color": discord.Color.from_rgb(220, 20, 60), "emoji": "<:rank26:1389367976104558692>"},
    {"tier": "Rank 27", "threshold": 500000, "tip": 810.00, "color": discord.Color.from_rgb(220, 20, 60), "emoji": "<:rank27:1389367978419683442>"},
    {"tier": "Rank 28", "threshold": 650000, "tip": 810.00, "color": discord.Color.from_rgb(220, 20, 60), "emoji": "<:rank28:1389367980793659432>"},
    {"tier": "Rank 29", "threshold": 800000, "tip": 810.00, "color": discord.Color.from_rgb(220, 20, 60), "emoji": "<:rank29:1389367970026754170>"},
    {"tier": "Rank 30", "threshold": 1000000, "tip": 1080.00, "color": discord.Color.from_rgb(255, 215, 0), "emoji": "<:rank30:1389367972090351626>"}
]

class Milestones(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tip_queue = asyncio.Queue()
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
        sent_tips = load_sent_tips(month, year)
        
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

    @app_commands.command(name="ranks", description="Display the top 10 players and their current milestone ranks")
    async def ranks_command(self, interaction: discord.Interaction):
        """Display top 10 players with their current ranks based on weighted wager"""
        await interaction.response.defer()
        
        # Get data from DataManager
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data not available. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
            
        weighted_wager_data = cached_data.get('weighted_wager', [])
        if not weighted_wager_data:
            await interaction.followup.send("❌ No wager data available.", ephemeral=True)
            return
        
        # Sort players by weighted wager (descending) and take top 10
        sorted_players = sorted(
            weighted_wager_data, 
            key=lambda x: x.get("weightedWagered", 0), 
            reverse=True
        )[:10]
        
        # Build the embed description
        desc = "🏆 **Top 10 Players by Milestone Rank** 🏆\n\n"
        
        for i, player in enumerate(sorted_players, 1):
            username = player.get("username", "Unknown")
            weighted_wagered = player.get("weightedWagered", 0)
            
            # Find the player's current rank
            current_rank = None
            for milestone in reversed(MILESTONES):  # Check from highest to lowest
                if weighted_wagered >= milestone["threshold"]:
                    current_rank = milestone
                    break
            
            if current_rank:
                rank_emoji = current_rank["emoji"]
                rank_name = current_rank["tier"]
                desc += f"**{i}.** {rank_emoji} **{username}** - {rank_name}\n"
                desc += f"    💰 **${weighted_wagered:,.2f}** wagered\n\n"
            else:
                # Player hasn't reached any milestone yet
                desc += f"**{i}.** 🎰 **{username}** - No Rank Yet\n"
                desc += f"    💰 **${weighted_wagered:,.2f}** wagered\n\n"
        
        embed = discord.Embed(
            title="🎖️ __Milestone Rankings Leaderboard__ 🎖️",
            description=desc,
            color=discord.Color.gold()
        )
        
        # Add footer with update info
        now = datetime.now(dt.UTC)
        embed.set_footer(text=f"Updated: {now.strftime('%Y-%m-%d %H:%M:%S')} GMT")
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(Milestones(bot))
