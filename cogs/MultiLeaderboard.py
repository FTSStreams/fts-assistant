import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import get_current_week_range, fetch_weighted_wager, send_tip
from db import get_leaderboard_message_id, save_leaderboard_message_id, save_tip_log, get_db_connection, release_db_connection
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
MULTI_LEADERBOARD_CHANNEL_ID = int(os.getenv("MULTI_LEADERBOARD_CHANNEL_ID"))  # No default, must be set in env
if not MULTI_LEADERBOARD_CHANNEL_ID:
    raise RuntimeError("MULTI_LEADERBOARD_CHANNEL_ID environment variable must be set!")
PRIZE_DISTRIBUTION = [20, 15, 5]  # Weekly prizes: $20, $15, $5

class MultiLeaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.update_multi_leaderboard.start()
        self.weekly_payout_check.start()  # New task for weekly payouts

    def get_data_manager(self):
        """Get the DataManager cog"""
        return self.bot.get_cog('DataManager')

    @tasks.loop(minutes=10)
    async def update_multi_leaderboard(self):
        logger.info("[MultiLeaderboard] Starting weekly multiplier leaderboard update cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        channel = self.bot.get_channel(MULTI_LEADERBOARD_CHANNEL_ID)
        if not channel:
            logger.error("MultiLeaderboard channel not found.")
            return
        
        # Get current week range (Monday to Sunday)
        start_date, end_date = get_current_week_range()
        logger.info(f"[MultiLeaderboard] Fetching weekly data from {start_date} to {end_date}")
        
        try:
            # Fetch weekly weighted wager data directly
            weekly_weighted_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
        except Exception as e:
            logger.error(f"[MultiLeaderboard] Error fetching weekly data: {e}")
            return
        
        # Filter and sort by highestMultiplier
        multi_data = [entry for entry in weekly_weighted_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
        multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
        embed = discord.Embed(
            title="🏆 **Weekly Top Multipliers Leaderboard** 🏆",
            description=(
                f"**Weekly Competition Period:**\n"
                f"From: <t:{int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())}:F>\n"
                f"To: <t:{int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "This leaderboard ranks users by their highest single multiplier hit this week.\n"
                "**Resets every Monday at 12:00 AM UTC**\n\n"
                "💵 **All amounts displayed are in USD.**\n\n"
            ),
            color=discord.Color.purple()
        )
        for i in range(3):  # Only top 3 for weekly competition
            if i < len(multi_data):
                entry = multi_data[i]
                username = entry.get("username", "Unknown")
                # Censor username and escape asterisks to prevent Discord markdown issues
                if len(username) > 3:
                    username = username[:-3] + "\\*\\*\\*"
                else:
                    username = "\\*\\*\\*"
                multiplier = entry["highestMultiplier"].get("multiplier", 0)
                game = entry["highestMultiplier"].get("gameTitle", "Unknown")
                game_identifier = entry["highestMultiplier"].get("gameIdentifier", None)
                wagered = entry["highestMultiplier"].get("wagered", 0)
                payout = entry["highestMultiplier"].get("payout", 0)
                prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
            else:
                username = "N/A"
                multiplier = 0
                game = "Unknown"
                game_identifier = None
                wagered = 0
                payout = 0
                prize = PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
            # Hyperlink the game if identifier exists
            if game_identifier:
                game_url = f"https://roobet.com/casino/game/{game_identifier}"
                game_display = f"[{game}]({game_url})"
            else:
                game_display = game
            embed.add_field(
                name=f"**#{i + 1} - {username}**",
                value=(
                    f"💥 **Highest Multiplier:** `x{multiplier:,.2f}`\n"
                    f"🎮 **Game:** {game_display}\n"
                    f"💰 **Payout:** `${payout:,.2f}` (`${wagered:,.2f}` Base Bet)\n"
                    f"🎁 **Prize:** `${prize} USD`"
                ),
                inline=False
            )
        embed.set_footer(text="Our automated reward distribution system tips winners every Sunday at 11:59 PM UTC.")
        
        # Prepare JSON data for export (weekly format)
        leaderboard_json = {
            "leaderboard_type": "weekly_multiplier",
            "period": {
                "start": start_date,
                "end": end_date,
                "start_timestamp": int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp()),
                "end_timestamp": int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())
            },
            "last_updated": datetime.now(dt.UTC).isoformat(),
            "last_updated_timestamp": int(datetime.now(dt.UTC).timestamp()),
            "entries": []
        }
        
        # Add top 3 entries to JSON
        for i in range(3):
            if i < len(multi_data):
                entry = multi_data[i]
                username = entry.get("username", "Unknown")
                # Apply username masking for JSON export too and escape asterisks
                if len(username) > 3:
                    masked_username = username[:-3] + "\\*\\*\\*"
                else:
                    masked_username = "\\*\\*\\*"
                
                leaderboard_json["entries"].append({
                    "rank": i + 1,
                    "username": masked_username,
                    "multiplier": entry["highestMultiplier"].get("multiplier", 0),
                    "game": entry["highestMultiplier"].get("gameTitle", "Unknown"),
                    "game_identifier": entry["highestMultiplier"].get("gameIdentifier", None),
                    "wagered": entry["highestMultiplier"].get("wagered", 0),
                    "payout": entry["highestMultiplier"].get("payout", 0),
                    "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
                })
            else:
                # Add empty slot for consistent structure
                leaderboard_json["entries"].append({
                    "rank": i + 1,
                    "username": "N/A",
                    "multiplier": 0,
                    "game": "Unknown",
                    "game_identifier": None,
                    "wagered": 0,
                    "payout": 0,
                    "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
                })
        
        # All JSON uploads are now handled by DataManager
        # The DataManager will generate and upload multiplier leaderboard data automatically
        
        # Post or update the leaderboard message
        # Use a unique key for the multi leaderboard message
        message_id = get_leaderboard_message_id(key="multi_leaderboard_message_id")
        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
                logger.info("[MultiLeaderboard] Leaderboard message updated.")
            except discord.errors.NotFound:
                logger.warning(f"MultiLeaderboard message ID {message_id} not found, sending new message.")
                try:
                    message = await channel.send(embed=embed)
                    save_leaderboard_message_id(message.id, key="multi_leaderboard_message_id")
                    logger.info("[MultiLeaderboard] New leaderboard message sent.")
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in MultiLeaderboard channel.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit messages in MultiLeaderboard channel.")
        else:
            logger.info("[MultiLeaderboard] No leaderboard message ID found, sending new message.")
            try:
                message = await channel.send(embed=embed)
                save_leaderboard_message_id(message.id, key="multi_leaderboard_message_id")
                logger.info("[MultiLeaderboard] New leaderboard message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in MultiLeaderboard channel.")

    @tasks.loop(minutes=1)  # Check every minute for precise timing
    async def weekly_payout_check(self):
        """Check if it's time for weekly multiplier payouts (Sunday 11:59 PM UTC)"""
        now = datetime.now(dt.UTC)
        
        # FIXED: Exact time check - Sunday at exactly 23:59 UTC
        if now.weekday() == 6 and now.hour == 23 and now.minute == 59:  # Sunday = 6, exactly 23:59
            logger.info("[MultiLeaderboard] Sunday 11:59 PM UTC - Processing weekly payouts")
            await self.process_weekly_payouts()

    async def process_weekly_payouts(self):
        """Process payouts for the current week's top 3 multiplier winners"""
        try:
            # Get current week range
            start_date, end_date = get_current_week_range()
            week_key = f"{start_date[:10]}"  # Use start date as week identifier (YYYY-MM-DD)
            
            # IMPROVED: Use database lock to prevent duplicate processing
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    # Check if we already paid out this week with a lock
                    cur.execute("BEGIN;")
                    cur.execute(
                        "SELECT COUNT(*) FROM weekly_multiplier_payouts WHERE week_start = %s FOR UPDATE;",
                        (week_key,)
                    )
                    already_paid = cur.fetchone()[0] > 0
                    
                    if already_paid:
                        logger.info(f"[MultiLeaderboard] Week {week_key} already paid out, skipping")
                        cur.execute("ROLLBACK;")
                        return
                    
                    # Insert a lock record immediately to prevent race conditions
                    cur.execute("""
                        INSERT INTO weekly_multiplier_payouts 
                        (week_start, rank, user_id, username, prize_amount, multiplier, game_name)
                        VALUES (%s, 0, 0, 'PROCESSING_LOCK', 0, 0, 'LOCK_RECORD')
                    """, (week_key,))
                    cur.execute("COMMIT;")
                    logger.info(f"[MultiLeaderboard] Acquired processing lock for week {week_key}")
                    
            except Exception as e:
                # Table might not exist yet, create it
                logger.info(f"[MultiLeaderboard] Creating weekly_multiplier_payouts table: {e}")
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS weekly_multiplier_payouts (
                                id SERIAL PRIMARY KEY,
                                week_start DATE NOT NULL,
                                rank INTEGER NOT NULL,
                                user_id BIGINT NOT NULL,
                                username VARCHAR(255) NOT NULL,
                                prize_amount DECIMAL(10,2) NOT NULL,
                                multiplier DECIMAL(10,2) NOT NULL,
                                game_name VARCHAR(255),
                                paid_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                                UNIQUE(week_start, rank)
                            );
                        """)
                        conn.commit()
                        logger.info("[MultiLeaderboard] Created weekly_multiplier_payouts table")
                        
                        # Insert lock record
                        cur.execute("""
                            INSERT INTO weekly_multiplier_payouts 
                            (week_start, rank, user_id, username, prize_amount, multiplier, game_name)
                            VALUES (%s, 0, 0, 'PROCESSING_LOCK', 0, 0, 'LOCK_RECORD')
                        """, (week_key,))
                        conn.commit()
                        
                except Exception as create_error:
                    logger.error(f"[MultiLeaderboard] Error creating table: {create_error}")
                    return
            finally:
                release_db_connection(conn)
            
            # Fetch weekly data and get top 3
            logger.info(f"[MultiLeaderboard] Fetching weekly data for payouts: {start_date} to {end_date}")
            weekly_weighted_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
            
            # Filter and sort by highest multiplier
            multi_data = [entry for entry in weekly_weighted_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
            multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
            
            # Process top 3 winners
            winners_processed = 0
            for i in range(min(3, len(multi_data))):
                entry = multi_data[i]
                user_id = entry.get("uid")
                username = entry.get("username", "Unknown")
                multiplier = entry["highestMultiplier"].get("multiplier", 0)
                game_name = entry["highestMultiplier"].get("gameTitle", "Unknown")
                prize_amount = PRIZE_DISTRIBUTION[i]
                
                if not user_id or not username:
                    continue
                
                # Send the tip
                logger.info(f"[MultiLeaderboard] Sending weekly prize: ${prize_amount} to {username} (Rank #{i+1})")
                tip_response = await send_tip(
                    user_id=os.getenv("ROOBET_USER_ID"),
                    to_username=username,
                    to_user_id=user_id,
                    amount=prize_amount
                )
                
                if tip_response.get("success"):
                    # Record the payout in database
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO weekly_multiplier_payouts 
                                (week_start, rank, user_id, username, prize_amount, multiplier, game_name)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (week_key, i+1, user_id, username, prize_amount, multiplier, game_name))
                            conn.commit()
                    finally:
                        release_db_connection(conn)
                    
                    # Also log to manualtips for tipstats inclusion
                    save_tip_log(
                        user_id,
                        username,
                        prize_amount,
                        "weekly_multiplier",
                        month=datetime.now(dt.UTC).month,
                        year=datetime.now(dt.UTC).year
                    )
                    
                    winners_processed += 1
                    logger.info(f"[MultiLeaderboard] Successfully paid ${prize_amount} to {username} for Rank #{i+1}")
                    
                else:
                    logger.error(f"[MultiLeaderboard] Failed to tip {username}: {tip_response.get('message')}")
                
                # INCREASED: 30 second delay between tips to ensure processing
                await asyncio.sleep(30)
            
            # Send summary to logs channel if any winners were processed
            if winners_processed > 0:
                logs_channel_id = int(os.getenv("WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID", "0"))  # New env var for weekly multiplier payouts
                if logs_channel_id:
                    logs_channel = self.bot.get_channel(logs_channel_id)
                    if logs_channel:
                        embed = discord.Embed(
                            title="🏆 Weekly Multiplier Leaderboard Payouts Complete!",
                            description=f"**Week of {week_key}**\n\n"
                                      f"✅ **{winners_processed} winners** have been paid their prizes!\n"
                                      f"💰 **Total Distributed**: ${sum(PRIZE_DISTRIBUTION[:winners_processed]):.2f} USD",
                            color=discord.Color.green()
                        )
                        
                        for i in range(winners_processed):
                            entry = multi_data[i]
                            username = entry.get("username", "Unknown")
                            # Censor username for public display
                            if len(username) > 3:
                                display_username = username[:-3] + "\\*\\*\\*"
                            else:
                                display_username = "\\*\\*\\*"
                            
                            multiplier = entry["highestMultiplier"].get("multiplier", 0)
                            game_name = entry["highestMultiplier"].get("gameTitle", "Unknown")
                            prize = PRIZE_DISTRIBUTION[i]
                            
                            embed.add_field(
                                name=f"🥇 Rank #{i+1} - {display_username}",
                                value=f"x{multiplier:.2f} on {game_name}\n💸 Prize: ${prize} USD",
                                inline=False
                            )
                        
                        embed.set_footer(text=f"Next weekly competition starts Monday 12:00 AM UTC")
                        await logs_channel.send(embed=embed)
                        
            # Clean up the processing lock record
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM weekly_multiplier_payouts WHERE week_start = %s AND rank = 0 AND username = 'PROCESSING_LOCK'",
                        (week_key,)
                    )
                    conn.commit()
                    logger.info(f"[MultiLeaderboard] Cleaned up processing lock for week {week_key}")
            finally:
                release_db_connection(conn)
                        
            logger.info(f"[MultiLeaderboard] Weekly payout process completed. {winners_processed} winners processed.")
            
        except Exception as e:
            logger.error(f"[MultiLeaderboard] Error in weekly payout process: {e}")
            import traceback
            logger.error(f"[MultiLeaderboard] Traceback: {traceback.format_exc()}")

    @app_commands.command(name="testmultihistory", description="Simulate current weekly multiplier leaderboard and payout timing (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def test_multi_history(self, interaction: discord.Interaction):
        """Test command to show current weekly leaderboard state and payout timing"""
        await interaction.response.defer()
        
        try:
            # Get current time and week information
            now = datetime.now(dt.UTC)
            start_date, end_date = get_current_week_range()
            
            # Calculate time until next payout (Sunday 11:59 PM UTC)
            days_until_sunday = (6 - now.weekday()) % 7  # 6 = Sunday
            if now.weekday() == 6:  # If it's already Sunday
                if now.hour < 23 or (now.hour == 23 and now.minute < 59):
                    # Payout is later today
                    next_payout = now.replace(hour=23, minute=59, second=0, microsecond=0)
                else:
                    # Payout is next week
                    next_payout = now + dt.timedelta(days=7)
                    next_payout = next_payout.replace(hour=23, minute=59, second=0, microsecond=0)
            else:
                # Payout is this coming Sunday
                next_payout = now + dt.timedelta(days=days_until_sunday)
                next_payout = next_payout.replace(hour=23, minute=59, second=0, microsecond=0)
            
            time_until_payout = next_payout - now
            hours_until = int(time_until_payout.total_seconds() // 3600)
            minutes_until = int((time_until_payout.total_seconds() % 3600) // 60)
            
            # Check if this week was already paid out
            week_key = f"{start_date[:10]}"
            conn = get_db_connection()
            already_paid = False
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM weekly_multiplier_payouts WHERE week_start = %s AND rank > 0",
                        (week_key,)
                    )
                    already_paid = cur.fetchone()[0] > 0
            except:
                # Table might not exist
                pass
            finally:
                release_db_connection(conn)
            
            # Fetch current weekly data
            logger.info(f"[TestMultiHistory] Fetching weekly data: {start_date} to {end_date}")
            weekly_weighted_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
            
            # Filter and sort by highest multiplier
            multi_data = [entry for entry in weekly_weighted_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
            multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
            
            # Create the simulation embed
            embed = discord.Embed(
                title="🧪 **Weekly Multiplier Leaderboard Simulation** 🧪",
                color=discord.Color.blue()
            )
            
            # Add timing information
            payout_status = "✅ ALREADY PAID OUT" if already_paid else f"⏰ Payouts in {hours_until}h {minutes_until}m"
            
            # Parse dates safely (handle microseconds)
            try:
                start_timestamp = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())
            except:
                start_timestamp = int(datetime.strptime(start_date[:19] + '+00:00', '%Y-%m-%dT%H:%M:%S%z').timestamp())
            
            try:
                end_timestamp = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())
            except:
                end_timestamp = int(datetime.strptime(end_date[:19] + '+00:00', '%Y-%m-%dT%H:%M:%S%z').timestamp())
            
            embed.add_field(
                name="📅 **Current Week Info**",
                value=(
                    f"**Week Period**: <t:{start_timestamp}:d> - <t:{end_timestamp}:d>\n"
                    f"**Next Payout**: <t:{int(next_payout.timestamp())}:F>\n"
                    f"**Status**: {payout_status}\n"
                    f"**Current Time**: <t:{int(now.timestamp())}:F>"
                ),
                inline=False
            )
            
            # Add current leaderboard
            if multi_data:
                leaderboard_text = ""
                for i in range(min(3, len(multi_data))):
                    entry = multi_data[i]
                    username = entry.get("username", "Unknown")
                    # Don't censor for admin command
                    multiplier = entry["highestMultiplier"].get("multiplier", 0)
                    game = entry["highestMultiplier"].get("gameTitle", "Unknown")
                    wagered = entry["highestMultiplier"].get("wagered", 0)
                    payout = entry["highestMultiplier"].get("payout", 0)
                    prize = PRIZE_DISTRIBUTION[i]
                    
                    medal = ["🥇", "🥈", "🥉"][i]
                    leaderboard_text += (
                        f"{medal} **#{i+1} - {username}**\n"
                        f"   💥 Multiplier: `x{multiplier:,.2f}`\n"
                        f"   🎮 Game: {game}\n"
                        f"   💰 Bet: `${wagered:,.2f}` → Payout: `${payout:,.2f}`\n"
                        f"   🎁 Prize: `${prize} USD`\n\n"
                    )
                
                embed.add_field(
                    name="🏆 **Current Top 3 Winners**",
                    value=leaderboard_text or "No qualifiers yet",
                    inline=False
                )
            else:
                embed.add_field(
                    name="🏆 **Current Top 3 Winners**",
                    value="No multiplier data found for this week",
                    inline=False
                )
            
            # Add payout simulation details
            if not already_paid and multi_data:
                total_payout = sum(PRIZE_DISTRIBUTION[:min(3, len(multi_data))])
                simulation_text = (
                    f"**Total Payout**: ${total_payout} USD\n"
                    f"**Processing Time**: ~90 seconds (30s delays between tips)\n"
                    f"**Announcement Channel**: <#{os.getenv('WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID', 'NOT_SET')}>\n"
                    f"**Database Lock**: Will prevent duplicate payouts\n"
                    f"**Tip Type**: `weekly_multiplier` in tipstats"
                )
                embed.add_field(
                    name="💸 **Payout Simulation**",
                    value=simulation_text,
                    inline=False
                )
            elif already_paid:
                embed.add_field(
                    name="✅ **Payout Complete**",
                    value="This week's winners have already been paid out.",
                    inline=False
                )
            
            # Add technical details
            embed.add_field(
                name="🔧 **Technical Details**",
                value=(
                    f"**Week Key**: `{week_key}`\n"
                    f"**Data Entries**: {len(weekly_weighted_data)} total users\n"
                    f"**Qualified Players**: {len(multi_data)} with multipliers\n"
                    f"**Current Day**: {now.strftime('%A')} (Sunday = Payout Day)\n"
                    f"**Timezone**: UTC"
                ),
                inline=False
            )
            
            embed.set_footer(text=f"Simulation run at {now.strftime('%Y-%m-%d %H:%M:%S')} UTC • Admin Only Command")
            
            await interaction.followup.send(embed=embed)
            logger.info(f"[TestMultiHistory] Simulation command executed by {interaction.user}")
            
        except Exception as e:
            logger.error(f"[TestMultiHistory] Error in simulation command: {e}")
            await interaction.followup.send(f"❌ Error running simulation: {str(e)}", ephemeral=True)

    @weekly_payout_check.before_loop
    async def before_weekly_payout_check(self):
        await self.bot.wait_until_ready()

    @update_multi_leaderboard.before_loop
    async def before_multi_leaderboard_loop(self):
        await self.bot.wait_until_ready()

    def cog_unload(self):
        self.update_multi_leaderboard.cancel()
        self.weekly_payout_check.cancel()

async def setup(bot):
    await bot.add_cog(MultiLeaderboard(bot))
