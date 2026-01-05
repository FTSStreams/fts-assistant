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
        self.last_payout_week = None  # Track last week we processed payouts for
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
        # Calculate next Friday 12:00 AM UTC for reset timestamp
        now = datetime.now(dt.UTC)
        days_until_friday = (4 - now.weekday()) % 7  # Friday is weekday 4
        if days_until_friday == 0:  # If it's Friday, get next Friday
            days_until_friday = 7
        next_friday = now + dt.timedelta(days=days_until_friday)
        next_friday = next_friday.replace(hour=0, minute=0, second=0, microsecond=0)
        
        embed = discord.Embed(
            title="🏆 **Weekly Top Multipliers Leaderboard** 🏆",
            description=(
                f"**Weekly Competition Period:**\n"
                f"From: <t:{int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())}:F>\n"
                f"To: <t:{int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "This leaderboard ranks users by their highest single multiplier hit this week.\n"
                f"**Resets:** <t:{int(next_friday.timestamp())}:F>\n\n"
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
        embed.set_footer(text="Our automated reward distribution system tips winners every Friday at 12:00 AM UTC.")
        
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

    @tasks.loop(minutes=5)  # Check every 5 minutes for weekly payout
    async def weekly_payout_check(self):
        """Check if it's time for weekly multiplier payouts (Friday 00:00 UTC)"""
        try:
            now = datetime.now(dt.UTC)
            
            # Check if we're within the payout window: Friday 00:00-00:04 UTC
            is_friday = now.weekday() == 4  # Friday = 4
            is_payout_time = now.hour == 0 and 0 <= now.minute <= 4
            
            # Debug logging - log every check during the critical window
            if is_friday and now.hour == 0 and -1 <= now.minute <= 5:
                logger.info(f"[MultiLeaderboard] Friday 00:XX UTC - Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC - Minute: {now.minute}")
            
            if not (is_friday and is_payout_time):
                return
            
            # Get the week identifier (Monday of current week)
            current_week_start, _ = get_current_week_range()
            current_week_key = current_week_start[:10]  # YYYY-MM-DD format
            
            logger.info(f"[MultiLeaderboard] ⏰ PAYOUT WINDOW DETECTED! Time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC, Week: {current_week_key}")
            
            # Prevent duplicate processing of same week
            if self.last_payout_week == current_week_key:
                logger.info(f"[MultiLeaderboard] Week {current_week_key} already processed locally, skipping")
                return
            
            logger.info(f"[MultiLeaderboard] Checking database for previous payouts on week {current_week_key}")
            
            # Check database to see if we've already paid out this week
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM weekly_multiplier_payouts WHERE week_start = %s AND rank > 0;",
                        (current_week_key,)
                    )
                    result = cur.fetchone()
                    already_paid = result[0] > 0 if result else False
                    
                    if already_paid:
                        logger.info(f"[MultiLeaderboard] Week {current_week_key} already paid out in database ({result[0]} records), skipping")
                        self.last_payout_week = current_week_key
                        return
                    else:
                        logger.info(f"[MultiLeaderboard] No previous payouts found for week {current_week_key}, proceeding with payout")
            except Exception as e:
                logger.warning(f"[MultiLeaderboard] Database check failed (table may not exist yet): {e}, attempting to create table")
            finally:
                release_db_connection(conn)
            
            # Process the payouts
            logger.info(f"[MultiLeaderboard] 🚀 EXECUTING WEEKLY PAYOUTS NOW!")
            await self.process_weekly_payouts()
            
            # Mark week as processed locally
            self.last_payout_week = current_week_key
            logger.info(f"[MultiLeaderboard] ✅ PAYOUT PROCESS COMPLETED for week {current_week_key}")
            
        except Exception as e:
            logger.error(f"[MultiLeaderboard] ERROR in weekly_payout_check: {e}", exc_info=True)

    async def process_weekly_payouts(self):
        """Process payouts for the current week's top 3 multiplier winners"""
        try:
            # Get current week range
            start_date, end_date = get_current_week_range()
            week_key = f"{start_date[:10]}"  # Use start date as week identifier (YYYY-MM-DD)
            
            # First, ensure the table exists
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS weekly_multiplier_payouts (
                            id SERIAL PRIMARY KEY,
                            week_start DATE NOT NULL,
                            rank INTEGER NOT NULL,
                            user_id VARCHAR(255) NOT NULL,
                            username VARCHAR(255) NOT NULL,
                            prize_amount DECIMAL(10,2) NOT NULL,
                            multiplier DECIMAL(10,2) NOT NULL,
                            game_name VARCHAR(255),
                            paid_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            UNIQUE(week_start, rank)
                        );
                    """)
                    conn.commit()
                    logger.info("[MultiLeaderboard] ✅ Ensured weekly_multiplier_payouts table exists")
            except Exception as e:
                logger.warning(f"[MultiLeaderboard] ⚠️ Could not create table: {e}")
                conn.rollback()
            finally:
                release_db_connection(conn)
            
            # Now check if already paid out this week
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM weekly_multiplier_payouts WHERE week_start = %s AND rank > 0;",
                        (week_key,)
                    )
                    result = cur.fetchone()
                    already_paid = result[0] > 0 if result else False
                    
                    if already_paid:
                        logger.info(f"[MultiLeaderboard] Week {week_key} already paid out in database ({result[0]} records), skipping")
                        return
                    else:
                        logger.info(f"[MultiLeaderboard] No previous payouts found for week {week_key}, proceeding with payout")
            except Exception as e:
                logger.error(f"[MultiLeaderboard] Error checking database: {e}")
            finally:
                release_db_connection(conn)
            
            # Fetch weekly data and get top 3
            logger.info(f"[MultiLeaderboard] 📊 Fetching weekly data for payouts: {start_date} to {end_date}")
            weekly_weighted_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
            logger.info(f"[MultiLeaderboard] 📊 Received {len(weekly_weighted_data)} entries from API")
            
            # Filter and sort by highest multiplier
            multi_data = [entry for entry in weekly_weighted_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
            multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
            logger.info(f"[MultiLeaderboard] 📊 Filtered to {len(multi_data)} entries with valid multipliers")
            
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
                    logger.warning(f"[MultiLeaderboard] ⚠️ Rank #{i+1} missing user_id or username, skipping")
                    continue
                
                # Send the tip
                logger.info(f"[MultiLeaderboard] 💸 Sending weekly prize: ${prize_amount} to {username} (Rank #{i+1}, x{multiplier:.2f})")
                tip_response = await send_tip(
                    user_id=os.getenv("ROOBET_USER_ID"),
                    to_username=username,
                    to_user_id=user_id,
                    amount=prize_amount
                )
                
                logger.info(f"[MultiLeaderboard] Tip response: {tip_response}")
                
                if tip_response.get("success"):
                    logger.info(f"[MultiLeaderboard] ✅ Tip SUCCESSFUL for {username}")
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
                            logger.info(f"[MultiLeaderboard] 💾 Recorded payout in database for {username}")
                    except Exception as db_error:
                        logger.error(f"[MultiLeaderboard] ❌ Failed to record payout in database: {db_error}")
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
                    logger.info(f"[MultiLeaderboard] 🏆 Successfully paid ${prize_amount} to {username} for Rank #{i+1}")
                    
                else:
                    logger.error(f"[MultiLeaderboard] ❌ FAILED to tip {username}: Response={tip_response}")
                
                # INCREASED: 30 second delay between tips to ensure processing
                logger.info(f"[MultiLeaderboard] ⏳ Waiting 30 seconds before next tip...")
                await asyncio.sleep(30)
            
            # Send summary to logs channel if any winners were processed
            if winners_processed > 0:
                logs_channel_id = int(os.getenv("WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID", "0"))  # New env var for weekly multiplier payouts
                if logs_channel_id:
                    logs_channel = self.bot.get_channel(logs_channel_id)
                    if logs_channel:
                        # Format the detailed winners list
                        winners_text = "**Winners:**\n\n"
                        for i in range(winners_processed):
                            entry = multi_data[i]
                            username = entry.get("username", "Unknown")
                            # Censor username for public display - use bullet points instead of asterisks to avoid markdown issues
                            if len(username) > 3:
                                display_username = username[:3] + "•••"
                            else:
                                display_username = "•••"
                            
                            multiplier = entry["highestMultiplier"].get("multiplier", 0)
                            game_name = entry["highestMultiplier"].get("gameTitle", "Unknown")
                            wagered = entry["highestMultiplier"].get("wagered", 0)
                            payout = entry["highestMultiplier"].get("payout", 0)
                            prize = PRIZE_DISTRIBUTION[i]
                            
                            medal = ["🥇", "🥈", "🥉"][i]
                            place = ["1st", "2nd", "3rd"][i]
                            
                            winners_text += (
                                f"{medal} **{place} Place:** @{display_username} - **x{multiplier:,.2f} multiplier** → **${prize:.2f}**\n"
                                f"   🎮 Game: {game_name}\n"
                                f"   💰 Bet: ${wagered:,.2f} | Payout: ${payout:,.2f}\n\n"
                            )
                        
                        # Create the embed with the detailed format
                        # Convert week_key (YYYY-MM-DD) to timestamp for start of week
                        try:
                            week_start_dt = datetime.fromisoformat(week_key + "T00:00:00+00:00")
                            week_start_ts = int(week_start_dt.timestamp())
                            # Week ends on Sunday, so add 6 days
                            week_end_dt = week_start_dt + dt.timedelta(days=6)
                            week_end_dt = week_end_dt.replace(hour=23, minute=59, second=59)
                            week_end_ts = int(week_end_dt.timestamp())
                        except:
                            week_start_ts = None
                            week_end_ts = None
                        
                        if week_start_ts and week_end_ts:
                            description = f"**Week Period:** <t:{week_start_ts}:F> → <t:{week_end_ts}:F>\n\n{winners_text}*Payouts completed successfully via Roobet affiliate system*"
                        else:
                            description = f"**Week of {week_key}**\n\n{winners_text}*Payouts completed successfully via Roobet affiliate system*"
                        
                        embed = discord.Embed(
                            title="🏆 Weekly Multiplier Leaderboard Payouts",
                            description=description,
                            color=discord.Color.green()
                        )
                        
                        embed.set_footer(text=f"Next weekly competition starts Friday 12:00 AM UTC")
                        
                        # Ping the notification role if configured
                        ping_role_id = os.getenv("WEEKLY_MULTIPLIER_PING_ROLE_ID")
                        content = f"<@&{ping_role_id}>" if ping_role_id else None
                        await logs_channel.send(content=content, embed=embed)
                        logger.info(f"[MultiLeaderboard] 📢 Posted payout summary to logs channel")
                    else:
                        logger.warning(f"[MultiLeaderboard] ⚠️ Logs channel {logs_channel_id} not found, cannot post summary")
                else:
                    logger.warning(f"[MultiLeaderboard] ⚠️ WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID not configured, skipping summary post")
            else:
                logger.warning(f"[MultiLeaderboard] ⚠️ No winners processed, skipping summary post to logs channel")
                        
            # Clean up the processing lock record
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM weekly_multiplier_payouts WHERE week_start = %s AND rank = 0 AND username = 'PROCESSING_LOCK'",
                        (week_key,)
                    )
                    conn.commit()
                    logger.info(f"[MultiLeaderboard] 🧹 Cleaned up processing lock for week {week_key}")
            except Exception as cleanup_error:
                logger.warning(f"[MultiLeaderboard] Failed to clean up lock record: {cleanup_error}")
            finally:
                release_db_connection(conn)
                        
            logger.info(f"[MultiLeaderboard] ✅✅✅ WEEKLY PAYOUT PROCESS COMPLETED. {winners_processed} winners processed. ✅✅✅")
            
        except Exception as e:
            logger.error(f"[MultiLeaderboard] ❌❌❌ CRITICAL ERROR in weekly payout process: {e}", exc_info=True)
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
            
            # Calculate time until next payout (Friday 00:00 UTC)
            days_until_friday = (4 - now.weekday()) % 7  # Friday = 4
            if now.weekday() == 4:  # If it's already Friday
                if now.hour == 0 and now.minute < 4:
                    # Payout window is now
                    next_payout = now.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    # Payout is next week
                    next_payout = now + dt.timedelta(days=7)
                    next_payout = next_payout.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                # Payout is this coming Friday
                next_payout = now + dt.timedelta(days=days_until_friday)
                next_payout = next_payout.replace(hour=0, minute=0, second=0, microsecond=0)
            
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
            
            # Add current leaderboard in EXACT same format as real payout logs
            if multi_data:
                # Show exact format that will appear in the logs channel
                week_key_for_display = start_date[:10]  # Same format as real payout
                winners_preview = f"🏆 **Weekly Multiplier Leaderboard Payouts**\n**Week of {week_key_for_display}**\n\n**Winners:**\n\n"
                
                for i in range(min(3, len(multi_data))):
                    entry = multi_data[i]
                    username = entry.get("username", "Unknown")
                    # Show uncensored for admin, but note what will be censored
                    display_username = username
                    if len(username) > 3:
                        censored_username = username[:-3] + "***"
                    else:
                        censored_username = "***"
                    
                    multiplier = entry["highestMultiplier"].get("multiplier", 0)
                    game = entry["highestMultiplier"].get("gameTitle", "Unknown")
                    wagered = entry["highestMultiplier"].get("wagered", 0)
                    payout = entry["highestMultiplier"].get("payout", 0)
                    prize = PRIZE_DISTRIBUTION[i]
                    
                    medal = ["🥇", "🥈", "🥉"][i]
                    place = ["1st", "2nd", "3rd"][i]
                    
                    winners_preview += (
                        f"{medal} **{place} Place:** @{display_username} - **x{multiplier:,.2f} multiplier** → **${prize:.2f}**\n"
                        f"   🎮 Game: {game}\n"
                        f"   💰 Bet: ${wagered:,.2f} | Payout: ${payout:,.2f}\n\n"
                    )
                
                winners_preview += "*Payouts completed successfully via Roobet affiliate system*"
                
                embed.add_field(
                    name="📋 **Exact Log Preview (What Will Be Posted)**",
                    value=winners_preview[:1024],  # Discord field limit
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
                    f"**Current Day**: {now.strftime('%A')} (Friday = Payout Day)\n"
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

    @app_commands.command(name="payoutmultilb", description="Test weekly payout with temporary amounts (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def test_payout_multilb(self, interaction: discord.Interaction):
        """Test command to simulate and execute weekly payout with test amounts ($3, $2, $1)"""
        await interaction.response.defer()
        
        test_prizes = [3, 2, 1]  # Test amounts instead of $20, $15, $5
        
        try:
            logger.info(f"[TestPayoutMultiLB] Starting test payout by {interaction.user}")
            
            # Get current week range
            start_date, end_date = get_current_week_range()
            week_key = f"{start_date[:10]}"
            now = datetime.now(dt.UTC)
            
            # Fetch weekly data and get top 3
            logger.info(f"[TestPayoutMultiLB] Fetching weekly data for test payout: {start_date} to {end_date}")
            weekly_weighted_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
            
            # Filter and sort by highest multiplier
            multi_data = [entry for entry in weekly_weighted_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
            multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
            
            # Process top 3 winners with test amounts
            winners_processed = 0
            winners_data = []
            
            for i in range(min(3, len(multi_data))):
                entry = multi_data[i]
                user_id = entry.get("uid")
                username = entry.get("username", "Unknown")
                multiplier = entry["highestMultiplier"].get("multiplier", 0)
                game_name = entry["highestMultiplier"].get("gameTitle", "Unknown")
                prize_amount = test_prizes[i]  # Use test amounts
                
                if not user_id or not username:
                    continue
                
                # Send the tip
                logger.info(f"[TestPayoutMultiLB] Sending test prize: ${prize_amount} to {username} (Rank #{i+1})")
                tip_response = await send_tip(
                    user_id=os.getenv("ROOBET_USER_ID"),
                    to_username=username,
                    to_user_id=user_id,
                    amount=prize_amount
                )
                
                if tip_response.get("success"):
                    # Store winner data for logging
                    winners_data.append({
                        "rank": i + 1,
                        "username": username,
                        "multiplier": multiplier,
                        "game_name": game_name,
                        "wagered": entry["highestMultiplier"].get("wagered", 0),
                        "payout": entry["highestMultiplier"].get("payout", 0),
                        "prize": prize_amount
                    })
                    
                    winners_processed += 1
                    logger.info(f"[TestPayoutMultiLB] Successfully sent test tip ${prize_amount} to {username} for Rank #{i+1}")
                else:
                    logger.error(f"[TestPayoutMultiLB] Failed to tip {username}: {tip_response.get('message')}")
                
                # 30 second delay between tips
                await asyncio.sleep(30)
            
            # Send summary to logs channel
            if winners_processed > 0:
                logs_channel_id = int(os.getenv("WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID", "0"))
                if logs_channel_id:
                    logs_channel = self.bot.get_channel(logs_channel_id)
                    if logs_channel:
                        # Format the detailed winners list - EXACT SAME FORMAT AS REAL PAYOUT
                        winners_text = ""
                        for winner in winners_data:
                            username = winner["username"]
                            # Censor username for public display - use bullet points instead of asterisks to avoid markdown issues
                            if len(username) > 3:
                                display_username = username[:3] + "•••"
                            else:
                                display_username = "•••"
                            
                            multiplier = winner["multiplier"]
                            game_name = winner["game_name"]
                            wagered = winner["wagered"]
                            payout = winner["payout"]
                            prize = winner["prize"]
                            rank = winner["rank"]
                            
                            medal = ["🥇", "🥈", "🥉"][rank - 1]
                            place = ["1st", "2nd", "3rd"][rank - 1]
                            
                            winners_text += (
                                f"{medal} **{place} Place:** @{display_username} - **x{multiplier:,.2f} multiplier** → **${prize:.2f}**\n"
                                f"   🎮 Game: {game_name}\n"
                                f"   💰 Bet: ${wagered:,.2f} | Payout: ${payout:,.2f}\n\n"
                            )
                        
                        # Create the embed with the EXACT SAME format as real payout
                        # Convert week_key (YYYY-MM-DD) to timestamp for start of week
                        try:
                            week_start_dt = datetime.fromisoformat(week_key + "T00:00:00+00:00")
                            week_start_ts = int(week_start_dt.timestamp())
                            # Week ends on Sunday, so add 6 days
                            week_end_dt = week_start_dt + dt.timedelta(days=6)
                            week_end_dt = week_end_dt.replace(hour=23, minute=59, second=59)
                            week_end_ts = int(week_end_dt.timestamp())
                        except:
                            week_start_ts = None
                            week_end_ts = None
                        
                        if week_start_ts and week_end_ts:
                            description = f"**Week Period:** <t:{week_start_ts}:F> → <t:{week_end_ts}:F>\n\n{winners_text}*Payouts completed successfully via Roobet affiliate system*"
                        else:
                            description = f"**Week of {week_key}**\n\n{winners_text}*Payouts completed successfully via Roobet affiliate system*"
                        
                        embed = discord.Embed(
                            title="🏆 Weekly Multiplier Leaderboard Payouts",
                            description=description,
                            color=discord.Color.green()
                        )
                        
                        embed.set_footer(text=f"Next weekly competition starts Friday 12:00 AM UTC")
                        
                        # Ping the notification role if configured
                        ping_role_id = os.getenv("WEEKLY_MULTIPLIER_PING_ROLE_ID")
                        content = f"<@&{ping_role_id}>" if ping_role_id else None
                        await logs_channel.send(content=content, embed=embed)
                        logger.info(f"[TestPayoutMultiLB] Test payout summary posted to logs channel")
            
            # Send confirmation to user
            confirmation = f"✅ Test payout completed! {winners_processed} winners processed with test amounts.\n"
            confirmation += f"Prizes: 1st: $3, 2nd: $2, 3rd: $1"
            await interaction.followup.send(confirmation, ephemeral=True)
            
        except Exception as e:
            logger.error(f"[TestPayoutMultiLB] Error in test payout: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Error running test payout: {str(e)}", ephemeral=True)

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
