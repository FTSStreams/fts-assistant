import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import get_current_week_range, fetch_weighted_wager, send_tip
from db import get_leaderboard_message_id, save_leaderboard_message_id, save_tip_log, get_db_connection, release_db_connection, get_setting_value, save_setting_value
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio
import json

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
MULTI_LEADERBOARD_CHANNEL_ID = int(os.getenv("MULTI_LEADERBOARD_CHANNEL_ID"))  # No default, must be set in env
WEEKLY_MULTIPLIER_LEADERBOARD_CHANNEL_ID = int(os.getenv("WEEKLY_MULTIPLIER_LEADERBOARD_CHANNEL_ID", "1352322188102991932"))
WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID = int(os.getenv("WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID", "1439815024565817434"))
WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID = int(os.getenv("WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID", "1440843895360590028"))
WEEKLY_MULTIPLIER_ALERT_STATE_KEY = "weekly_multiplier_leaderboard_alert_state"
if not MULTI_LEADERBOARD_CHANNEL_ID:
    raise RuntimeError("MULTI_LEADERBOARD_CHANNEL_ID environment variable must be set!")
PRIZE_DISTRIBUTION = [25, 15, 10]  # Weekly prizes: $25, $15, $10

class MultiLeaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.last_payout_week = None  # Track last week we processed payouts for
        self.update_multi_leaderboard.start()
        self.weekly_payout_check.start()  # New task for weekly payouts

    def get_data_manager(self):
        """Get the DataManager cog"""
        return self.bot.get_cog('DataManager')

    def _mask_public_username(self, username):
        if len(username) > 3:
            return username[:3] + "•••"
        return "•••"

    @staticmethod
    def _rank_label(rank):
        return {1: "1st", 2: "2nd", 3: "3rd"}.get(rank, f"#{rank}")

    @staticmethod
    def _rank_medal(rank):
        return {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, "🏅")

    @staticmethod
    def _snapshot_identity(entry):
        uid = entry.get("uid")
        if uid is not None and str(uid).strip():
            return str(uid)

        username = str(entry.get("username") or "").strip().lower()
        return f"user:{username}" if username else None

    def _load_leaderboard_alert_state(self, week_key):
        raw_state = get_setting_value(WEEKLY_MULTIPLIER_ALERT_STATE_KEY, default=None)
        if not raw_state:
            return {"week_key": week_key, "entries": []}

        try:
            parsed = json.loads(raw_state)
        except (TypeError, json.JSONDecodeError):
            logger.warning("[MultiLeaderboard] Invalid leaderboard alert state JSON; resetting state.")
            return {"week_key": week_key, "entries": []}

        if not isinstance(parsed, dict) or parsed.get("week_key") != week_key:
            return {"week_key": week_key, "entries": []}

        entries = parsed.get("entries")
        if not isinstance(entries, list):
            entries = []

        return {"week_key": week_key, "entries": entries}

    def _save_leaderboard_alert_state(self, state):
        save_setting_value(
            WEEKLY_MULTIPLIER_ALERT_STATE_KEY,
            json.dumps(state, separators=(",", ":")),
        )

    def _build_leaderboard_snapshot(self, multi_data):
        snapshot = []
        for i in range(min(3, len(multi_data))):
            entry = multi_data[i]
            highest_multiplier = entry.get("highestMultiplier") or {}
            snapshot.append({
                "rank": i + 1,
                "identity": self._snapshot_identity(entry),
                "uid": entry.get("uid"),
                "username": entry.get("username", "Unknown"),
                "multiplier": float(highest_multiplier.get("multiplier", 0) or 0),
                "game": highest_multiplier.get("gameTitle", "Unknown"),
                "wagered": float(highest_multiplier.get("wagered", 0) or 0),
                "payout": float(highest_multiplier.get("payout", 0) or 0),
            })
        return snapshot

    def _detect_leaderboard_changes(self, previous_entries, current_entries):
        previous_by_rank = {
            int(entry.get("rank")): entry
            for entry in previous_entries
            if isinstance(entry, dict) and entry.get("rank") is not None
        }
        previous_rank_by_identity = {
            entry.get("identity"): int(entry.get("rank"))
            for entry in previous_entries
            if isinstance(entry, dict) and entry.get("identity")
        }

        changes = []
        for current_entry in current_entries:
            current_rank = int(current_entry["rank"])
            current_identity = current_entry.get("identity")
            previous_entry = previous_by_rank.get(current_rank)
            previous_identity = previous_entry.get("identity") if previous_entry else None

            if current_identity != previous_identity:
                changes.append({
                    "rank": current_rank,
                    "current_entry": current_entry,
                    "previous_rank": previous_rank_by_identity.get(current_identity),
                    "previous_entry": previous_entry,
                })

        return changes

    def _build_weekly_leaderboard_change_embed(self, previous_entries, current_entries, week_start_ts, week_end_ts):
        prev_by_rank = {int(e["rank"]): e for e in previous_entries if isinstance(e, dict) and e.get("rank")}
        curr_by_rank = {int(e["rank"]): e for e in current_entries if isinstance(e, dict) and e.get("rank")}
        curr_identities = {e.get("identity") for e in current_entries if isinstance(e, dict) and e.get("identity")}

        # Find which ranks changed identity
        changed_ranks = [
            rank for rank in [1, 2, 3]
            if rank in curr_by_rank and curr_by_rank[rank].get("identity") != (prev_by_rank[rank].get("identity") if rank in prev_by_rank else None)
        ]
        if not changed_ranks:
            return None

        primary_rank = changed_ranks[0]
        primary_curr = curr_by_rank[primary_rank]
        primary_prev = prev_by_rank.get(primary_rank)
        primary_identity = primary_curr.get("identity")

        was_on_board = any(
            isinstance(e, dict) and e.get("identity") == primary_identity
            for e in previous_entries
        )

        # Determine header line
        if not primary_prev:
            header = "📈 A new leaderboard position has been claimed."
        elif not was_on_board:
            old_holder_still_in = primary_prev.get("identity") in curr_identities
            if old_holder_still_in:
                header = "🚨 Leaderboard change detected."
            else:
                header = "📈 A new user entered the leaderboard."
        else:
            header = "🚨 Leaderboard change detected."

        # Primary entry block
        username = self._mask_public_username(primary_curr.get("username", "Unknown"))
        medal = self._rank_medal(primary_rank)
        place = self._rank_label(primary_rank)
        primary_label = f"New {place}" if primary_prev else place
        entry_block = (
            f"{medal} **{primary_label}: @{username}**\n"
            f"💥 **Multiplier:** x{primary_curr.get('multiplier', 0):,.2f}\n"
            f"🎰 **Game:** {primary_curr.get('game', 'Unknown')}\n"
            f"💰 **Bet:** ${primary_curr.get('wagered', 0):,.2f} | **Payout:** ${primary_curr.get('payout', 0):,.2f}"
        )

        # Displaced / dropped users
        displaced_lines = []
        dropped_lines = []
        for rank in changed_ranks:
            prev_e = prev_by_rank.get(rank)
            if not prev_e:
                continue
            prev_id = prev_e.get("identity")
            if prev_id == primary_identity:
                continue
            prev_uname = self._mask_public_username(prev_e.get("username", "Unknown"))
            survivor = next(
                (e for e in current_entries if isinstance(e, dict) and e.get("identity") == prev_id),
                None,
            )
            if survivor:
                displaced_lines.append(
                    f"• @{prev_uname} moved from {self._rank_label(rank)} to {self._rank_label(int(survivor['rank']))}"
                )
            else:
                dropped_lines.append(
                    f"• @{prev_uname} dropped out of {self._rank_label(rank)} place"
                )

        description = (
            f"**Week Period:** <t:{week_start_ts}:F> → <t:{week_end_ts}:F>\n\n"
            f"{header}\n\n"
            f"{entry_block}"
        )
        if displaced_lines:
            description += "\n\n⬇️ **Position Changes:**\n" + "\n".join(displaced_lines)
        if dropped_lines:
            description += "\n\n⬇️ **Replaced:**\n" + "\n".join(dropped_lines)
        description += (
            f"\n\n📍 **Track this week's multiplier leaderboard:** <#{WEEKLY_MULTIPLIER_LEADERBOARD_CHANNEL_ID}>\n"
            f"🎭 **Claim the Weekly Multiplier role:** <#{WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID}>"
        )

        embed = discord.Embed(
            title="🏆 Weekly Multiplier Leaderboard Update",
            description=description,
            color=discord.Color.gold(),
        )
        embed.set_footer(text="AutoTip Engine Live • Leaderboard Change Detected")
        return embed

    def _build_weekly_payout_embed(self, title, winners_data, week_start_ts=None, week_end_ts=None, week_key=None):
        winners_text = "***Winners:***\n\n"
        for winner in winners_data:
            display_username = self._mask_public_username(winner["username"])
            medal = ["🥇", "🥈", "🥉"][winner["rank"] - 1]
            place = ["1st", "2nd", "3rd"][winner["rank"] - 1]
            winners_text += (
                f"{medal} **{place} Place:** @{display_username} - **x{winner['multiplier']:,.2f} multiplier**\n"
                f"   🎰 **Game:** {winner['game_name']}\n"
                f"   💰 **Bet:** ${winner['wagered']:,.2f} | **Payout:** ${winner['payout']:,.2f}\n"
                f"   💸 **Prize:** ${winner['prize']:.0f}\n\n"
            )

        if winners_text == "***Winners:***\n\n":
            winners_text = "No multiplier data found for this week.\n\n"

        if week_start_ts and week_end_ts:
            description = (
                f"**Week Period:** <t:{week_start_ts}:F> → <t:{week_end_ts}:F>\n\n"
                f"{winners_text}"
                f"📍 **Track this week's multiplier leaderboard:** <#{WEEKLY_MULTIPLIER_LEADERBOARD_CHANNEL_ID}>\n"
                f"🎭 **Claim the Weekly Multiplier role:** <#{WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID}>"
            )
        else:
            description = (
                f"**Week of {week_key}**\n\n"
                f"{winners_text}"
                f"📍 **Track this week's multiplier leaderboard:** <#{WEEKLY_MULTIPLIER_LEADERBOARD_CHANNEL_ID}>\n"
                f"🎭 **Claim the Weekly Multiplier role:** <#{WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID}>"
            )

        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.green()
        )
        embed.set_footer(text="AutoTip Engine Live • Payouts Sent Successfully")
        return embed

    @tasks.loop(minutes=10)
    async def update_multi_leaderboard(self):
        logger.info("[MultiLeaderboard] Starting weekly multiplier leaderboard update cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        channel = self.bot.get_channel(MULTI_LEADERBOARD_CHANNEL_ID)
        if not channel:
            logger.error("MultiLeaderboard channel not found.")
            return
        
        # Get the UPCOMING week range (the week we're currently competing in)
        now = datetime.now(dt.UTC)
        days_since_friday = (now.weekday() - 4) % 7
        start_of_week = now - dt.timedelta(days=days_since_friday)
        start_date = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        
        end_of_week = start_of_week + dt.timedelta(days=6)
        end_date = end_of_week.replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()
        week_start_ts = int(datetime.fromisoformat(start_date.replace('Z', '+00:00')).timestamp())
        week_end_ts = int(datetime.fromisoformat(end_date.replace('Z', '+00:00')).timestamp())
        week_key = datetime.fromisoformat(start_date.replace('Z', '+00:00')).date().isoformat()
        
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
        current_snapshot = self._build_leaderboard_snapshot(multi_data)
        alert_state = self._load_leaderboard_alert_state(week_key)
        leaderboard_changes = self._detect_leaderboard_changes(alert_state.get("entries", []), current_snapshot)
        embed = discord.Embed(
            title="🏆 **Weekly Top Multipliers Leaderboard** 🏆",
            description=(
                f"🗓️ **Competition Period:**\n"
                f"From: <t:{week_start_ts}:F>\n"
                f"To: <t:{week_end_ts}:F>\n\n"
                f"⏰ **Last Updated:** <t:{int(datetime.now(dt.UTC).timestamp())}:R>\n\n"
                "This leaderboard ranks users by their highest single multiplier hit this week.\n"
                "💵 **All amounts displayed are in USD.**\n\n"
            ),
            color=discord.Color.purple()
        )
        position_markers = ["🥇", "🥈", "🥉"]
        for i in range(3):  # Only top 3 for weekly competition
            if i < len(multi_data):
                entry = multi_data[i]
                username = entry.get("username", "Unknown")
                # Censor username using bullet characters for consistency.
                if len(username) > 3:
                    username = username[:-3] + "•••"
                else:
                    username = "•••"
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
            position_marker = position_markers[i] if i < len(position_markers) else f"#{i + 1}"
            embed.add_field(
                name=f"{position_marker} — ***__{username}__***",
                value=(
                    f"💥 **Highest Multiplier:** `x{multiplier:,.2f}`\n"
                    f"🎰 **Game:** `{game}`\n"
                    f"💰 **Payout:** `${payout:,.2f}` (`${wagered:,.2f}` Base Bet)\n"
                    f"🎁 **Prize:** `${prize:.2f}`"
                ),
                inline=False
            )

        channel_lines = []
        if WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID:
            channel_lines.append(
                f"📍 **Track payout logs:** <#{WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID}>"
            )
        if WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID:
            channel_lines.append(
                f"🎭 **Claim the Weekly Multiplier role:** <#{WEEKLY_MULTIPLIER_ROLE_CLAIM_CHANNEL_ID}>"
            )
        if channel_lines:
            embed.add_field(name="\u200b", value="\n".join(channel_lines), inline=False)

        embed.set_footer(text="AutoTip Engine • Auto-pays at midnight UTC every Friday.")
        
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

        if leaderboard_changes and WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID:
            logs_channel = self.bot.get_channel(WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID)
            if logs_channel:
                try:
                    change_embed = self._build_weekly_leaderboard_change_embed(
                        alert_state.get("entries", []),
                        current_snapshot,
                        week_start_ts,
                        week_end_ts,
                    )
                    await logs_channel.send(embed=change_embed)
                    logger.info(
                        f"[MultiLeaderboard] Posted {len(leaderboard_changes)} leaderboard change(s) to logs channel"
                    )
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in weekly multiplier logs channel.")
            else:
                logger.warning(
                    f"[MultiLeaderboard] Logs channel {WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID} not found for leaderboard changes"
                )

        self._save_leaderboard_alert_state({
            "week_key": week_key,
            "entries": current_snapshot,
        })

    @tasks.loop(minutes=5)  # Check every 5 minutes for weekly payout
    async def weekly_payout_check(self):
        """Check if it's time for weekly multiplier payouts (Friday 00:15 UTC)"""
        try:
            now = datetime.now(dt.UTC)
            
            # Check if we're within the payout window: Friday 00:15-00:59 UTC
            is_friday = now.weekday() == 4  # Friday = 4
            is_payout_time = now.hour == 0 and 15 <= now.minute <= 59
            
            # Debug logging - log every check during the critical window
            if is_friday and now.hour == 0:
                logger.info(f"[MultiLeaderboard] 🔍 PAYOUT WINDOW CHECK - Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC - Minute: {now.minute} - Is payout time: {is_payout_time}")
            
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
            
            # Process the payouts
            logger.info(f"[MultiLeaderboard] 🚀 EXECUTING WEEKLY PAYOUTS NOW!")
            payout_complete = await self.process_weekly_payouts()

            if payout_complete:
                # Mark week as processed locally only when week is fully complete.
                self.last_payout_week = current_week_key
                logger.info(f"[MultiLeaderboard] ✅ PAYOUT PROCESS COMPLETED for week {current_week_key}")
            else:
                logger.warning(
                    f"[MultiLeaderboard] ⚠️ PAYOUT PARTIAL for week {current_week_key}. "
                    "Will allow retry in next payout window/manual trigger."
                )
            
        except Exception as e:
            logger.error(f"[MultiLeaderboard] ERROR in weekly_payout_check: {e}", exc_info=True)

    async def process_weekly_payouts(self):
        """Process payouts for the previous week's top 3 multiplier winners.

        Returns True when the week's expected winners are fully paid/recorded.
        Returns False when payout is partial or a fatal error occurs.
        """
        try:
            # Get PREVIOUS week range (the week that just completed)
            now = datetime.now(dt.UTC)
            # Go back 7 days to get the previous week
            prev_week = now - dt.timedelta(days=7)
            # Get the week range for that previous week
            days_since_friday = (prev_week.weekday() - 4) % 7
            start_of_week = prev_week - dt.timedelta(days=days_since_friday)
            start_date = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            
            end_of_week = start_of_week + dt.timedelta(days=6)
            end_date = end_of_week.replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()
            
            logger.info(f"[MultiLeaderboard] 📅 Payout week range: {start_date} to {end_date}")
            week_key = f"{start_date[:10]}"  # Use start date as week identifier (YYYY-MM-DD)
            
            # First, ensure the table exists and has correct schema
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    # Create table if it doesn't exist
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
                            wagered DECIMAL(10,2) DEFAULT 0,
                            payout DECIMAL(10,2) DEFAULT 0,
                            paid_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            UNIQUE(week_start, rank)
                        );
                    """)
                    
                    # Try to alter user_id column to VARCHAR if it exists as BIGINT
                    try:
                        cur.execute("ALTER TABLE weekly_multiplier_payouts ALTER COLUMN user_id TYPE VARCHAR(255);")
                        logger.info("[MultiLeaderboard] ✅ Altered user_id column to VARCHAR(255)")
                    except Exception as alter_error:
                        # Column might already be VARCHAR, that's fine
                        logger.debug(f"[MultiLeaderboard] user_id column alter (may already be correct): {alter_error}")

                    # Ensure wager/payout columns exist for accurate summary embeds.
                    try:
                        cur.execute("ALTER TABLE weekly_multiplier_payouts ADD COLUMN IF NOT EXISTS wagered DECIMAL(10,2) DEFAULT 0;")
                        cur.execute("ALTER TABLE weekly_multiplier_payouts ADD COLUMN IF NOT EXISTS payout DECIMAL(10,2) DEFAULT 0;")
                    except Exception as column_error:
                        logger.debug(f"[MultiLeaderboard] wager/payout column ensure (may already be correct): {column_error}")
                    
                    conn.commit()
                    logger.info("[MultiLeaderboard] ✅ Ensured weekly_multiplier_payouts table exists with correct schema")
            except Exception as e:
                logger.warning(f"[MultiLeaderboard] ⚠️ Could not create/alter table: {e}")
                conn.rollback()
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

            expected_winner_count = min(3, len(multi_data))
            if expected_winner_count == 0:
                logger.warning("[MultiLeaderboard] ⚠️ No valid multiplier winners found for payout week")
                return True

            # Load already-paid ranks for this week so we can retry only missing ranks.
            conn = get_db_connection()
            paid_rows_by_rank = {}
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT rank, username, multiplier, game_name, prize_amount, wagered, payout
                        FROM weekly_multiplier_payouts
                        WHERE week_start = %s AND rank > 0
                        ORDER BY rank ASC;
                        """,
                        (week_key,)
                    )
                    for rank, username, multiplier, game_name, prize_amount, wagered, payout in cur.fetchall():
                        paid_rows_by_rank[int(rank)] = {
                            "rank": int(rank),
                            "username": username,
                            "multiplier": float(multiplier),
                            "game_name": game_name,
                            "wagered": float(wagered or 0),
                            "payout": float(payout or 0),
                            "prize": float(prize_amount),
                        }
            except Exception as e:
                logger.error(f"[MultiLeaderboard] Error checking database: {e}")
            finally:
                release_db_connection(conn)

            if len(paid_rows_by_rank) >= expected_winner_count:
                logger.info(
                    f"[MultiLeaderboard] Week {week_key} already complete in database "
                    f"({len(paid_rows_by_rank)}/{expected_winner_count}), skipping payouts"
                )
                return True

            # Process only unpaid ranks among the top expected winners.
            winners_processed = 0
            for i in range(expected_winner_count):
                rank = i + 1
                if rank in paid_rows_by_rank:
                    logger.info(f"[MultiLeaderboard] Rank #{rank} already paid for week {week_key}, skipping")
                    continue

                entry = multi_data[i]
                user_id = entry.get("uid")
                username = entry.get("username", "Unknown")
                multiplier = entry["highestMultiplier"].get("multiplier", 0)
                game_name = entry["highestMultiplier"].get("gameTitle", "Unknown")
                wagered = entry["highestMultiplier"].get("wagered", 0)
                payout = entry["highestMultiplier"].get("payout", 0)
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
                                (week_start, rank, user_id, username, prize_amount, multiplier, game_name, wagered, payout)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (week_start, rank) DO NOTHING
                            """, (week_key, rank, user_id, username, prize_amount, multiplier, game_name, wagered, payout))
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
                    logger.info(f"[MultiLeaderboard] 🏆 Successfully paid ${prize_amount} to {username} for Rank #{rank}")
                    
                else:
                    logger.error(f"[MultiLeaderboard] ❌ FAILED to tip {username}: Response={tip_response}")
                
                # INCREASED: 30 second delay between tips to ensure processing
                logger.info(f"[MultiLeaderboard] ⏳ Waiting 30 seconds before next tip...")
                await asyncio.sleep(30)
            
            # Reload paid rows to determine completion and build summary from actual recorded winners.
            paid_rows_by_rank = {}
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT rank, username, multiplier, game_name, prize_amount, wagered, payout
                        FROM weekly_multiplier_payouts
                        WHERE week_start = %s AND rank > 0
                        ORDER BY rank ASC;
                        """,
                        (week_key,)
                    )
                    for rank, username, multiplier, game_name, prize_amount, wagered, payout in cur.fetchall():
                        paid_rows_by_rank[int(rank)] = {
                            "rank": int(rank),
                            "username": username,
                            "multiplier": float(multiplier),
                            "game_name": game_name,
                            "wagered": float(wagered or 0),
                            "payout": float(payout or 0),
                            "prize": float(prize_amount),
                        }
            except Exception as e:
                logger.error(f"[MultiLeaderboard] Error reloading paid rows: {e}")
            finally:
                release_db_connection(conn)

            payout_complete = len(paid_rows_by_rank) >= expected_winner_count

            # Send summary to logs channel if we processed payouts in this run.
            if winners_processed > 0:
                logs_channel_id = WEEKLY_MULTIPLIER_LOGS_CHANNEL_ID
                if logs_channel_id:
                    logs_channel = self.bot.get_channel(logs_channel_id)
                    if logs_channel:
                        summary_winners = []
                        for rank in sorted(paid_rows_by_rank.keys()):
                            summary_winners.append(paid_rows_by_rank[rank])
                        
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
                        
                        embed = self._build_weekly_payout_embed(
                            "🏆 Weekly Multiplier Leaderboard Payouts",
                            summary_winners,
                            week_start_ts=week_start_ts,
                            week_end_ts=week_end_ts,
                            week_key=week_key,
                        )
                        
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
            if not payout_complete:
                logger.warning(
                    f"[MultiLeaderboard] ⚠️ Week {week_key} payout is partial: "
                    f"{len(paid_rows_by_rank)}/{expected_winner_count} ranks recorded"
                )
            return payout_complete
            
        except Exception as e:
            logger.error(f"[MultiLeaderboard] ❌❌❌ CRITICAL ERROR in weekly payout process: {e}", exc_info=True)
            import traceback
            logger.error(f"[MultiLeaderboard] Traceback: {traceback.format_exc()}")
            return False

    @weekly_payout_check.before_loop
    async def before_weekly_payout_check(self):
        await self.bot.wait_until_ready()

    @update_multi_leaderboard.before_loop
    async def before_multi_leaderboard_loop(self):
        await self.bot.wait_until_ready()

    @weekly_payout_check.error
    async def weekly_payout_check_error(self, error):
        logger.error(f"[MultiLeaderboard] TASK LOOP ERROR in weekly_payout_check: {error}", exc_info=True)

    def cog_unload(self):
        self.update_multi_leaderboard.cancel()
        self.weekly_payout_check.cancel()

async def setup(bot):
    await bot.add_cog(MultiLeaderboard(bot))
