import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import send_tip, fetch_weighted_wager
from db import (
    ensure_roovsflip_tables,
    get_roovsflip_queue,
    set_roovsflip_queue_slot,
    swap_roovsflip_queue_positions,
    clear_roovsflip_queue_slot,
    get_roovsflip_draft_queue,
    set_roovsflip_draft_queue_slot,
    clear_roovsflip_draft_queue_slot,
    copy_roovsflip_draft_to_active,
    is_roovsflip_paid,
    is_roovsflip_winner_paid,
    record_roovsflip_payout,
    get_roovsflip_event_start,
    set_roovsflip_event_start,
    get_leaderboard_message_id,
    save_leaderboard_message_id,
    save_tip_log,
    get_setting_value,
    save_setting_value,
)
import json
import os
import logging
from datetime import datetime, timezone
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)

GUILD_ID = int(os.getenv("GUILD_ID"))
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))
ROO_VS_FLIP_CHANNEL_ID = int(os.getenv("ROO_VS_FLIP_CHANNEL_ID", "0"))
ROO_VS_FLIP_HISTORY_CHANNEL_ID = int(os.getenv("ROO_VS_FLIP_HISTORY_CHANNEL_ID", "0"))
ROO_VS_FLIP_PING_ROLE_ID = 1501438806895759482
ROO_VS_FLIP_ROLE_CLAIM_CHANNEL_ID = 1440843895360590028
ROO_VS_FLIP_ALERT_STATE_KEY = "roovsflip_alert_state"

PRIZE_POOL = 250.00
MAX_QUEUE_SIZE = 5
EMBED_MAX_PARTICIPANTS = 8  # Keep description under Discord's 4096-char limit
PAYOUT_DELAY_SECONDS = 30


class RooVsFlip(commands.Cog):
    rvf = app_commands.Group(name="rvf", description="Roo Vs Flip management commands")

    def __init__(self, bot):
        self.bot = bot
        self.last_payout_month = None  # e.g. "2026-03" – prevents double-processing

    async def cog_load(self):
        ensure_roovsflip_tables()
        # Bootstrap: If draft queue is empty but active queue has games,
        # copy active to draft to continue the cycle.
        active_queue = get_roovsflip_queue()
        draft_queue = get_roovsflip_draft_queue()
        if active_queue and not draft_queue:
            logger.info("[RooVsFlip] Bootstrapping draft queue from active queue.")
            for game in active_queue:
                set_roovsflip_draft_queue_slot(
                    game["position"],
                    game["game_name"],
                    game["game_identifier"],
                    game.get("emoji", "🎮"),
                    game["req_multi"],
                )
        await self.recover_missed_payout_on_startup()
        self.update_embed.start()
        self.monthly_payout_check.start()
        logger.info("[RooVsFlip] Cog loaded, tables ensured, tasks started.")

    def cog_unload(self):
        self.update_embed.cancel()
        self.monthly_payout_check.cancel()

    async def recover_missed_payout_on_startup(self):
        """Self-heal: if the current period has ended but wasn't paid, run payout once."""
        now = datetime.now(dt.UTC)
        event_start = get_roovsflip_event_start()
        if not event_start:
            return

        period_end = self.compute_period_end(event_start)
        if now < period_end:
            logger.info("[RooVsFlip] Startup check: period still running, no recovery needed.")
            return

        # Derive the payout month from the period end (the month just before it)
        if period_end.month == 1:
            payout_year, payout_month = period_end.year - 1, 12
        else:
            payout_year, payout_month = period_end.year, period_end.month - 1

        month_key = f"{payout_year}-{payout_month:02d}"
        if is_roovsflip_paid(payout_year, payout_month):
            logger.info(f"[RooVsFlip] Startup check: {month_key} already finalized.")
            self.last_payout_month = month_key
            return

        logger.warning(
            f"[RooVsFlip] Startup self-heal: month {month_key} not finalized, running payout."
        )
        await self.run_monthly_payout(payout_year, payout_month, automated=True)
        if is_roovsflip_paid(payout_year, payout_month):
            self.last_payout_month = month_key

    # ─── Period helpers ──────────────────────────────────────────────────────

    @staticmethod
    def compute_period_end(event_start_str):
        """
        Return the UTC datetime when the current challenge period ends.

        Rule:
          - If the event started on the 1st of a month (normal monthly reset),
            the period ends at the start of the *next* month.
          - If the event started mid-month (first-ever launch), extend by an
            extra month so the first period runs roughly 5-6 weeks instead of
            just the remaining days of the launch month.
        """
        try:
            start = datetime.fromisoformat(event_start_str.replace("Z", "+00:00"))
        except Exception:
            start = datetime.now(dt.UTC)

        # Mid-month launch → skip forward two months; first-of-month → one month
        months_ahead = 2 if start.day > 1 else 1
        target_month = start.month + months_ahead
        target_year = start.year + (target_month - 1) // 12
        target_month = ((target_month - 1) % 12) + 1
        return datetime(target_year, target_month, 1, tzinfo=dt.UTC)

    # ─── Prize helpers ────────────────────────────────────────────────────────

    def compute_prize_split(self, winner_count):
        """
        Cent-safe prize split. Returns a list of floats, one per winner.
        The first N winners receive one extra cent when the total doesn't divide evenly.
        """
        if winner_count == 0:
            return []
        total_cents = round(PRIZE_POOL * 100)
        base_cents = total_cents // winner_count
        remainder = total_cents - base_cents * winner_count
        prizes = [base_cents / 100.0] * winner_count
        for i in range(remainder):
            prizes[i] = (base_cents + 1) / 100.0
        return prizes

    # ─── Data helpers ─────────────────────────────────────────────────────────

    async def fetch_all_game_data(self, queue, event_start):
        """
        For each queued game make a single affiliate API call filtered to that
        game_identifier and the event start timestamp.
        Returns {game_identifier: [entries]}.
        """
        game_data = {}
        for game in queue:
            gid = game["game_identifier"]
            try:
                data = await asyncio.to_thread(
                    fetch_weighted_wager, event_start, None, gid
                )
                game_data[gid] = data if isinstance(data, list) else []
                logger.info(
                    f"[RooVsFlip] {game['game_name']} ({gid}): "
                    f"{len(game_data[gid])} user entries"
                )
            except Exception as e:
                logger.error(f"[RooVsFlip] API error for {gid}: {e}")
                game_data[gid] = []
            await asyncio.sleep(2)  # Avoid hammering the API
        return game_data

    def build_participant_list(self, queue, game_data):
        """
        Build a sorted participant list from raw API entries.

        Sorting priority:
          1. Completed games count (desc)
          2. Average multiplier across completed games (desc)
          3. Highest single completed multiplier (desc)
        """
        player_map = {}  # {uid: {username, games: {gid: {multi, met}}}}

        for game in queue:
            gid = game["game_identifier"]
            req = float(game["req_multi"])
            for entry in game_data.get(gid, []):
                uid = entry.get("uid")
                username = entry.get("username")
                hm = entry.get("highestMultiplier")
                if not (uid and username and hm):
                    continue
                if hm.get("gameId") != gid:
                    continue
                multi = float(hm.get("multiplier", 0))
                if uid not in player_map:
                    player_map[uid] = {"username": username, "games": {}}
                player_map[uid]["games"][gid] = {
                    "multi": multi,
                    "met": multi >= req,
                }

        total_games = len(queue)
        participants = []
        for uid, data in player_map.items():
            games = data["games"]
            completions = sum(
                1 for g in queue
                if games.get(g["game_identifier"], {}).get("met", False)
            )
            completed_multis = [
                games[g["game_identifier"]]["multi"]
                for g in queue
                if g["game_identifier"] in games and games[g["game_identifier"]]["met"]
            ]
            avg_multi = (
                sum(completed_multis) / len(completed_multis)
                if completed_multis else 0.0
            )
            max_multi = max(completed_multis) if completed_multis else 0.0
            participants.append(
                {
                    "uid": uid,
                    "username": data["username"],
                    "games": games,
                    "completions": completions,
                    "avg_multi": avg_multi,
                    "max_multi": max_multi,
                    "is_winner": completions == total_games,
                }
            )

        participants.sort(
            key=lambda x: (-x["completions"], -x["avg_multi"], -x["max_multi"])
        )
        return participants

    @staticmethod
    def format_winner_game_multis(winner, queue):
        """Format one winner's per-game multipliers for payout/preview embeds."""
        parts = []
        for game in queue:
            gid = game["game_identifier"]
            emoji_str = game.get("emoji", "🎮")
            info = winner.get("games", {}).get(gid)
            if info and isinstance(info.get("multi"), (int, float)):
                parts.append(f"{emoji_str} x{float(info['multi']):,.2f}")
            else:
                parts.append(f"{emoji_str} n/a")
        return " | ".join(parts)

    @staticmethod
    def mask_username(username):
        if not isinstance(username, str) or not username:
            return "•••"
        return username[:-3] + "•••" if len(username) > 3 else "•••"

    @staticmethod
    def format_req_multi(value):
        return f"{float(value):,.2f}".rstrip("0").rstrip(".")

    def build_prize_summary(self, winner_count):
        prize_splits = self.compute_prize_split(winner_count)
        prize_each = prize_splits[0] if prize_splits else 0.0
        winner_label = "winner" if winner_count == 1 else "winners"
        return f"💰 Prize Pool: ${PRIZE_POOL:,.2f} • {winner_count} {winner_label} • ${prize_each:,.2f} each"

    def _load_alert_state(self, event_start):
        raw_state = get_setting_value(ROO_VS_FLIP_ALERT_STATE_KEY, default=None)
        if not raw_state:
            return {"event_start": event_start, "users": {}}

        try:
            parsed = json.loads(raw_state)
        except (TypeError, json.JSONDecodeError):
            logger.warning("[RooVsFlip] Invalid alert state JSON; resetting state.")
            return {"event_start": event_start, "users": {}}

        if not isinstance(parsed, dict) or parsed.get("event_start") != event_start:
            return {"event_start": event_start, "users": {}}

        users = parsed.get("users")
        if not isinstance(users, dict):
            users = {}

        return {"event_start": event_start, "users": users}

    def _save_alert_state(self, state):
        save_setting_value(
            ROO_VS_FLIP_ALERT_STATE_KEY,
            json.dumps(state, separators=(",", ":")),
        )

    async def _get_history_channel(self):
        if not ROO_VS_FLIP_HISTORY_CHANNEL_ID:
            return None

        channel = self.bot.get_channel(ROO_VS_FLIP_HISTORY_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(ROO_VS_FLIP_HISTORY_CHANNEL_ID)
            except Exception as e:
                logger.error(f"[RooVsFlip] Failed to fetch history channel: {e}")
                return None

        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            logger.error("[RooVsFlip] History channel is not a text channel/thread.")
            return None

        return channel

    def build_progress_alert_embed(self, participant, game, game_info, winner_count, completed_ts, total_games):
        masked_username = self.mask_username(participant.get("username"))
        req_display = self.format_req_multi(game["req_multi"])
        description = (
            f"👑 {masked_username} • {participant['completions']}/{total_games} complete\n"
            f"✅ {game['game_name']} {game.get('emoji', '🎮')}: x{float(game_info['multi']):,.2f} / x{req_display}\n"
            f"🕒 <t:{completed_ts}:F>\n"
            f"{self.build_prize_summary(winner_count)}\n\n"
            f"📍 Track this month's Roo vs Flip challenge: <#{ROO_VS_FLIP_CHANNEL_ID}>\n"
            f"🎭 Claim the Roo Vs Flip Degens role: <#{ROO_VS_FLIP_ROLE_CLAIM_CHANNEL_ID}>"
        )
        return discord.Embed(
            title="🎯 Roo Vs Flip Progress",
            description=description,
            color=discord.Color.gold(),
        )

    def build_completion_alert_embed(self, participant, queue, winner_count, completed_ts):
        masked_username = self.mask_username(participant.get("username"))
        completed_lines = []
        for game in queue:
            gid = game["game_identifier"]
            game_info = participant.get("games", {}).get(gid, {})
            req_display = self.format_req_multi(game["req_multi"])
            multi_display = float(game_info.get("multi", 0.0))
            completed_lines.append(
                f"{game['game_name']} {game.get('emoji', '🎮')}: x{multi_display:,.2f} / x{req_display}"
            )

        description = (
            f"👑 {masked_username} • {participant['completions']}/{len(queue)} complete\n"
            f"🕒 <t:{completed_ts}:F>\n"
            f"{self.build_prize_summary(winner_count)}\n\n"
            "✅ Completed Games\n"
            + "\n".join(completed_lines)
            + "\n\n"
            f"📍 Track this month's Roo vs Flip challenge: <#{ROO_VS_FLIP_CHANNEL_ID}>\n"
            f"🎭 Claim the Roo Vs Flip Degens role: <#{ROO_VS_FLIP_ROLE_CLAIM_CHANNEL_ID}>"
        )
        return discord.Embed(
            title="🏁 Roo Vs Flip Completion Alert",
            description=description,
            color=discord.Color.gold(),
        )

    async def post_progress_alerts(self, queue, participants, event_start):
        history_channel = await self._get_history_channel()
        if history_channel is None:
            return

        state = self._load_alert_state(event_start)
        state_changed = False
        winner_count = len([participant for participant in participants if participant["is_winner"]])
        ping = f"<@&{ROO_VS_FLIP_PING_ROLE_ID}>" if ROO_VS_FLIP_PING_ROLE_ID else None
        completed_ts = int(datetime.now(dt.UTC).timestamp())

        for participant in participants:
            if participant.get("completions", 0) <= 0:
                continue

            user_key = str(participant["uid"])
            user_state = state["users"].setdefault(
                user_key,
                {"announced_games": [], "completion_posted": False},
            )
            announced_games = set(user_state.get("announced_games", []))

            for game in queue:
                gid = game["game_identifier"]
                game_info = participant.get("games", {}).get(gid)
                if not game_info or not game_info.get("met") or gid in announced_games:
                    continue

                embed = self.build_progress_alert_embed(
                    participant,
                    game,
                    game_info,
                    winner_count,
                    completed_ts,
                    len(queue),
                )
                await history_channel.send(content=ping, embed=embed)
                announced_games.add(gid)
                state_changed = True

            user_state["announced_games"] = sorted(announced_games)

            if participant.get("is_winner") and not user_state.get("completion_posted"):
                completion_embed = self.build_completion_alert_embed(
                    participant,
                    queue,
                    winner_count,
                    completed_ts,
                )
                await history_channel.send(content=ping, embed=completion_embed)
                user_state["completion_posted"] = True
                state_changed = True

        if state_changed:
            self._save_alert_state(state)

    # ─── Embed builder ────────────────────────────────────────────────────────

    def build_embed(self, queue, participants, event_start_str):
        now = datetime.now(dt.UTC)
        now_ts = int(now.timestamp())

        try:
            start_ts = int(
                datetime.fromisoformat(
                    event_start_str.replace("Z", "+00:00")
                ).timestamp()
            )
        except Exception:
            start_ts = now_ts

        # End of this challenge period (mid-month start → 2 months; 1st-of-month → 1 month)
        end_dt = self.compute_period_end(event_start_str)
        end_ts = int(end_dt.timestamp())

        total_games = len(queue)
        winners = [p for p in participants if p["is_winner"]]
        winner_count = len(winners)
        prize_splits = self.compute_prize_split(winner_count)
        prize_str = (
            f"${prize_splits[0]:,.2f} each"
            if winner_count > 0
            else "N/A"
        )

        desc = (
            f"🗓️ **Challenge Period:**\n"
            f"From: <t:{start_ts}:F>\n"
            f"To: <t:{end_ts}:F>\n\n"
            f"⏰ **Last Updated:** <t:{now_ts}:R>\n\n"
            "📜 **Rules & Disclosure:**\n"
            "• Beat Flip's multipliers with a minimum bet size of $0.20 USD\n"
            f"• ${PRIZE_POOL:,.2f} prize pool is split between all qualifying players\n"
            "• All challenges must be completed to win\n\n"
            f"💰 **Total Prize Pool:** ${PRIZE_POOL:,.2f} USD\n"
            f"👑 **Current Winners:** {winner_count}\n"
            f"🎁 **Current Prize:** {prize_str}\n\n"
            "💵 **All amounts displayed are in USD.**\n\n"
        )

        # Queued games
        desc += f"🎰 **Required Games ({total_games}):**\n"
        for g in queue:
            game_url = f"https://roobet.com/casino/game/{g['game_identifier']}"
            emoji_str = g.get("emoji", "🎮")
            desc += (
                f"**{g['position']}.** {emoji_str} [{g['game_name']}]({game_url})"
                f" — Req x{g['req_multi']:,.2f}\n"
            )
        desc += "\n"

        # Participants (only show those with at least 1 completion)
        qualified_participants = [p for p in participants if p["completions"] >= 1]
        if not qualified_participants:
            desc += "📊 **Participants:**\n*No activity yet — start playing!*\n"
        else:
            shown = min(len(qualified_participants), EMBED_MAX_PARTICIPANTS)
            desc += f"📊 **Participants** (top {shown} shown):\n"
            for i, p in enumerate(qualified_participants[:EMBED_MAX_PARTICIPANTS]):
                uname = p["username"]
                display = (uname[:-3] + "•••") if len(uname) > 3 else "•••"
                completion_badge = " 🏆" if p["completions"] == total_games else ""
                desc += (
                    f"\n**#{i + 1} — {display}**"
                    f" — `{p['completions']}/{total_games} Complete`{completion_badge}\n"
                )
                for g in queue:
                    gid = g["game_identifier"]
                    info = p["games"].get(gid)
                    emoji_str = g.get("emoji", "🎮")
                    req_display = f"{float(g['req_multi']):,.2f}".rstrip("0").rstrip(".")
                    if info is None:
                        desc += f"{emoji_str} -- / {req_display}x ⏳\n"
                    elif info["met"]:
                        desc += (
                            f"{emoji_str} {info['multi']:,.2f}x / {req_display}x ✅\n"
                        )
                    else:
                        desc += (
                            f"{emoji_str} {info['multi']:,.2f}x / {req_display}x ❌\n"
                        )
                if i < shown - 1:
                    desc += "──────────────\n"

            if len(qualified_participants) > EMBED_MAX_PARTICIPANTS:
                extra = len(qualified_participants) - EMBED_MAX_PARTICIPANTS
                desc += f"\n*...and {extra} more participant(s)*\n"

        desc += (
            "\n**Legend:** ✅ requirement met"
            " | ❌ played but below requirement"
            " | ⏳ no data yet"
        )

        # Safety: Discord embed description limit is 4096 chars
        if len(desc) > 4000:
            desc = desc[:3990] + "\n*...(truncated)*"

        embed = discord.Embed(
            title="🏆 **Roo Vs Flip Live Challenge** 🏆",
            description=desc,
            color=discord.Color.gold(),
        )
        embed.set_footer(
            text="AutoTip Engine • Auto-pays automatically once the period ends."
        )
        return embed

    # ─── Live embed update ────────────────────────────────────────────────────

    async def post_or_edit_embed(self, embed):
        """Post a new embed or edit the existing one, persisting the message ID."""
        channel = self.bot.get_channel(ROO_VS_FLIP_CHANNEL_ID)
        if not channel:
            logger.error("[RooVsFlip] ROO_VS_FLIP_CHANNEL_ID channel not found.")
            return
        message_id = get_leaderboard_message_id("roovsflip_embed_message_id")
        if message_id:
            try:
                msg = await channel.fetch_message(message_id)
                await msg.edit(embed=embed)
                logger.info("[RooVsFlip] Live embed updated.")
                return
            except discord.errors.NotFound:
                logger.warning("[RooVsFlip] Embed message not found, sending new one.")
            except discord.errors.Forbidden:
                logger.error("[RooVsFlip] Missing permissions to edit embed.")
                return
        try:
            msg = await channel.send(embed=embed)
            save_leaderboard_message_id(msg.id, "roovsflip_embed_message_id")
            logger.info("[RooVsFlip] New live embed posted.")
        except discord.errors.Forbidden:
            logger.error("[RooVsFlip] Missing permissions to post embed.")

    # ─── Tasks ────────────────────────────────────────────────────────────────

    @tasks.loop(minutes=10)
    async def update_embed(self):
        """Refresh the live leaderboard embed every 10 minutes."""
        logger.info("[RooVsFlip] Embed update cycle starting, waiting 3 min offset...")
        await asyncio.sleep(180)  # Offset: DataManager=0:00, Leaderboard=0:02, Challenges=0:02, RooVsFlip=0:03

        queue = get_roovsflip_queue()
        if not queue:
            logger.info("[RooVsFlip] Queue empty, skipping embed update.")
            return

        event_start = get_roovsflip_event_start()
        game_data = await self.fetch_all_game_data(queue, event_start)
        participants = self.build_participant_list(queue, game_data)
        await self.post_progress_alerts(queue, participants, event_start)
        embed = self.build_embed(queue, participants, event_start)
        await self.post_or_edit_embed(embed)

    @update_embed.before_loop
    async def before_update_embed(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=5)
    async def monthly_payout_check(self):
        """
        Check for an overdue period every 5 minutes.
        Pays out the finished event once and resets for the new month.
        """
        try:
            now = datetime.now(dt.UTC)

            # Only proceed if the current period has actually ended.
            event_start = get_roovsflip_event_start()
            if not event_start:
                return

            period_end = self.compute_period_end(event_start)
            if now < period_end:
                return

            # Determine which month just ended (the month before period_end).
            if period_end.month == 1:
                payout_year, payout_month = period_end.year - 1, 12
            else:
                payout_year, payout_month = period_end.year, period_end.month - 1

            month_key = f"{payout_year}-{payout_month:02d}"
            logger.info(f"[RooVsFlip] Payout window — checking month {month_key}")

            if self.last_payout_month == month_key:
                return

            if is_roovsflip_paid(payout_year, payout_month):
                logger.info(f"[RooVsFlip] {month_key} already in DB, skipping.")
                self.last_payout_month = month_key
                return

            logger.info(f"[RooVsFlip] Running automated payout for {month_key}...")
            await self.run_monthly_payout(payout_year, payout_month, automated=True)
            self.last_payout_month = month_key

        except Exception as e:
            logger.error(f"[RooVsFlip] Error in monthly_payout_check: {e}", exc_info=True)

    @monthly_payout_check.before_loop
    async def before_monthly_payout_check(self):
        await self.bot.wait_until_ready()

    # ─── Payout logic ─────────────────────────────────────────────────────────

    async def run_monthly_payout(self, payout_year, payout_month, automated=False):
        """
        1. Fetch final API data using stored event_start.
        2. Identify full winners.
        3. Split prize pool and send tips.
        4. Post results embed to history channel.
        5. Record all payouts in DB.
        6. Reset event_start to 1st of new month.
        7. Clear embed message ID so a fresh board is posted next cycle.
        """
        history_channel = (
            self.bot.get_channel(ROO_VS_FLIP_HISTORY_CHANNEL_ID)
            if ROO_VS_FLIP_HISTORY_CHANNEL_ID
            else None
        )
        queue = get_roovsflip_queue()
        event_start = get_roovsflip_event_start()

        # ── Determine next event start from the paid-out month ───────────────
        if payout_month == 12:
            next_year, next_month = payout_year + 1, 1
        else:
            next_year, next_month = payout_year, payout_month + 1
        new_start = datetime(next_year, next_month, 1, tzinfo=dt.UTC).isoformat()
        now = datetime.now(dt.UTC)

        if not queue:
            logger.warning(
                f"[RooVsFlip] No games in queue at payout for "
                f"{payout_year}-{payout_month:02d} — skipping payout."
            )
            record_roovsflip_payout(
                payout_year, payout_month, "NO_GAMES", "NO_GAMES", 0.0
            )
            record_roovsflip_payout(
                payout_year, payout_month, "PAID_COMPLETE", "PAID_COMPLETE", 0.0
            )
            set_roovsflip_event_start(new_start)
            return

        # ── Fetch final state ─────────────────────────────────────────────────
        game_data = await self.fetch_all_game_data(queue, event_start)
        participants = self.build_participant_list(queue, game_data)
        winners = [p for p in participants if p["is_winner"]]
        winner_count = len(winners)
        prize_splits = self.compute_prize_split(winner_count)

        logger.info(
            f"[RooVsFlip] {payout_year}-{payout_month:02d}: "
            f"{winner_count} winner(s), prize ${prize_splits[0] if prize_splits else 0:.2f} each"
        )

        # ── Build results embed ───────────────────────────────────────────────
        try:
            start_ts = int(
                datetime.fromisoformat(
                    event_start.replace("Z", "+00:00")
                ).timestamp()
            )
        except Exception:
            start_ts = int(now.timestamp())

        result_embed = discord.Embed(
            title="🏆 Roo Vs Flip Payouts",
            color=discord.Color.gold(),
        )
        desc = f"**Challenge Period:** <t:{start_ts}:F> → <t:{int(now.timestamp())}:F>\n\n"

        desc += "***Challenge Games:***\n\n"
        for idx, g in enumerate(queue, start=1):
            req_display = (
                int(g["req_multi"])
                if g["req_multi"] == int(g["req_multi"])
                else g["req_multi"]
            )
            emoji_str = g.get("emoji", "🎮")
            desc += f"**{idx}.** {emoji_str} {g['game_name']} — Req x{req_display}\n"

        desc += (
            f"\n💰 **Total Prizepool:** ${PRIZE_POOL:,.2f} USD\n"
            f"👑 **Total Winners:** {winner_count}\n"
        )

        if winner_count == 0:
            desc += (
                "\n"
                "❌ **No winners this month** — all queued games must be completed.\n"
                "💰 Prizepool does not carry over.\n"
            )
        else:
            desc += f"\n🎁 **Prize per winner:** ${prize_splits[0]:,.2f} USD\n\n"
            desc += "***Winners:***\n\n"
            for i, winner in enumerate(winners):
                uname = winner["username"]
                display = (uname[:-3] + "•••") if len(uname) > 3 else "•••"
                desc += f"👑 {display} — ${prize_splits[i]:,.2f}\n"
                desc += f"   {self.format_winner_game_multis(winner, queue)}\n"

        result_embed.description = desc
        result_embed.set_footer(text="AutoTip Engine Live • Payouts Sent Successfully")

        # ── Send tips ─────────────────────────────────────────────────────────
        failed_winners = []
        for i, winner in enumerate(winners):
            prize = prize_splits[i]

            if is_roovsflip_winner_paid(payout_year, payout_month, winner["uid"]):
                logger.info(
                    f"[RooVsFlip] Winner {winner['username']} already recorded for "
                    f"{payout_year}-{payout_month:02d}, skipping re-tip."
                )
                continue

            try:
                tip_response = await send_tip(
                    user_id=os.getenv("ROOBET_USER_ID"),
                    to_username=winner["username"],
                    to_user_id=winner["uid"],
                    amount=prize,
                )
                if tip_response.get("success"):
                    logger.info(
                        f"[RooVsFlip] Paid ${prize:.2f} to {winner['username']}"
                    )
                    save_tip_log(
                        winner["uid"],
                        winner["username"],
                        prize,
                        "roo_vs_flip",
                        month=payout_month,
                        year=payout_year,
                    )
                    record_roovsflip_payout(
                        payout_year, payout_month,
                        winner["uid"], winner["username"], prize,
                    )
                else:
                    logger.error(
                        f"[RooVsFlip] Tip failed for {winner['username']}: "
                        f"{tip_response.get('message')}"
                    )
                    failed_winners.append(winner["username"])
            except Exception as e:
                logger.error(
                    f"[RooVsFlip] Exception tipping {winner['username']}: {e}"
                )
                failed_winners.append(winner["username"])
            await asyncio.sleep(PAYOUT_DELAY_SECONDS)

        # If no winners, still mark month as processed
        if winner_count == 0:
            record_roovsflip_payout(
                payout_year, payout_month, "NO_WINNERS", "NO_WINNERS", 0.0
            )
            record_roovsflip_payout(
                payout_year, payout_month, "PAID_COMPLETE", "PAID_COMPLETE", 0.0
            )
        else:
            unpaid_winners = [
                w["username"]
                for w in winners
                if not is_roovsflip_winner_paid(payout_year, payout_month, w["uid"])
            ]
            if unpaid_winners:
                logger.error(
                    "[RooVsFlip] Month not finalized; unpaid winners remain: "
                    + ", ".join(unpaid_winners)
                )
                if history_channel:
                    await history_channel.send(
                        f"⚠️ Roo Vs Flip payout for **{payout_year}-{payout_month:02d}** "
                        f"is incomplete. Unpaid winners: **{len(unpaid_winners)}**."
                    )
                return
            record_roovsflip_payout(
                payout_year, payout_month, "PAID_COMPLETE", "PAID_COMPLETE", 0.0
            )

        # ── Post results to history channel ───────────────────────────────────
        if history_channel:
            ping = f"<@&{ROO_VS_FLIP_PING_ROLE_ID}>" if ROO_VS_FLIP_PING_ROLE_ID else None
            await history_channel.send(content=ping, embed=result_embed)

        # ── Copy draft queue to active for next month ──────────────────────────
        copy_roovsflip_draft_to_active()
        logger.info(f"[RooVsFlip] Draft queue copied to active queue.")

        # ── Reset for new month ───────────────────────────────────────────────
        set_roovsflip_event_start(new_start)
        # Setting to 0 makes get_leaderboard_message_id return 0 (falsy),
        # causing the next update cycle to post a fresh embed.
        save_leaderboard_message_id(0, "roovsflip_embed_message_id")
        logger.info(
            f"[RooVsFlip] Payout complete. Event reset, new start: {new_start}"
        )

    # ─── Admin Commands ───────────────────────────────────────────────────────

    @rvf.command(
        name="setgame",
        description="Set or overwrite a slot in the active queue.",
    )
    @app_commands.describe(
        position="Queue slot (1–5)",
        game_name="Display name for the game",
        game_identifier="Game identifier (e.g. pragmatic:vs20olympgate)",
        emoji="Emoji for this game (e.g. 🎰)",
        req_multi="Required multiplier to complete this game (e.g. 500)",
    )
    async def set_queue(
        self,
        interaction: discord.Interaction,
        position: int,
        game_name: str,
        game_identifier: str,
        emoji: str,
        req_multi: float,
    ):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        if not 1 <= position <= MAX_QUEUE_SIZE:
            await interaction.response.send_message(
                f"❌ Position must be between 1 and {MAX_QUEUE_SIZE}.", ephemeral=True
            )
            return
        if req_multi <= 0:
            await interaction.response.send_message(
                "❌ Required multiplier must be greater than 0.", ephemeral=True
            )
            return
        # Block duplicate game identifiers across other slots
        existing = get_roovsflip_queue()
        for g in existing:
            if g["game_identifier"] == game_identifier and g["position"] != position:
                await interaction.response.send_message(
                    f"❌ `{game_identifier}` is already in slot **{g['position']}**. "
                    f"Remove it first.",
                    ephemeral=True,
                )
                return
        clean_name = game_name.replace('"', "").replace("'", "")
        set_roovsflip_queue_slot(position, clean_name, game_identifier, emoji, req_multi)
        logger.info(
            f"[RooVsFlip] Slot {position} set: {emoji} {clean_name} ({game_identifier}) "
            f"req x{req_multi} by {interaction.user.id}"
        )
        await interaction.response.send_message(
            f"✅ Slot **{position}** set to **{emoji} {clean_name}** "
            f"(`{game_identifier}`) — Req: **x{req_multi}**",
            ephemeral=True,
        )

    @rvf.command(
        name="swap",
        description="Swap two positions in the active Roo Vs Flip queue.",
    )
    @app_commands.describe(
        position_1="First queue slot (1-5)",
        position_2="Second queue slot (1-5)",
    )
    async def swap_queue_positions(
        self,
        interaction: discord.Interaction,
        position_1: int,
        position_2: int,
    ):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        if not 1 <= position_1 <= MAX_QUEUE_SIZE or not 1 <= position_2 <= MAX_QUEUE_SIZE:
            await interaction.response.send_message(
                f"❌ Positions must both be between 1 and {MAX_QUEUE_SIZE}.",
                ephemeral=True,
            )
            return
        if position_1 == position_2:
            await interaction.response.send_message(
                "❌ Choose two different positions to swap.",
                ephemeral=True,
            )
            return

        success, message = swap_roovsflip_queue_positions(position_1, position_2)
        if not success:
            await interaction.response.send_message(
                f"❌ {message}",
                ephemeral=True,
            )
            return

        logger.info(
            f"[RooVsFlip] Swapped active queue slots {position_1} and {position_2} "
            f"by {interaction.user.id}"
        )
        await interaction.response.send_message(
            f"✅ Swapped active queue slots **{position_1}** and **{position_2}**.",
            ephemeral=True,
        )

    @rvf.command(
        name="queue",
        description="View the active and next-month Roo Vs Flip queues.",
    )
    async def view_queue(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        active_queue = get_roovsflip_queue()
        draft_queue = get_roovsflip_draft_queue()
        event_start = get_roovsflip_event_start()
        if not active_queue and not draft_queue:
            await interaction.response.send_message(
                "📋 Queues are empty. Use `/rvf setgame` to bootstrap the active "
                "queue, then `/rvf draftgame` for future months.",
                ephemeral=True,
            )
            return
        try:
            start_ts = int(
                datetime.fromisoformat(
                    event_start.replace("Z", "+00:00")
                ).timestamp()
            )
            start_str = f"<t:{start_ts}:F>"
        except Exception:
            start_str = event_start

        lines = [
            f"📋 **Roo Vs Flip Queues** — Current tracking from {start_str}\n"
        ]

        # Active (current month)
        lines.append("**🟢 ACTIVE (This Month):**")
        if active_queue:
            for g in active_queue:
                game_url = f"https://roobet.com/casino/game/{g['game_identifier']}"
                emoji_str = g.get('emoji', '🎮')
                lines.append(
                    f"**{g['position']}.** {emoji_str} [{g['game_name']}]({game_url})"
                    f" — Req: **x{g['req_multi']}**"
                )
            lines.append(f"`{len(active_queue)}/{MAX_QUEUE_SIZE} slots`\n")
        else:
            lines.append("*(empty)*\n")

        # Draft (next month)
        lines.append("**🔵 DRAFT (Next Month):**")
        if draft_queue:
            for g in draft_queue:
                game_url = f"https://roobet.com/casino/game/{g['game_identifier']}"
                emoji_str = g.get('emoji', '🎮')
                lines.append(
                    f"**{g['position']}.** {emoji_str} [{g['game_name']}]({game_url})"
                    f" — Req: **x{g['req_multi']}**"
                )
            lines.append(f"`{len(draft_queue)}/{MAX_QUEUE_SIZE} slots`")
        else:
            lines.append("*(empty — edit with `/rvf draftgame`)*")

        await interaction.response.send_message(
            "\n".join(lines), ephemeral=True
        )

    @rvf.command(
        name="cleargame",
        description="Remove one active queue slot, or clear the active queue.",
    )
    @app_commands.describe(
        position="Slot to remove (1–5). Omit to clear ALL slots."
    )
    async def clear_queue(
        self, interaction: discord.Interaction, position: int = None
    ):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        if position is not None and not 1 <= position <= MAX_QUEUE_SIZE:
            await interaction.response.send_message(
                f"❌ Position must be between 1 and {MAX_QUEUE_SIZE}.", ephemeral=True
            )
            return
        clear_roovsflip_queue_slot(position)
        if position is not None:
            await interaction.response.send_message(
                f"✅ Slot **{position}** cleared.", ephemeral=True
            )
        else:
            await interaction.response.send_message(
                "✅ All queue slots cleared.", ephemeral=True
            )

    @rvf.command(
        name="draftgame",
        description="Queue a game into next month's Roo Vs Flip draft.",
    )
    @app_commands.describe(
        position="Queue slot (1–5)",
        game_name="Display name for the game",
        game_identifier="Game identifier (e.g. pragmatic:vs20olympgate)",
        emoji="Emoji for this game (e.g. 🎰)",
        req_multi="Required multiplier to complete (e.g. 100, 250.5)",
    )
    async def queue_next_month(
        self,
        interaction: discord.Interaction,
        position: int,
        game_name: str,
        game_identifier: str,
        emoji: str,
        req_multi: float,
    ):
        """Queue a game for next month's Roo Vs Flip event (draft queue)."""
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        if not 1 <= position <= MAX_QUEUE_SIZE:
            await interaction.response.send_message(
                f"❌ Position must be between 1 and {MAX_QUEUE_SIZE}.", ephemeral=True
            )
            return
        if req_multi <= 0:
            await interaction.response.send_message(
                "❌ Required multiplier must be > 0.", ephemeral=True
            )
            return

        # Block duplicate game identifiers across other draft slots
        existing_draft = get_roovsflip_draft_queue()
        for g in existing_draft:
            if g["game_identifier"] == game_identifier and g["position"] != position:
                await interaction.response.send_message(
                    f"❌ `{game_identifier}` is already in draft slot **{g['position']}**. "
                    f"Remove it first.",
                    ephemeral=True,
                )
                return

        clean_name = game_name.replace('"', "").replace("'", "")
        set_roovsflip_draft_queue_slot(position, clean_name, game_identifier, emoji, req_multi)
        game_url = f"https://roobet.com/casino/game/{game_identifier}"
        await interaction.response.send_message(
            f"✅ {emoji} [**{clean_name}**]({game_url}) queued at slot **{position}** for next month (req: **x{req_multi}**).",
            ephemeral=True,
        )

    @rvf.command(
        name="refresh",
        description="Fetch fresh Roo Vs Flip data and update the live embed now.",
    )
    async def temp_fetch_update(self, interaction: discord.Interaction):
        """Immediately update the live embed with current data."""
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        try:
            queue = get_roovsflip_queue()
            if not queue:
                await interaction.followup.send(
                    "❌ Queue is empty. Nothing to fetch.", ephemeral=True
                )
                return
            event_start = get_roovsflip_event_start()
            game_data = await self.fetch_all_game_data(queue, event_start)
            participants = self.build_participant_list(queue, game_data)
            embed = self.build_embed(queue, participants, event_start)

            channel = (
                self.bot.get_channel(ROO_VS_FLIP_CHANNEL_ID)
                if ROO_VS_FLIP_CHANNEL_ID
                else None
            )
            if not channel:
                await interaction.followup.send(
                    "❌ Roo Vs Flip channel not configured.", ephemeral=True
                )
                return

            msg_id = get_leaderboard_message_id("roovsflip_embed_message_id")
            if msg_id and msg_id > 0:
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(embed=embed)
                except discord.NotFound:
                    msg = await channel.send(embed=embed)
                    save_leaderboard_message_id(msg.id, "roovsflip_embed_message_id")
            else:
                msg = await channel.send(embed=embed)
                save_leaderboard_message_id(msg.id, "roovsflip_embed_message_id")

            await interaction.followup.send(
                "✅ Embed updated with fresh data.", ephemeral=True
            )
        except Exception as e:
            logger.error(f"[RooVsFlip] Error in temp_fetch_update: {e}")
            await interaction.followup.send(
                f"❌ Error: {str(e)[:100]}", ephemeral=True
            )

    @rvf.command(
        name="preview",
        description="Preview the results embed as if Roo Vs Flip ended right now.",
    )
    async def temp_log_output(self, interaction: discord.Interaction):
        """Build and post a preview of the monthly results to the history channel."""
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True)
        try:
            queue = get_roovsflip_queue()
            if not queue:
                await interaction.followup.send(
                    "❌ Queue is empty. Nothing to preview.", ephemeral=True
                )
                return

            event_start = get_roovsflip_event_start()
            now = datetime.now(dt.UTC)
            game_data = await self.fetch_all_game_data(queue, event_start)
            participants = self.build_participant_list(queue, game_data)
            winners = [p for p in participants if p["is_winner"]]
            winner_count = len(winners)
            prize_splits = self.compute_prize_split(winner_count)

            try:
                start_ts = int(
                    datetime.fromisoformat(
                        event_start.replace("Z", "+00:00")
                    ).timestamp()
                )
            except Exception:
                start_ts = int(now.timestamp())

            result_embed = discord.Embed(
                title="🧪 Roo Vs Flip Payout Preview (If ended NOW)",
                color=discord.Color.gold(),
            )
            desc = f"**Challenge Period:** <t:{start_ts}:F> → <t:{int(now.timestamp())}:F>\n\n"

            desc += "***Challenge Games:***\n\n"
            for idx, g in enumerate(queue, start=1):
                req_display = (
                    int(g["req_multi"])
                    if g["req_multi"] == int(g["req_multi"])
                    else g["req_multi"]
                )
                emoji_str = g.get("emoji", "🎮")
                desc += f"**{idx}.** {emoji_str} {g['game_name']} — Req x{req_display}\n"

            desc += (
                f"\n💰 **Total Prizepool:** ${PRIZE_POOL:,.2f} USD\n"
                f"👑 **Total Winners:** {winner_count}\n"
            )

            if winner_count == 0:
                desc += (
                    "\n"
                    "❌ **No winners yet** — all queued games must be completed.\n"
                    "💰 Prizepool does not carry over.\n"
                )
            else:
                desc += f"\n🎁 **Prize per winner:** ${prize_splits[0]:,.2f} USD\n\n"
                desc += "***Winners:***\n\n"
                for i, winner in enumerate(winners):
                    uname = winner["username"]
                    display = (uname[:-3] + "•••") if len(uname) > 3 else "•••"
                    desc += f"👑 {display} — ${prize_splits[i]:,.2f}\n"
                    desc += f"   {self.format_winner_game_multis(winner, queue)}\n"

            result_embed.description = desc
            result_embed.set_footer(text="AutoTip Engine Live • Preview Only (No Payout Sent)")

            history_channel = (
                self.bot.get_channel(ROO_VS_FLIP_HISTORY_CHANNEL_ID)
                if ROO_VS_FLIP_HISTORY_CHANNEL_ID
                else None
            )
            if not history_channel:
                await interaction.followup.send(
                    "❌ History channel not configured.", ephemeral=True
                )
                return

            await history_channel.send(f"📌 **PREVIEW** (not real payout):\n", embed=result_embed)
            await interaction.followup.send(
                "✅ Preview posted to history channel.", ephemeral=True
            )

        except Exception as e:
            logger.error(f"[RooVsFlip] Error in temp_log_output: {e}")
            await interaction.followup.send(
                f"❌ Error: {str(e)[:100]}", ephemeral=True
            )

    @rvf.command(
        name="payout",
        description="Manually trigger the Roo Vs Flip payout and reset. ⚠️ Official payout.",
    )
    async def manual_result(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return
        now = datetime.now(dt.UTC)

        # Match automated payout behavior: settle the previous month.
        if now.month == 1:
            target_year, target_month = now.year - 1, 12
        else:
            target_year, target_month = now.year, now.month - 1

        if is_roovsflip_paid(target_year, target_month):
            await interaction.response.send_message(
                f"⚠️ Payout for **{target_year}-{target_month:02d}** has already been processed.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            f"⏳ Running Roo Vs Flip payout for **{target_year}-{target_month:02d}** now…",
            ephemeral=True,
        )
        await self.run_monthly_payout(target_year, target_month, automated=False)
        await interaction.followup.send(
            "✅ Roo Vs Flip payout complete. Event has been reset for the new period.",
            ephemeral=True,
        )


async def setup(bot):
    await bot.add_cog(RooVsFlip(bot))
