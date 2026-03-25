import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import send_tip, fetch_weighted_wager
from db import (
    ensure_roovsflip_tables,
    get_roovsflip_queue,
    set_roovsflip_queue_slot,
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
)
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
ROO_VS_FLIP_PING_ROLE_ID = os.getenv("ROO_VS_FLIP_PING_ROLE_ID")

PRIZE_POOL = 10.00
MAX_QUEUE_SIZE = 5
EMBED_MAX_PARTICIPANTS = 15  # Keep description under Discord's 4096-char limit
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
        """Self-heal: if previous month is not finalized, run payout once at startup."""
        now = datetime.now(dt.UTC)
        if now.month == 1:
            payout_year, payout_month = now.year - 1, 12
        else:
            payout_year, payout_month = now.year, now.month - 1

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

        # End-of-current-month timestamp
        if now.month == 12:
            end_dt = now.replace(
                year=now.year + 1, month=1, day=1,
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            end_dt = now.replace(
                month=now.month + 1, day=1,
                hour=0, minute=0, second=0, microsecond=0
            )
        end_ts = int(end_dt.timestamp())

        total_games = len(queue)
        winners = [p for p in participants if p["is_winner"]]
        winner_count = len(winners)
        prize_splits = self.compute_prize_split(winner_count)
        prize_str = (
            f"`${prize_splits[0]:,.2f} each`"
            if winner_count > 0
            else "`N/A`"
        )

        desc = (
            f"🗓️ **Challenge Period:**\n"
            f"From: <t:{start_ts}:F>\n"
            f"To: <t:{end_ts}:F>\n\n"
            f"⏰ **Last Updated:** <t:{now_ts}:R>\n\n"
            f"💰 **Total Prizepool:** `${PRIZE_POOL:,.2f} USD`\n"
            f"👑 **Current Winners:** `{winner_count}`\n"
            f"🎁 **Current Prize:** {prize_str}\n\n"
        )

        # Rules & Disclosure
        desc += (
            "📜 **Roo Vs. Flip Rules & Disclosure:**\n"
            "• Beat Flip's multipliers with a minimum bet size of $0.20 USD\n"
            f"• ${PRIZE_POOL:,.2f} prize pool will be split between all qualifying players\n"
            "• ALL challenges must be completed to win\n\n"
        )

        # Queued games
        desc += f"🎮 **Required Games ({total_games}):**\n"
        for g in queue:
            game_url = f"https://roobet.com/casino/game/{g['game_identifier']}"
            emoji_str = g.get("emoji", "🎮")
            req_display = (
                int(g["req_multi"])
                if g["req_multi"] == int(g["req_multi"])
                else g["req_multi"]
            )
            desc += (
                f"**{g['position']}.** {emoji_str} [{g['game_name']}]({game_url})"
                f" — `Req x{req_display}`\n"
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
                display = (uname[:-3] + "\\*\\*\\*") if len(uname) > 3 else "\\*\\*\\*"
                completion_badge = " 🏆" if p["completions"] == total_games else ""
                desc += (
                    f"\n**#{i + 1} — {display}**"
                    f" — `{p['completions']}/{total_games} Complete`{completion_badge}\n"
                )
                row = ""
                for g in queue:
                    gid = g["game_identifier"]
                    info = p["games"].get(gid)
                    pos = g["position"]
                    if info is None:
                        row += f"`{pos})` ⏳  "
                    elif info["met"]:
                        row += f"`{pos})` ✅ `x{info['multi']:,.2f}`  "
                    else:
                        row += f"`{pos})` ❌ `x{info['multi']:,.2f}`  "
                desc += row.rstrip() + "\n"

            if len(qualified_participants) > EMBED_MAX_PARTICIPANTS:
                extra = len(qualified_participants) - EMBED_MAX_PARTICIPANTS
                desc += f"\n*...and {extra} more participant(s)*\n"

        desc += (
            "\n**Legend:** ✅ requirement met"
            " | ❌ played but below req"
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
            text="Prize pool auto-pays at midnight UTC on the 1st of each month."
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
        embed = self.build_embed(queue, participants, event_start)
        await self.post_or_edit_embed(embed)

    @update_embed.before_loop
    async def before_update_embed(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=5)
    async def monthly_payout_check(self):
        """
        Fire at UTC midnight on the 1st of each month (00:00–00:04 window).
        Pays out the previous month's event and resets for the new month.
        """
        try:
            now = datetime.now(dt.UTC)
            if not (now.day == 1 and now.hour == 0 and 0 <= now.minute <= 4):
                return

            # Determine which month just ended
            if now.month == 1:
                payout_year, payout_month = now.year - 1, 12
            else:
                payout_year, payout_month = now.year, now.month - 1

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
            title=f"🏆 Roo Vs Flip — {payout_year}/{payout_month:02d} Final Results",
            color=discord.Color.gold(),
        )
        desc = (
            f"**Challenge Period:** <t:{start_ts}:F> → <t:{int(now.timestamp())}:F>\n\n"
            f"💰 **Total Prizepool:** `${PRIZE_POOL:,.2f} USD`\n"
            f"👑 **Total Winners:** `{winner_count}`\n\n"
        )

        if winner_count == 0:
            desc += (
                "❌ **No winners this month** — all queued games must be completed.\n"
                "💰 Prizepool does not carry over.\n"
            )
        else:
            desc += f"🎁 **Prize per winner:** `${prize_splits[0]:,.2f} USD`\n\n"
            desc += "🥇 **Winners:**\n"
            for i, winner in enumerate(winners):
                uname = winner["username"]
                display = (
                    (uname[:-3] + "\\*\\*\\*") if len(uname) > 3 else "\\*\\*\\*"
                )
                desc += f"**{i + 1}.** {display} — `${prize_splits[i]:,.2f}`\n"

        result_embed.description = desc

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
                title=f"🏆 Roo Vs Flip — Preview (If ended NOW)",
                color=discord.Color.gold(),
            )
            desc = (
                f"**Challenge Period:** <t:{start_ts}:F> → <t:{int(now.timestamp())}:F>\n\n"
                f"💰 **Total Prizepool:** `${PRIZE_POOL:,.2f} USD`\n"
                f"👑 **Total Winners:** `{winner_count}`\n\n"
            )

            if winner_count == 0:
                desc += (
                    "❌ **No winners yet** — all queued games must be completed.\n"
                    "💰 Prizepool does not carry over.\n"
                )
            else:
                desc += f"🎁 **Prize per winner:** `${prize_splits[0]:,.2f} USD`\n\n"
                desc += "🥇 **Winners:**\n"
                for i, winner in enumerate(winners):
                    uname = winner["username"]
                    display = (
                        (uname[:-3] + "\\*\\*\\*") if len(uname) > 3 else "\\*\\*\\*"
                    )
                    desc += f"**{i + 1}.** {display} — `${prize_splits[i]:,.2f}`\n"

            result_embed.description = desc

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
