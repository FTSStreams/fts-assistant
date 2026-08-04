import discord
from discord import app_commands
from discord.ext import commands
from utils import fetch_weighted_wager, fetch_total_wager, get_current_month_range, get_current_week_range
from db import get_roovsflip_queue, get_roovsflip_event_start
import os
import logging
import asyncio
from datetime import datetime
import datetime as dt

logger = logging.getLogger(__name__)

BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))
GUILD_ID = int(os.getenv("GUILD_ID"))

ROLES_CHANNEL_ID = int(os.getenv("ROLES_CHANNEL_ID", "1440843895360590028"))
WAGER_LB_CHANNEL_ID = int(os.getenv("WAGER_LEADERBOARD_CHANNEL_ID", "1324462489404051487"))
MULTI_LB_CHANNEL_ID = int(os.getenv("MULTI_LEADERBOARD_CHANNEL_ID", "1352322188102991932"))
SLOT_CHALLENGES_CHANNEL_ID = int(os.getenv("SLOT_CHALLENGES_CHANNEL_ID", "1385820512529158226"))
ROO_VS_FLIP_CHANNEL_ID = int(os.getenv("ROO_VS_FLIP_CHANNEL_ID", "1486202172378189925"))
MILESTONE_CHANNEL_ID = int(os.getenv("MILESTONE_PRIZES_CHANNEL_ID", "1362517492651790416"))
FTS_VAULT_CHANNEL_ID = int(os.getenv("CHECKIN_BALANCE_LEADERBOARD_CHANNEL_ID", "1501283696928362497"))
GTB_CHANNEL_ID = int(os.getenv("GTB_CHANNEL_ID", "1527380205759500369"))
ROO_VS_FLIP_CYCLE_DAYS = int(os.getenv("ROO_VS_FLIP_CYCLE_DAYS", "7"))


async def _fetch_live_stats():
    """
    Returns a dict with:
      - wager_10th: float  (10th place weighted wager this month, 0 if unavailable)
      - multi_3rd:  float  (3rd place highest multiplier this week, 0 if unavailable)
      - rvf_eligible: int  (number of participants eligible in current RVF cycle)
    """
    stats = {"wager_10th": 0.0, "multi_3rd": 0.0, "rvf_eligible": 0}

    # ── Monthly wager 10th place ──────────────────────────────────────────────
    try:
        start_date, end_date = get_current_month_range()
        wager_data = await asyncio.to_thread(fetch_total_wager, start_date, end_date)
        sorted_wager = sorted(
            [e for e in wager_data if isinstance(e.get("wagered"), (int, float)) and e["wagered"] > 0],
            key=lambda x: x["wagered"],
            reverse=True,
        )
        if len(sorted_wager) >= 10:
            stats["wager_10th"] = float(sorted_wager[9]["wagered"])
        elif sorted_wager:
            stats["wager_10th"] = float(sorted_wager[-1]["wagered"])
    except Exception as e:
        logger.warning(f"[Welcome] Failed to fetch wager 10th: {e}")

    # ── Weekly multi 3rd place ────────────────────────────────────────────────
    try:
        week_start, week_end = get_current_week_range()
        weekly_data = await asyncio.to_thread(fetch_weighted_wager, week_start, week_end)
        multi_data = sorted(
            [e for e in weekly_data if e.get("highestMultiplier") and float(e["highestMultiplier"].get("multiplier", 0)) > 0],
            key=lambda x: float(x["highestMultiplier"]["multiplier"]),
            reverse=True,
        )
        if len(multi_data) >= 3:
            stats["multi_3rd"] = float(multi_data[2]["highestMultiplier"]["multiplier"])
        elif multi_data:
            stats["multi_3rd"] = float(multi_data[-1]["highestMultiplier"]["multiplier"])
    except Exception as e:
        logger.warning(f"[Welcome] Failed to fetch multi 3rd: {e}")

    # ── RVF eligible participants ─────────────────────────────────────────────
    try:
        queue = get_roovsflip_queue()
        event_start = get_roovsflip_event_start()
        if queue and event_start:
            player_map: dict = {}
            for game in queue:
                gid = game["game_identifier"]
                req = float(game["req_multi"])
                entries = await asyncio.to_thread(fetch_weighted_wager, event_start, None, gid)
                for entry in entries:
                    uid = entry.get("uid")
                    hm = entry.get("highestMultiplier")
                    if not (uid and hm):
                        continue
                    if hm.get("gameId") != gid:
                        continue
                    multi = float(hm.get("multiplier", 0))
                    if uid not in player_map:
                        player_map[uid] = {}
                    player_map[uid][gid] = multi >= req
                await asyncio.sleep(1)

            total_games = len(queue)
            stats["rvf_eligible"] = sum(
                1 for games in player_map.values()
                if sum(1 for met in games.values() if met) == total_games
            )
    except Exception as e:
        logger.warning(f"[Welcome] Failed to fetch RVF eligible: {e}")

    return stats


def _build_welcome_embed(stats: dict) -> discord.Embed:
    wager_10th = stats["wager_10th"]
    multi_3rd = stats["multi_3rd"]
    rvf_eligible = stats["rvf_eligible"]

    wager_line = f"Current 10th place cutoff: **${wager_10th:,.2f}** wagered this month" if wager_10th else "No data yet this month"
    multi_line = f"Current 3rd place cutoff: **x{multi_3rd:,.2f}**" if multi_3rd else "No data yet this week"
    rvf_line = f"Current eligible participants: **{rvf_eligible}**"

    description = (
        "Here's a quick guide to get you started!\n\n"
        "Use code **FlipTheSwitch** on Roobet and you're automatically\n"
        "enrolled and powered by our AutoTip Engine, the only fully\n"
        "automated tipping system on Roobet that pays out your\n"
        "rewards **INSTANTLY** and **AUTOMATICALLY**.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎭 **Get Your Roles**\nStay in the loop → <#{ROLES_CHANNEL_ID}>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 **Monthly Wager Leaderboard**\n<#{WAGER_LB_CHANNEL_ID}>\n{wager_line}\n\n"
        f"⚡ **Weekly Multi Leaderboard**\n<#{MULTI_LB_CHANNEL_ID}>\n{multi_line}\n\n"
        f"🎰 **Slot Challenges**\n<#{SLOT_CHALLENGES_CHANNEL_ID}>\n\n"
        f"🆚 **Roo Vs Flip**\n<#{ROO_VS_FLIP_CHANNEL_ID}>\n{rvf_line}\n\n"
        f"🎯 **Milestone Prizes**\n<#{MILESTONE_CHANNEL_ID}>\n\n"
        f"🏦 **FTS Vault**\n<#{FTS_VAULT_CHANNEL_ID}>\n\n"
        f"🎲 **Guess the Balance**\n<#{GTB_CHANNEL_ID}>"
    )

    embed = discord.Embed(
        title="👋 Welcome to FTS!",
        description=description,
        color=discord.Color.gold(),
    )
    embed.set_footer(text="FTS Assistant • Use /help anytime to pull this up again")
    return embed


class Welcome(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="welcome",
        description="Post a welcome guide for up to 5 newly verified members (owner only).",
    )
    @app_commands.describe(
        user1="First member to welcome",
        user2="Second member (optional)",
        user3="Third member (optional)",
        user4="Fourth member (optional)",
        user5="Fifth member (optional)",
    )
    async def welcome(
        self,
        interaction: discord.Interaction,
        user1: discord.Member,
        user2: discord.Member = None,
        user3: discord.Member = None,
        user4: discord.Member = None,
        user5: discord.Member = None,
    ):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message(
                "❌ You do not have permission to use this command.", ephemeral=True
            )
            return

        await interaction.response.defer()

        members = [m for m in [user1, user2, user3, user4, user5] if m is not None]
        stats = await _fetch_live_stats()
        embed = _build_welcome_embed(stats)

        mentions = " ".join(m.mention for m in members)
        await interaction.followup.send(content=mentions, embed=embed)

    @app_commands.command(
        name="help",
        description="Show the FTS quick guide with live stats (only visible to you).",
    )
    async def help_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        stats = await _fetch_live_stats()
        embed = _build_welcome_embed(stats)
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Welcome(bot))
