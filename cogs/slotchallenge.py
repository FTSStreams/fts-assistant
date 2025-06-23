import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import fetch_weighted_wager, send_tip, get_current_month_range
from db import (
    get_all_active_slot_challenges, add_active_slot_challenge, remove_active_slot_challenge, update_challenge_message_id, log_slot_challenge
)
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
CHALLENGE_CHANNEL_ID = int(os.getenv("CHALLENGE_CHANNEL_ID"))
LOGS_CHANNEL_ID = 1386537169170071572  # Winner/cancel log channel
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))

class SlotChallenge(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_challenge.start()

    @app_commands.command(name="setchallenge", description="Set a slot challenge for a specific game and multiplier.")
    @app_commands.describe(game_identifier="Game identifier (e.g. pragmatic:vs10bbbbrnd)", game_name="Game name for display", required_multi="Required multiplier (e.g. 100)", prize="Prize amount in USD", emoji="Optional emoji for this challenge", min_bet="Minimum bet size in USD (optional)")
    async def set_challenge(self, interaction: discord.Interaction, game_identifier: str, game_name: str, required_multi: float, prize: float, emoji: str = None, min_bet: float = None):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to set a challenge.", ephemeral=True)
            return
        active = get_all_active_slot_challenges()
        if len(active) >= 10:
            await interaction.response.send_message("There are already 10 active slot challenges. Please cancel one before adding another.", ephemeral=True)
            return
        challenge_start_utc = datetime.now(dt.UTC).replace(microsecond=0).isoformat()
        challenge_id = add_active_slot_challenge(
            game_identifier, game_name, required_multi, prize, challenge_start_utc,
            interaction.user.id, interaction.user.display_name, None, emoji, min_bet
        )
        # Update or create the single embed listing all challenges
        await self.update_challenges_embed()
        await interaction.response.send_message(f"Slot challenge set and announced. Challenge ID: {challenge_id}", ephemeral=True)

    async def update_challenges_embed(self):
        channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
        if not channel:
            return
        active = get_all_active_slot_challenges()
        if not active:
            # Optionally delete the embed if no challenges remain
            return
        # Build a styled description for all challenges
        now_ts = int(datetime.now(dt.UTC).timestamp())
        desc = f"⏰ **Last Updated:** <t:{now_ts}:R>\n\n"
        desc += "First to hit the required multiplier wins the prize!\n"
        desc += "All prizes are paid out automatically by our secure system.\n"
        desc += "See <#{}> for payout logs!\n\n".format(LOGS_CHANNEL_ID)
        for challenge in active:
            try:
                dt_obj = challenge['start_time']
                if isinstance(dt_obj, str):
                    dt_obj = datetime.fromisoformat(dt_obj)
                unix_ts = int(dt_obj.timestamp())
                start_str = f'<t:{unix_ts}:f>'  # Discord timestamp (long date/time)
            except Exception:
                start_str = str(challenge['start_time'])
            emoji = challenge.get('emoji') or '🎰'
            min_bet_str = f"  **Min Bet:** `${challenge['min_bet']}`" if challenge.get('min_bet') else ""
            desc += f"**`#{challenge['challenge_id']}` | {emoji} {challenge['game_name']}**\n"
            desc += f"**Multiplier:** `x{challenge['required_multi']}`  **Prize:** `${challenge['prize']}`{min_bet_str}\n"
            desc += f"**Start:** {start_str}\n\n"
        embed = discord.Embed(
            title="🎰 __Active Slot Challenges__ 🎰",
            description=desc,
            color=discord.Color.gold()
        )
        # Find the existing embed message (if any)
        message_id = None
        for challenge in active:
            if challenge['message_id']:
                message_id = challenge['message_id']
                break
        if message_id:
            try:
                msg = await channel.fetch_message(message_id)
                await msg.edit(embed=embed)
            except discord.errors.NotFound:
                # Message was deleted, send a new one and update all active challenges with new message id
                msg = await channel.send(embed=embed)
                for challenge in active:
                    update_challenge_message_id(challenge['challenge_id'], msg.id)
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit or send messages in challenge channel.")
        else:
            msg = await channel.send(embed=embed)
            for challenge in active:
                update_challenge_message_id(challenge['challenge_id'], msg.id)

    @app_commands.command(name="cancelchallenge", description="Cancel a specific slot challenge by its ID.")
    @app_commands.describe(challenge_id="The ID of the challenge to cancel.")
    async def cancel_challenge(self, interaction: discord.Interaction, challenge_id: int):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to cancel a challenge.", ephemeral=True)
            return
        active = get_all_active_slot_challenges()
        challenge = next((c for c in active if c['challenge_id'] == challenge_id), None)
        if not challenge:
            await interaction.response.send_message(f"No active slot challenge found with ID {challenge_id}.", ephemeral=True)
            return
        log_slot_challenge(
            challenge["game_identifier"], challenge["game_name"], challenge["required_multi"], challenge["prize"],
            challenge["start_time"], datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
            challenge["posted_by"], challenge["posted_by_username"], None, None, None, "cancelled"
        )
        remove_active_slot_challenge(challenge_id)
        await self.update_challenges_embed()
        await interaction.response.send_message(f"Slot challenge ID {challenge_id} cancelled.", ephemeral=True)
        # Log to logs channel
        logs_channel = self.bot.get_channel(LOGS_CHANNEL_ID)
        if logs_channel:
            embed = discord.Embed(
                title="❌ Slot Challenge Cancelled",
                description=f"Challenge ID {challenge_id} ({challenge['game_name']}) was cancelled by an admin.",
                color=discord.Color.red()
            )
            embed.add_field(name="Required Multiplier", value=f"x{challenge['required_multi']}", inline=True)
            embed.add_field(name="Prize", value=f"${challenge['prize']}", inline=True)
            embed.set_footer(text=f"Challenge start time (UTC): {challenge['start_time']}")
            await logs_channel.send(embed=embed)

    @tasks.loop(minutes=7.5)
    async def check_challenge(self):
        active = get_all_active_slot_challenges()
        if not active:
            return
        # Gather all start times for active challenges
        start_dates = [c["start_time"] for c in active]
        _, end_date = get_current_month_range()
        try:
            data = fetch_weighted_wager(min(start_dates), end_date)
        except Exception as e:
            logger.error(f"Failed to fetch weighted wager data: {e}")
            return
        completed_ids = set()
        for challenge in active:
            winners = []
            for entry in data:
                hm = entry.get("highestMultiplier")
                if not hm:
                    continue
                # Only count as winner if bet meets min_bet (if set)
                bet = hm.get("bet", 0)
                min_bet = challenge.get("min_bet")
                if (
                    hm.get("gameId") == challenge["game_identifier"]
                    and hm.get("multiplier", 0) >= challenge["required_multi"]
                    and (min_bet is None or bet >= min_bet)
                ):
                    winners.append({
                        "uid": entry.get("uid"),
                        "username": entry.get("username"),
                        "multiplier": hm.get("multiplier", 0),
                        "bet": bet,
                        "payout": hm.get("payout", 0)
                    })
            if winners:
                # Sort by multiplier, show up to top 2
                winners_sorted = sorted(winners, key=lambda x: x["multiplier"], reverse=True)
                winner = winners_sorted[0]
                second = winners_sorted[1] if len(winners_sorted) > 1 else None
                # Tip out the prize to first place
                tip_response = await send_tip(
                    user_id=os.getenv("ROOBET_USER_ID"),
                    to_username=winner["username"],
                    to_user_id=winner["uid"],
                    amount=challenge["prize"]
                )
                logs_channel = self.bot.get_channel(LOGS_CHANNEL_ID)
                if tip_response.get("success"):
                    embed = discord.Embed(
                        title="🏆 Slot Challenge Results! 🏆",
                        description=f"**1st Place:** {winner['username']}\nMultiplier: x{winner['multiplier']:.2f}",
                        color=discord.Color.green()
                    )
                    if second:
                        embed.description += f"\n\n**2nd Place:** {second['username']}\nMultiplier: x{second['multiplier']:.2f}"
                    # Show bet and payout if available
                    embed.add_field(name="Bet Size", value=f"${winner.get('bet', '?')}", inline=True)
                    embed.add_field(name="Payout", value=f"${winner.get('payout', '?')}", inline=True)
                    embed.add_field(name="Required Multiplier", value=f"x{challenge['required_multi']}", inline=True)
                    embed.add_field(name="Prize", value=f"${challenge['prize']}", inline=True)
                    embed.add_field(name="Game", value=challenge['game_name'], inline=True)
                    if challenge.get('min_bet'):
                        embed.add_field(name="Minimum Bet", value=f"${challenge['min_bet']}", inline=True)
                    embed.set_footer(text=f"Challenge start: <t:{int(datetime.fromisoformat(str(challenge['start_time'])).timestamp())}:f>")
                    if logs_channel:
                        await logs_channel.send(embed=embed)
                    log_slot_challenge(
                        challenge["game_identifier"], challenge["game_name"], challenge["required_multi"], challenge["prize"],
                        challenge["start_time"], datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
                        challenge["posted_by"], challenge["posted_by_username"],
                        winner["uid"], winner["username"], winner["multiplier"], "completed"
                    )
                else:
                    if logs_channel:
                        await logs_channel.send(f"❌ Failed to tip prize to {winner['username']}. Please check logs.")
                completed_ids.add(challenge["challenge_id"])
        # Remove completed challenges and update embed
        for cid in completed_ids:
            remove_active_slot_challenge(cid)
        if completed_ids:
            await self.update_challenges_embed()

    @check_challenge.before_loop
    async def before_challenge_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(SlotChallenge(bot))
