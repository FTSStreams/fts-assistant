import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import fetch_weighted_wager, send_tip, get_current_month_range
from db import (
    get_active_slot_challenge, set_active_slot_challenge, clear_active_slot_challenge, log_slot_challenge
)
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
CHALLENGE_CHANNEL_ID = int(os.getenv("CHALLENGE_CHANNEL_ID"))
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))

class SlotChallenge(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_challenge.start()

    @app_commands.command(name="setchallenge", description="Set a slot challenge for a specific game and multiplier.")
    @app_commands.describe(game_identifier="Game identifier (e.g. pragmatic:vs10bbbbrnd)", game_name="Game name for display", required_multi="Required multiplier (e.g. 100)", prize="Prize amount in USD")
    async def set_challenge(self, interaction: discord.Interaction, game_identifier: str, game_name: str, required_multi: float, prize: float):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to set a challenge.", ephemeral=True)
            return
        # Check if a challenge is already active
        active = get_active_slot_challenge()
        if active:
            await interaction.response.send_message("A slot challenge is already active. Please cancel it or wait for it to be completed.", ephemeral=True)
            return
        challenge_start_utc = datetime.now(dt.UTC).replace(microsecond=0).isoformat()
        set_active_slot_challenge(
            game_identifier, game_name, required_multi, prize, challenge_start_utc,
            interaction.user.id, interaction.user.display_name
        )
        channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
        if channel:
            embed = discord.Embed(
                title="🎰 Slot Challenge Started! 🎰",
                description=f"First to hit it wins!",
                color=discord.Color.gold()
            )
            embed.add_field(name="Game", value=game_name, inline=False)
            embed.add_field(name="Required Multiplier", value=f"x{required_multi}", inline=True)
            embed.add_field(name="Prize", value=f"${prize}", inline=True)
            embed.set_footer(text=f"Challenge start time (UTC): {challenge_start_utc}")
            await channel.send(embed=embed)
        await interaction.response.send_message("Slot challenge set and announced.", ephemeral=True)

    @app_commands.command(name="cancelchallenge", description="Cancel the current slot challenge.")
    async def cancel_challenge(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to cancel a challenge.", ephemeral=True)
            return
        active = get_active_slot_challenge()
        if not active:
            await interaction.response.send_message("No active slot challenge to cancel.", ephemeral=True)
            return
        log_slot_challenge(
            active["game_identifier"], active["game_name"], active["required_multi"], active["prize"],
            active["start_time"], datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
            active["posted_by"], active["posted_by_username"], None, None, None, "cancelled"
        )
        clear_active_slot_challenge()
        await interaction.response.send_message("Slot challenge cancelled.", ephemeral=True)
        channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
        if channel:
            await channel.send("❌ The current slot challenge has been cancelled by an admin.")

    @tasks.loop(minutes=7.5)
    async def check_challenge(self):
        active = get_active_slot_challenge()
        if not active:
            return
        start_date = active["start_time"]
        _, end_date = get_current_month_range()
        try:
            data = fetch_weighted_wager(start_date, end_date)
        except Exception as e:
            logger.error(f"Failed to fetch weighted wager data: {e}")
            return
        winners = []
        for entry in data:
            hm = entry.get("highestMultiplier")
            if not hm:
                continue
            if (
                hm.get("gameId") == active["game_identifier"]
                and hm.get("multiplier", 0) >= active["required_multi"]
            ):
                winners.append({
                    "uid": entry.get("uid"),
                    "username": entry.get("username"),
                    "multiplier": hm.get("multiplier", 0)
                })
        if winners:
            winner = max(winners, key=lambda x: x["multiplier"])
            # Tip out the prize
            tip_response = await send_tip(
                user_id=os.getenv("ROOBET_USER_ID"),
                to_username=winner["username"],
                to_user_id=winner["uid"],
                amount=active["prize"]
            )
            channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
            if tip_response.get("success"):
                embed = discord.Embed(
                    title="🏆 Slot Challenge Winner! 🏆",
                    description=f"Congrats to {winner['username']} for hitting x{winner['multiplier']:.2f} on {active['game_name']}! Prize: ${active['prize']} has been tipped out.",
                    color=discord.Color.green()
                )
                await channel.send(embed=embed)
                log_slot_challenge(
                    active["game_identifier"], active["game_name"], active["required_multi"], active["prize"],
                    active["start_time"], datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
                    active["posted_by"], active["posted_by_username"],
                    winner["uid"], winner["username"], winner["multiplier"], "completed"
                )
            else:
                await channel.send(f"❌ Failed to tip prize to {winner['username']}. Please check logs.")
            clear_active_slot_challenge()

    @check_challenge.before_loop
    async def before_challenge_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(SlotChallenge(bot))
