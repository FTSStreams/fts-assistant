import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import fetch_weighted_wager, send_tip, get_current_month_range
from db import get_db_connection, release_db_connection
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
        self.challenge = None  # Store current challenge as a dict
        self.challenge_winner = None
        self.check_challenge.start()

    @app_commands.command(name="setchallenge", description="Set a slot challenge for a specific game and multiplier.")
    @app_commands.describe(game_identifier="Game identifier (e.g. pragmatic:vs10bbbbrnd)", required_multi="Required multiplier (e.g. 100)", prize="Prize amount in USD")
    async def set_challenge(self, interaction: discord.Interaction, game_identifier: str, required_multi: float, prize: float):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to set a challenge.", ephemeral=True)
            return
        self.challenge = {
            "game_identifier": game_identifier,
            "required_multi": required_multi,
            "prize": prize,
            "active": True
        }
        self.challenge_winner = None
        channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
        if channel:
            await channel.send(f"🎰 **Slot Challenge Started!** 🎰\nGame: `{game_identifier}`\nRequired Multiplier: x{required_multi}\nPrize: ${prize}\nFirst to hit it wins!")
        await interaction.response.send_message("Slot challenge set and announced.", ephemeral=True)

    @tasks.loop(minutes=7.5)
    async def check_challenge(self):
        if not self.challenge or not self.challenge.get("active"):
            return
        start_date, end_date = get_current_month_range()
        try:
            data = fetch_weighted_wager(start_date, end_date)
        except Exception as e:
            logger.error(f"Failed to fetch weighted wager data: {e}")
            return
        # Find all users who hit the required multiplier on the specified game
        winners = []
        for entry in data:
            hm = entry.get("highestMultiplier")
            if not hm:
                continue
            if (
                hm.get("gameId") == self.challenge["game_identifier"]
                and hm.get("multiplier", 0) >= self.challenge["required_multi"]
            ):
                winners.append({
                    "uid": entry.get("uid"),
                    "username": entry.get("username"),
                    "multiplier": hm.get("multiplier", 0)
                })
        if winners:
            # Pick the user with the highest multiplier
            winner = max(winners, key=lambda x: x["multiplier"])
            if not self.challenge_winner or winner["uid"] != self.challenge_winner.get("uid"):
                # Tip out the prize
                tip_response = await send_tip(
                    user_id=os.getenv("ROOBET_USER_ID"),
                    to_username=winner["username"],
                    to_user_id=winner["uid"],
                    amount=self.challenge["prize"]
                )
                channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
                if tip_response.get("success"):
                    await channel.send(f"🏆 **Slot Challenge Winner!** 🏆\nCongrats to {winner['username']} for hitting x{winner['multiplier']:.2f} on `{self.challenge['game_identifier']}`! Prize: ${self.challenge['prize']} has been tipped out.")
                else:
                    await channel.send(f"❌ Failed to tip prize to {winner['username']}. Please check logs.")
                self.challenge["active"] = False
                self.challenge_winner = winner

    @check_challenge.before_loop
    async def before_challenge_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(SlotChallenge(bot))
