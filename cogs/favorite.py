import discord
from discord import app_commands
from discord.ext import commands
import aiohttp
import os
import logging
from utils import fetch_weighted_wager, get_current_month_range

class Favorite(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger(__name__)

    @app_commands.command(name="favorite", description="Get all affiliate stats for a user and print to console.")
    @app_commands.describe(username="The affiliate's username.")
    async def favorite(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer(thinking=True, ephemeral=True)
        # Step 1: Find UID for the username
        start_date = "2025-01-01T00:00:00"
        end_date = "2025-12-31T23:59:59"
        weighted_wager_data = fetch_weighted_wager(start_date, end_date)
        username_lower = username.lower()
        roobet_uid = None
        for entry in weighted_wager_data:
            entry_username = entry.get("username", "").lower()
            if username_lower == entry_username:
                roobet_uid = entry.get("uid")
                username = entry.get("username")
                break
        if not roobet_uid:
            await interaction.followup.send(f"❌ No user found with username '{username}' who wagered in 2025.", ephemeral=True)
            return
        # Step 2: Fetch affiliate stats using UID
        url = "https://roobetconnect.com/affiliate/v2/stats"
        headers = {
            "Authorization": f"Bearer {os.getenv('ROOBET_API_TOKEN')}"
        }
        params = {"userId": roobet_uid}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                data = await resp.json()
                print("/favorite command response:")
                print(data)
                await interaction.followup.send("Data fetched and printed to console.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Favorite(bot))
