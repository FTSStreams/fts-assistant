import discord
from discord import app_commands
from discord.ext import commands
import aiohttp
import os
import logging

class Favorite(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger(__name__)

    @app_commands.command(name="Favorite", description="Get all affiliate stats for a user and print to console.")
    @app_commands.describe(username="The affiliate's username.")
    async def favorite(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer(thinking=True, ephemeral=True)
        # You may need to map username to userId. For now, assume username == userId
        user_id = username  # Replace with actual mapping if needed
        url = "https://roobetconnect.com/affiliate/v2/stats"
        headers = {
            "Authorization": f"Bearer {os.getenv('ROOBET_API_TOKEN')}"
        }
        params = {"userId": user_id}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                data = await resp.json()
                print("/Favorite command response:")
                print(data)
                await interaction.followup.send("Data fetched and printed to console.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Favorite(bot))
