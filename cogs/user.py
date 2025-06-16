import discord
from discord import app_commands
from discord.ext import commands
from utils import fetch_total_wager, fetch_weighted_wager
import os
from datetime import datetime
import datetime as dt

GUILD_ID = int(os.getenv("GUILD_ID"))

class User(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="mywager", description="Check your personal wager stats for the current month using your Roobet username")
    @app_commands.describe(username="Your Roobet username")
    async def mywager(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()
        start_date = "2025-06-01T00:00:00"
        end_date = "2025-06-30T23:59:59"
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
            await interaction.followup.send(f"❌ No user found with username '{username}' who wagered in June 2025.", ephemeral=True)
            return
        total_wager_data = fetch_total_wager(start_date, end_date)
        total_wager = 0
        weighted_wager = 0
        for entry in total_wager_data:
            if entry.get("uid") == roobet_uid:
                total_wager = entry.get("wagered", 0) if isinstance(entry.get("wagered"), (int, float)) else 0
                break
        for entry in weighted_wager_data:
            if entry.get("uid") == roobet_uid:
                weighted_wager = entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) else 0
                break
        embed = discord.Embed(
            title=f"🎰 Your Wager Stats, {username}! 🎰",
            description=(
                f"💰 **Total Wager**: **${total_wager:,.2f} USD** 💸\n"
                f"✨ **Weighted Wager**: **${weighted_wager:,.2f} USD** 🌟\n"
                f"🔥 Keep betting to climb the ranks! 🎲"
            ),
            color=discord.Color.gold()
        )
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        embed.set_footer(text=f"🕒 Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(User(bot))
