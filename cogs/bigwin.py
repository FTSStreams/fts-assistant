import discord
from discord import app_commands
from discord.ext import commands
import logging
import os

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))

class BigWin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="bigwin", description="Post a big win announcement")
    @app_commands.describe(
        game_name="Name of the game",
        replay_link="Replay URL for the win",
        bet_size="Bet amount in USD",
        payout="Payout amount in USD"
    )
    @app_commands.default_permissions(administrator=True)
    async def bigwin(
        self, 
        interaction: discord.Interaction, 
        game_name: str, 
        replay_link: str,
        bet_size: float,
        payout: float
    ):
        """Post a big win announcement with calculated multiplier"""
        
        # Validate inputs
        if bet_size <= 0:
            await interaction.response.send_message("⚠️ Bet size must be greater than 0.", ephemeral=True)
            return
        
        if payout <= 0:
            await interaction.response.send_message("⚠️ Payout must be greater than 0.", ephemeral=True)
            return
        
        # Calculate multiplier
        multiplier = payout / bet_size
        
        # Create embed
        embed = discord.Embed(
            title="🎰 BIG WIN ALERT 🎰",
            description=f"**Game:** {game_name}\n"
                       f"**Multi:** {multiplier:,.2f}x\n"
                       f"**Payout:** ${payout:,.2f} (${bet_size:,.2f} Bet)",
            color=discord.Color.gold()
        )
        
        # Get role ID from environment
        bigwin_role_id = os.getenv("BIGWIN_ROLE_ID")
        role_ping = f"<@&{bigwin_role_id}>" if bigwin_role_id else ""
        
        # Combine role ping, embed, and replay link
        content = f"{role_ping}\n{replay_link}" if role_ping else replay_link
        
        # Send the big win announcement
        await interaction.response.send_message(content=content, embed=embed)
        
        logger.info(f"[BigWin] Posted by {interaction.user} - Game: {game_name}, Multiplier: {multiplier:.2f}x, Payout: ${payout:.2f}")

async def setup(bot):
    await bot.add_cog(BigWin(bot))
