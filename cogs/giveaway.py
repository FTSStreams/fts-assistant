import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import random
from datetime import datetime, timedelta
import datetime as dt
import logging
import os

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))

class Giveaway(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="giveaway", description="Start a giveaway with reactions")
    @app_commands.describe(
        time_minutes="Duration of the giveaway in minutes",
        prize="What the winner will receive"
    )
    @app_commands.default_permissions(administrator=True)
    async def giveaway(self, interaction: discord.Interaction, time_minutes: int, prize: str):
        """Start a giveaway that users can enter by reacting"""
        
        # Validate inputs
        if time_minutes < 1:
            await interaction.response.send_message("⚠️ Time must be at least 1 minute.", ephemeral=True)
            return
        
        if time_minutes > 10080:  # 1 week max
            await interaction.response.send_message("⚠️ Maximum giveaway duration is 1 week (10080 minutes).", ephemeral=True)
            return
        
        # Calculate end time
        now = datetime.now(dt.UTC)
        start_timestamp = int(now.timestamp())
        end_time = now + timedelta(minutes=time_minutes)
        end_timestamp = int(end_time.timestamp())
        
        # Create embed
        embed = discord.Embed(
            title="🎉 GIVEAWAY 🎉",
            description=f"**Prize:** {prize}\n\n"
                       f"React with 🎉 to enter!\n\n"
                       f"🕒 **Ends:** <t:{end_timestamp}:F>\n"
                       f"⏰ **Closes:** <t:{end_timestamp}:R>",
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"Started by {interaction.user.display_name}")
        embed.timestamp = now
        
        # Send the giveaway message with role ping
        giveaway_role_id = os.getenv("GIVEAWAY_ROLE_ID")
        content = f"<@&{giveaway_role_id}>" if giveaway_role_id else None
        
        await interaction.response.send_message(content=content, embed=embed)
        message = await interaction.original_response()
        
        # Add reaction
        await message.add_reaction("🎉")
        
        logger.info(f"[Giveaway] Started by {interaction.user} - Prize: {prize}, Duration: {time_minutes}m")
        
        # Wait for the duration
        await asyncio.sleep(time_minutes * 60)
        
        # Fetch the message again to get updated reactions
        try:
            message = await message.channel.fetch_message(message.id)
        except discord.NotFound:
            logger.warning(f"[Giveaway] Message {message.id} was deleted before giveaway ended")
            return
        
        # Get all users who reacted with 🎉 (excluding bots)
        reaction = discord.utils.get(message.reactions, emoji="🎉")
        
        if not reaction:
            logger.warning(f"[Giveaway] No reaction found on message {message.id}")
            end_embed = discord.Embed(
                title="🎉 GIVEAWAY ENDED 🎉",
                description=f"**Prize:** {prize}\n\n❌ No valid entries found.",
                color=discord.Color.red()
            )
            await message.edit(embed=end_embed)
            return
        
        # Get users who reacted (exclude bots and the bot itself)
        users = []
        async for user in reaction.users():
            if not user.bot:
                users.append(user)
        
        if not users:
            logger.info(f"[Giveaway] No valid entries for giveaway {message.id}")
            end_embed = discord.Embed(
                title="🎉 GIVEAWAY ENDED 🎉",
                description=f"**Prize:** {prize}\n\n❌ No valid entries found.",
                color=discord.Color.red()
            )
            end_embed.set_footer(text=f"Started by {interaction.user.display_name}")
            await message.edit(embed=end_embed)
            return
        
        # Pick a random winner
        winner = random.choice(users)
        
        logger.info(f"[Giveaway] Winner selected: {winner} (from {len(users)} entries)")
        
        # Update embed with winner
        end_embed = discord.Embed(
            title="🏆 GIVEAWAY ENDED 🏆",
            description=f"**Prize:** {prize}\n\n"
                       f"🎊 **Winner:** {winner.mention} 🎊\n\n"
                       f"**Started:** <t:{start_timestamp}:F>\n"
                       f"**Ended:** <t:{end_timestamp}:F>\n"
                       f"**Total Entries:** {len(users)} participants",
            color=discord.Color.green()
        )
        end_embed.set_footer(text=f"Hosted by {interaction.user.display_name} • Thanks for participating!")
        
        await message.edit(embed=end_embed)
        
        # Send winner announcement with claim instructions
        support_channel_id = os.getenv("SUPPORT_TICKET_CHANNEL_ID")
        congrats_embed = discord.Embed(
            title="🎊 GIVEAWAY WINNER 🎊",
            description=f"Congratulations {winner.mention}!\n\n"
                       f"You won **{prize}**!\n\n"
                       f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                       f"**🎁 How to Claim Your Prize:**\n"
                       f"Please create a ticket in <#{support_channel_id}> to claim your reward!\n\n"
                       f"*Make sure you mention your Roobet ID in your ticket.*",
            color=discord.Color.gold()
        )
        congrats_embed.set_thumbnail(url=winner.display_avatar.url)
        congrats_embed.set_footer(text="🎉 Thank you for participating in our giveaway!")
        
        await message.channel.send(content=winner.mention, embed=congrats_embed)

async def setup(bot):
    await bot.add_cog(Giveaway(bot))
