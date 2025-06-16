import discord
from discord import app_commands
from discord.ext import commands
from db import get_db_connection, release_db_connection
import logging
import os

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))

class Admin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="clear_tips", description="Clear all milestone tips from the database (admin only)", guild=discord.Object(id=GUILD_ID))
    @app_commands.default_permissions(administrator=True)
    async def clear_tips(self, interaction: discord.Interaction):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE tips; TRUNCATE pending_tips;")
                conn.commit()
            global SENT_TIPS
            SENT_TIPS = set()
            logger.info("Cleared all milestone tips and pending tips from database and in-memory set.")
            await interaction.response.send_message("✅ All milestone tips have been cleared from the database.", ephemeral=True)
        except Exception as e:
            logger.error(f"Failed to clear milestone tips: {e}")
            await interaction.response.send_message(f"❌ Error clearing milestone tips: {e}", ephemeral=True)
        finally:
            release_db_connection(conn)

    @app_commands.command(name="status", description="Check bot status (admin only)", guild=discord.Object(id=GUILD_ID))
    @app_commands.default_permissions(administrator=True)
    async def status(self, interaction: discord.Interaction):
        db_status = "Connected"
        try:
            conn = get_db_connection()
            release_db_connection(conn)
        except Exception:
            db_status = "Disconnected"
        await interaction.response.send_message(
            f"Bot Status:\n- Database: {db_status}", ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(Admin(bot))
