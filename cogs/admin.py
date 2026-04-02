import discord
from discord import app_commands
from discord.ext import commands
from db import get_db_connection, release_db_connection
import logging
import os
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))

class Admin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="clear_tips", description="Clear all milestone tips from the database (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def clear_tips(self, interaction: discord.Interaction):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE milestonetips; TRUNCATE pending_tips;")
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

    @app_commands.command(name="status", description="Check bot status (admin only)")
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

    @app_commands.command(name="backfillmonths", description="Backfill missing monthly totals (admin only, one-time use)")
    @app_commands.default_permissions(administrator=True)
    async def backfill_months(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        data_manager = self.bot.get_cog('DataManager')
        if not data_manager:
            await interaction.followup.send("❌ DataManager is not available.", ephemeral=True)
            return

        try:
            await data_manager.backfill_historical_data()

            # Trigger immediate monthly winner logs post (if configured) so admins can run once and verify.
            leaderboard_cog = self.bot.get_cog('Leaderboard')
            if leaderboard_cog:
                await leaderboard_cog.maybe_post_monthly_winner_logs()

            await interaction.followup.send(
                "✅ Monthly backfill finished. Missing months from Jan 2025 to current month were backfilled. If WAGER_LEADERBOARD_LOGS_CHANNEL_ID is configured, monthly winners were posted now.",
                ephemeral=True,
            )
        except Exception as e:
            logger.error(f"Failed to backfill monthly totals: {e}")
            await interaction.followup.send(f"❌ Backfill failed: {e}", ephemeral=True)

    @app_commands.command(name="backfillmonthlylogs", description="Post historical monthly winner log embeds (admin only)")
    @app_commands.describe(
        start_year="Start year (e.g. 2025)",
        start_month="Start month 1-12",
        end_year="End year (defaults to previous month)",
        end_month="End month 1-12 (defaults to previous month)",
    )
    @app_commands.default_permissions(administrator=True)
    async def backfill_monthly_logs(
        self,
        interaction: discord.Interaction,
        start_year: int = 2025,
        start_month: int = 1,
        end_year: int = None,
        end_month: int = None,
    ):
        await interaction.response.defer(ephemeral=True)

        leaderboard = self.bot.get_cog('Leaderboard')
        if not leaderboard:
            await interaction.followup.send("❌ Leaderboard cog is not available.", ephemeral=True)
            return

        if not (1 <= start_month <= 12):
            await interaction.followup.send("❌ start_month must be between 1 and 12.", ephemeral=True)
            return

        now = datetime.now(dt.UTC)
        prev_month_anchor = now.replace(day=1) - dt.timedelta(days=1)
        resolved_end_year = end_year if end_year is not None else prev_month_anchor.year
        resolved_end_month = end_month if end_month is not None else prev_month_anchor.month

        if not (1 <= resolved_end_month <= 12):
            await interaction.followup.send("❌ end_month must be between 1 and 12.", ephemeral=True)
            return

        if (start_year, start_month) > (resolved_end_year, resolved_end_month):
            await interaction.followup.send("❌ Start month must be before or equal to end month.", ephemeral=True)
            return

        posted = 0
        failed = []

        y = start_year
        m = start_month
        while (y, m) <= (resolved_end_year, resolved_end_month):
            try:
                success = await leaderboard.post_monthly_winner_logs_for_month(y, m, force=True)
                if success:
                    posted += 1
            except Exception as e:
                logger.error(f"Failed posting monthly log for {y}-{m:02d}: {e}")
                failed.append(f"{y}-{m:02d}")

            m += 1
            if m > 12:
                m = 1
                y += 1

            # Keep a little spacing between messages.
            await asyncio.sleep(1)

        summary = (
            f"✅ Monthly logs backfill completed.\n"
            f"Posted embeds: {posted}\n"
            f"Range: {start_year}-{start_month:02d} to {resolved_end_year}-{resolved_end_month:02d}"
        )
        if failed:
            summary += f"\n⚠️ Failed months: {', '.join(failed[:10])}"

        await interaction.followup.send(summary, ephemeral=True)

async def setup(bot):
    await bot.add_cog(Admin(bot))
