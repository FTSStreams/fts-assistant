import discord
from discord import app_commands
from discord.ext import commands
from utils import fetch_total_wager, fetch_weighted_wager, send_tip, get_current_month_range
from db import get_db_connection, release_db_connection, save_tip_log
import os
from datetime import datetime
import datetime as dt
import logging

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))

class User(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="mywager", description="Check your personal wager stats for the current month using your Roobet username")
    @app_commands.describe(username="Your Roobet username")
    async def mywager(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()
        start_date, end_date = get_current_month_range()
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

    @app_commands.command(name="monthlygoal", description="Display total wager and weighted wager for the current month")
    async def monthlygoal(self, interaction: discord.Interaction):
        await interaction.response.defer()
        start_date, end_date = get_current_month_range()
        try:
            total_wager_data = fetch_total_wager(start_date, end_date)
            weighted_wager_data = fetch_weighted_wager(start_date, end_date)
            total_wager = sum(
                entry.get("wagered", 0)
                for entry in total_wager_data
                if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
            )
            total_weighted_wager = sum(
                entry.get("weightedWagered", 0)
                for entry in weighted_wager_data
                if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
            )
            embed = discord.Embed(
                title="📈 Monthly Wager Stats",
                description=(
                    f"**TOTAL WAGER THIS MONTH**: ${total_wager:,.2f} USD\n"
                    f"**TOTAL WEIGHTED WAGER THIS MONTH**: ${total_weighted_wager:,.2f} USD"
                ),
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Error retrieving monthly stats: {str(e)}", ephemeral=True)
            logger.error(f"Error in /monthlygoal: {str(e)}")

    @app_commands.command(name="tipstats", description="Display tip statistics (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def tipstats(self, interaction: discord.Interaction):
        await interaction.response.defer()
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                now = datetime.now(dt.UTC)
                last_24h = now - dt.timedelta(hours=24)
                last_7d = now - dt.timedelta(days=7)
                last_30d = now - dt.timedelta(days=30)
                since_jan1 = datetime(2025, 1, 1, tzinfo=dt.UTC)
                cur.execute("""
                    SELECT 
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_24h,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_7d,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS last_30d,
                        COALESCE(SUM(CASE WHEN tipped_at >= %s THEN amount ELSE 0 END), 0) AS since_jan1
                    FROM manualtips;
                """, (last_24h, last_7d, last_30d, since_jan1))
                result = cur.fetchone()
                stats = {
                    "last_24h": float(result[0]),
                    "last_7d": float(result[1]),
                    "last_30d": float(result[2]),
                    # Updated hardcoded base to match your account's actual total
                    "since_jan1": float(result[3]) + 11295.53
                }
            embed = discord.Embed(
                title="📊 Tip Statistics",
                description=(
                    f"**Past 24 Hours**: ${stats['last_24h']:.2f} USD\n"
                    f"**Past 7 Days**: ${stats['last_7d']:.2f} USD\n"
                    f"**Past 30 Days**: ${stats['last_30d']:.2f} USD\n"
                    f"**Lifetime (Since Jan. 1st 2025)**: ${stats['since_jan1']:.2f} USD"
                ),
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Generated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Error retrieving tip stats: {str(e)}", ephemeral=True)
            logger.error(f"Error in /tipstats: {str(e)}")

    @app_commands.command(name="tipuser", description="Manually tip a Roobet user by username (admin only)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The Roobet username of the player",
        amount="The tip amount in USD (e.g., 5.00)"
    )
    async def tipuser(self, interaction: discord.Interaction, username: str, amount: float):
        if amount <= 0:
            await interaction.response.send_message("❌ Tip amount must be greater than 0.", ephemeral=True)
            logger.error(f"Invalid tip amount: {amount} by {interaction.user}")
            return
        await interaction.response.defer()
        # Search for UID in wager data (current year)
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
            logger.error(f"No UID found for username {username} in /tipuser by {interaction.user}")
            return
        logger.info(f"Attempting to send manual tip of ${amount} to {username} (UID: {roobet_uid})")
        response = await send_tip(
            user_id=os.getenv("ROOBET_USER_ID"),
            to_username=username,
            to_user_id=roobet_uid,
            amount=amount,
            show_in_chat=True,
            balance_type="crypto"
        )
        masked_username = username[:-3] + "***" if len(username) > 3 else "***"
        if response.get("success"):
            save_tip_log(roobet_uid, username, amount, "manual", month=datetime.now(dt.UTC).month, year=datetime.now(dt.UTC).year)
            embed = discord.Embed(
                title="🎉 Manual Tip Sent!",
                description=(
                    f"**{masked_username}** received a tip of **${amount:.2f} USD**!\n"
                    f"Sent by: **{interaction.user.display_name}**\n"
                    f"Keep shining! ✨"
                ),
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Tipped on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
            await interaction.followup.send(embed=embed)
            logger.info(f"Manual tip of ${amount} sent to {username} (UID: {roobet_uid})")
        else:
            error_message = response.get("message", "Unknown error")
            await interaction.followup.send(
                f"❌ Failed to send tip to {username}: {error_message}", ephemeral=True
            )
            logger.error(f"Failed to send tip to {username} (UID: {roobet_uid}): {error_message}")

    @app_commands.command(name="monthtomonth", description="Show a month-to-month wager line chart")
    async def monthtomonth(self, interaction: discord.Interaction):
        await interaction.response.defer()
        import matplotlib.pyplot as plt
        import io

        # Hardcoded values for now (WEIGHTED wager)
        months = [
            "January", "February", "March", "April", "May", "June"
        ]
        weighted_wagers = [
            121784.00, 312112.00, 283245.00, 108998.00, 151137.00
        ]
        total_wagers = [
            200000.00, 400000.00, 350000.00, 150000.00, 180000.00
        ]
        # Fetch current month WEIGHTED and TOTAL wager dynamically
        start_date, end_date = get_current_month_range()
        weighted_wager_data = fetch_weighted_wager(start_date, end_date)
        total_wager_data = fetch_total_wager(start_date, end_date)
        total_weighted_wager = sum(
            entry.get("weightedWagered", 0)
            for entry in weighted_wager_data
            if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
        )
        total_wager = sum(
            entry.get("wagered", 0)
            for entry in total_wager_data
            if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
        )
        weighted_wagers.append(total_weighted_wager)
        total_wagers.append(total_wager)

        plt.figure(figsize=(10, 5))
        plt.plot(months, weighted_wagers, marker='o', color='b', label='Weighted Wager')
        plt.plot(months, total_wagers, marker='o', color='r', label='Total Wager')
        plt.title('Month-to-Month Wager Totals')
        plt.xlabel('Month')
        plt.ylabel('Wager (USD)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close()

        file = discord.File(buf, filename="monthtomonth.png")
        embed = discord.Embed(title="📈 Month-to-Month Wager Totals", color=discord.Color.green())
        embed.set_image(url="attachment://monthtomonth.png")
        await interaction.followup.send(embed=embed, file=file)

    @app_commands.command(name="lifetimestats", description="Show total wager and weighted wager since Jan 1st, 2025")
    async def lifetimestats(self, interaction: discord.Interaction):
        await interaction.response.defer()
        from datetime import datetime
        import datetime as dt
        start_date = "2025-01-01T00:00:00"
        now = datetime.now(dt.UTC)
        end_date = now.strftime("%Y-%m-%dT%H:%M:%S")
        total_wager_data = fetch_total_wager(start_date, end_date)
        weighted_wager_data = fetch_weighted_wager(start_date, end_date)
        total_wager = sum(
            entry.get("wagered", 0)
            for entry in total_wager_data
            if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
        )
        total_weighted_wager = sum(
            entry.get("weightedWagered", 0)
            for entry in weighted_wager_data
            if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
        )
        embed = discord.Embed(
            title="🏆 Lifetime Wager Stats",
            description=(
                f"**TOTAL WAGER (Since Jan 1st, 2025):** ${total_wager:,.2f} USD\n"
                f"**TOTAL WEIGHTED WAGER (Since Jan 1st, 2025):** ${total_weighted_wager:,.2f} USD"
            ),
            color=discord.Color.purple()
        )
        embed.set_footer(text=f"Generated on {now.strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(User(bot))
