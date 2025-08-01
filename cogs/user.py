import discord
from discord import app_commands
from discord.ext import commands
from utils import send_tip, get_current_month_range
from db import get_db_connection, release_db_connection, save_tip_log, get_monthly_totals
import os
from datetime import datetime
import datetime as dt
import logging
import asyncio
import re

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))

class User(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
    
    def get_data_manager(self):
        """Helper to get DataManager cog"""
        return self.bot.get_cog('DataManager')

    @app_commands.command(name="mywager", description="Check your personal wager stats for the current month using your Roobet username")
    @app_commands.describe(username="Your Roobet username")
    async def mywager(self, interaction: discord.Interaction, username: str):
        await interaction.response.defer()
        
        # Input validation - only allow alphanumeric characters and underscores
        if not re.match(r'^[a-zA-Z0-9_]+$', username):
            await interaction.followup.send("❌ Username can only contain letters, numbers, and underscores.", ephemeral=True)
            return
        
        if len(username) > 50:  # Reasonable length limit
            await interaction.followup.send("❌ Username is too long (max 50 characters).", ephemeral=True)
            return
        
        # Get data from DataManager
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
            
        weighted_wager_data = cached_data.get('weighted_wager', [])
        total_wager_data = cached_data.get('total_wager', [])
        
        username_lower = username.lower()
        roobet_uid = None
        
        # Find user in weighted wager data
        for entry in weighted_wager_data:
            entry_username = entry.get("username", "").lower()
            if username_lower == entry_username:
                roobet_uid = entry.get("uid")
                username = entry.get("username")
                break
                
        if not roobet_uid:
            await interaction.followup.send(f"❌ No user found with username '{username}' who wagered this month.", ephemeral=True)
            return
            
        # Find user's total wager
        total_wager = 0
        for entry in total_wager_data:
            if entry.get("uid") == roobet_uid:
                total_wager = entry.get("wagered", 0) if isinstance(entry.get("wagered"), (int, float)) else 0
                break
                
        # Find user's weighted wager
        weighted_wager = 0
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
        
        # Get data from DataManager
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
            
        try:
            total_wager_data = cached_data.get('total_wager', [])
            weighted_wager_data = cached_data.get('weighted_wager', [])
            
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
        
        # Get data from DataManager to search for user
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        # Search for user in YEARLY data (since Jan 1st) instead of just current month
        from utils import fetch_weighted_wager
        from datetime import datetime
        import datetime as dt
        
        try:
            # Get yearly data from Jan 1st to now
            current_year = datetime.now(dt.UTC).year
            start_date = f"{current_year}-01-01T00:00:00Z"
            end_date = datetime.now(dt.UTC).isoformat()
            
            logger.info(f"[TipUser] Searching for {username} in yearly data from {start_date} to {end_date}")
            yearly_wager_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
            
            username_lower = username.lower()
            roobet_uid = None
            
            # Search in yearly data first
            for entry in yearly_wager_data:
                entry_username = entry.get("username", "").lower()
                if username_lower == entry_username:
                    roobet_uid = entry.get("uid")
                    username = entry.get("username")
                    logger.info(f"[TipUser] Found {username} (UID: {roobet_uid}) in yearly data")
                    break
                    
            # If not found in yearly data, try current month as fallback
            if not roobet_uid:
                cached_data = data_manager.get_cached_data()
                if cached_data:
                    weighted_wager_data = cached_data.get('weighted_wager', [])
                    for entry in weighted_wager_data:
                        entry_username = entry.get("username", "").lower()
                        if username_lower == entry_username:
                            roobet_uid = entry.get("uid")
                            username = entry.get("username")
                            logger.info(f"[TipUser] Found {username} (UID: {roobet_uid}) in current month data")
                            break
                            
        except Exception as e:
            logger.error(f"Error fetching yearly data for tipuser: {e}")
            # Fallback to current month data only
            cached_data = data_manager.get_cached_data()
            if not cached_data:
                await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
                return
                
            weighted_wager_data = cached_data.get('weighted_wager', [])
            username_lower = username.lower()
            roobet_uid = None
            
            for entry in weighted_wager_data:
                entry_username = entry.get("username", "").lower()
                if username_lower == entry_username:
                    roobet_uid = entry.get("uid")
                    username = entry.get("username")
                    break
                
        if not roobet_uid:
            await interaction.followup.send(f"❌ No user found with username '{username}' in {datetime.now(dt.UTC).year} wager data.", ephemeral=True)
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
        # Censor username and escape asterisks to prevent Discord markdown issues
        masked_username = username[:-3] + "\\*\\*\\*" if len(username) > 3 else "\\*\\*\\*"
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
        from datetime import datetime
        import calendar

        # Get historical monthly data from database
        monthly_data = get_monthly_totals()
        
        # Force fresh data fetch for current month
        current_total = 0
        current_weighted = 0
        
        try:
            # Always fetch fresh data for current month - no caching
            from utils import get_current_month_range, fetch_total_wager, fetch_weighted_wager
            
            logger.info("[monthtomonth] Fetching fresh current month data...")
            
            # Get current month date range
            start_date, end_date = get_current_month_range()
            
            # Fetch fresh data for current month
            fresh_total_data = fetch_total_wager(start_date, end_date)
            fresh_weighted_data = fetch_weighted_wager(start_date, end_date)
            
            # Calculate totals from fresh data
            current_total = sum(
                entry.get("wagered", 0)
                for entry in fresh_total_data
                if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
            )
            current_weighted = sum(
                entry.get("weightedWagered", 0)
                for entry in fresh_weighted_data
                if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
            )
            
            logger.info(f"[monthtomonth] Fresh data: Total=${current_total:,.2f}, Weighted=${current_weighted:,.2f}")
            
        except Exception as e:
            logger.error(f"[monthtomonth] Failed to fetch fresh data: {e}")
            
            # Fallback to cached data if fresh fetch fails
            data_manager = self.get_data_manager()
            if data_manager:
                cached_data = data_manager.get_cached_data()
                if cached_data:
                    weighted_wager_data = cached_data.get('weighted_wager', [])
                    total_wager_data = cached_data.get('total_wager', [])
                    
                    current_weighted = sum(
                        entry.get("weightedWagered", 0)
                        for entry in weighted_wager_data
                        if isinstance(entry.get("weightedWagered"), (int, float)) and entry.get("weightedWagered") >= 0
                    )
                    current_total = sum(
                        entry.get("wagered", 0)
                        for entry in total_wager_data
                        if isinstance(entry.get("wagered"), (int, float)) and entry.get("wagered") >= 0
                    )
                    
                    logger.info(f"[monthtomonth] Fallback cached data: Total=${current_total:,.2f}, Weighted=${current_weighted:,.2f}")

        # Add current month to the data if not already present
        now = datetime.now()
        current_month_key = f"{now.year}_{now.month:02d}"
        
        # Check if current month is already in the data
        current_month_exists = any(
            data['year'] == now.year and data['month'] == now.month 
            for data in monthly_data
        )
        
        if not current_month_exists:
            monthly_data.append({
                'year': now.year,
                'month': now.month,
                'total_wager': current_total,
                'weighted_wager': current_weighted
            })
        
        # Ensure we have at least some data to display
        if not monthly_data:
            embed = discord.Embed(
                title="📈 Month-to-Month Wager Totals", 
                description="No monthly data available yet. Please try again later.",
                color=discord.Color.orange()
            )
            await interaction.followup.send(embed=embed)
            return
        
        # Prepare data for plotting (limit to last 12 months)
        monthly_data = monthly_data[-12:]  # Show last 12 months max
        
        months = []
        weighted_wagers = []
        total_wagers = []
        
        for data in monthly_data:
            month_name = calendar.month_name[data['month']]
            year_suffix = f" {data['year']}" if data['year'] != now.year else ""
            months.append(f"{month_name[:3]}{year_suffix}")
            weighted_wagers.append(data['weighted_wager'])
            total_wagers.append(data['total_wager'])

        # Create the plot
        plt.figure(figsize=(12, 6))
        plt.plot(months, weighted_wagers, marker='o', color='b', label='Weighted Wager', linewidth=2, markersize=6)
        plt.plot(months, total_wagers, marker='s', color='r', label='Total Wager', linewidth=2, markersize=6)
        plt.title('Month-to-Month Wager Totals', fontsize=16, fontweight='bold')
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Wager (USD)', fontsize=12)
        plt.legend(fontsize=11)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        
        # Format y-axis to show values in thousands/millions
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        plt.tight_layout()

        # Save to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        plt.close()

        # Create embed with additional info
        file = discord.File(buf, filename="monthtomonth.png")
        embed = discord.Embed(title="📈 Month-to-Month Wager Totals", color=discord.Color.green())
        
        if monthly_data:
            latest = monthly_data[-1]
            embed.add_field(
                name="Current Month", 
                value=f"**Total:** ${latest['total_wager']:,.2f}\n**Weighted:** ${latest['weighted_wager']:,.2f}", 
                inline=True
            )
            
            if len(monthly_data) > 1:
                previous = monthly_data[-2]
                total_change = latest['total_wager'] - previous['total_wager']
                weighted_change = latest['weighted_wager'] - previous['weighted_wager']
                
                total_emoji = "📈" if total_change >= 0 else "📉"
                weighted_emoji = "📈" if weighted_change >= 0 else "📉"
                
                embed.add_field(
                    name="Month-over-Month Change", 
                    value=f"**Total:** {total_emoji} ${total_change:+,.2f}\n**Weighted:** {weighted_emoji} ${weighted_change:+,.2f}", 
                    inline=True
                )
        
        embed.set_image(url="attachment://monthtomonth.png")
        embed.set_footer(text=f"Showing last {len(monthly_data)} months • Data auto-updates monthly")
        
        await interaction.followup.send(embed=embed, file=file)

    @app_commands.command(name="lifetimestats", description="Show total wager and weighted wager for the current month")
    async def lifetimestats(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        # Get data from DataManager (current month data)
        data_manager = self.get_data_manager()
        if not data_manager:
            await interaction.followup.send("❌ Data service unavailable. Please try again later.", ephemeral=True)
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            await interaction.followup.send("❌ No data available. Please try again later.", ephemeral=True)
            return
        
        total_wager_data = cached_data.get('total_wager', [])
        weighted_wager_data = cached_data.get('weighted_wager', [])
        
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
        
        now = datetime.now(dt.UTC)
        
        embed = discord.Embed(
            title="🏆 Current Month Wager Stats",
            description=(
                f"**TOTAL WAGER (This Month):** ${total_wager:,.2f} USD\n"
                f"**TOTAL WEIGHTED WAGER (This Month):** ${total_weighted_wager:,.2f} USD"
            ),
            color=discord.Color.purple()
        )
        embed.set_footer(text=f"Generated on {now.strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(User(bot))
