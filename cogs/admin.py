import discord
from discord import app_commands
from discord.ext import commands, tasks
from db import (
    get_db_connection,
    release_db_connection,
    get_setting_value,
    save_setting_value,
    edit_checkin_balance,
    resolve_checkin_withdrawal_hold,
)
import logging
import os
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
ROLE_ASSIGNMENT_CHANNEL_ID = 1440843895360590028
ROLE_ASSIGNMENT_MESSAGE_KEY = "role_assignment_menu_message_id"

ROLE_MENU_OPTIONS = [
    ("X Notis", 1441147596491063377),
    ("Kick Notis", 1441148710024118332),
    ("Giveaway Merchants", 1441158750386917526),
    ("Multi Leaderboard Warriors", 1441159759389528264),
    ("Slot Challenge Warriors", 1441160392830222497),
    ("Big Wins", 1441161426671636661),
    ("Roo Vs Flip Degens", 1501438806895759482),
]


class RoleAssignmentSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=name, value=str(role_id))
            for name, role_id in ROLE_MENU_OPTIONS
        ]
        super().__init__(
            placeholder="Select a role to add or remove",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="role_assignment_select_v1",
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, RoleAssignmentView):
            await interaction.response.send_message("❌ Role menu is not configured correctly.", ephemeral=True)
            return
        await view.handle_selection(interaction, self.values[0])


class RoleAssignmentView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(RoleAssignmentSelect())
        self._last_action_at = {}

    async def handle_selection(self, interaction: discord.Interaction, selected_role_id: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("❌ This menu can only be used inside the server.", ephemeral=True)
            return

        now = datetime.now(dt.UTC)
        last_action_at = self._last_action_at.get(interaction.user.id)
        if last_action_at is not None and (now - last_action_at).total_seconds() < 1:
            await interaction.response.send_message("⏳ Slow down a bit and try again in a couple seconds.", ephemeral=True)
            return
        self._last_action_at[interaction.user.id] = now

        try:
            role_id = int(selected_role_id)
        except ValueError:
            await interaction.response.send_message("❌ Invalid role selection.", ephemeral=True)
            return

        role = interaction.guild.get_role(role_id)
        if role is None:
            await interaction.response.send_message("❌ That role no longer exists.", ephemeral=True)
            return

        member = interaction.user
        has_role = role in member.roles

        try:
            if has_role:
                await member.remove_roles(role, reason="Self-role menu removal")
                await interaction.response.send_message(f"✅ Removed role: **{role.name}**", ephemeral=True)
            else:
                await member.add_roles(role, reason="Self-role menu assignment")
                await interaction.response.send_message(f"✅ Added role: **{role.name}**", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ I don't have permission to manage that role. Check role hierarchy and permissions.",
                ephemeral=True,
            )
        except Exception as e:
            logger.error(f"Failed to toggle role {role_id} for user {member.id}: {e}")
            await interaction.response.send_message("❌ Failed to update your role. Please try again.", ephemeral=True)


class Admin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.role_assignment_view = RoleAssignmentView()
        self.bot.add_view(self.role_assignment_view)
        self.ensure_role_assignment_panel.start()

    async def _build_role_assignment_embed(self):
        lines = [
            "Use the dropdown below to toggle your roles.",
            "Selecting a role adds it if you do not have it, or removes it if you already have it.",
            "",
            "Available roles:",
        ]
        for name, role_id in ROLE_MENU_OPTIONS:
            lines.append(f"• {name}: <@&{role_id}>")

        embed = discord.Embed(
            title="Role Assignment",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text="Role menu is persistent and managed automatically")
        return embed

    async def _post_role_assignment_panel(self, channel: discord.TextChannel):
        embed = await self._build_role_assignment_embed()
        message = await channel.send(embed=embed, view=self.role_assignment_view)
        save_setting_value(ROLE_ASSIGNMENT_MESSAGE_KEY, str(message.id))
        logger.info(f"Posted role assignment panel in channel {channel.id} as message {message.id}")

    async def _channel_has_any_messages(self, channel: discord.TextChannel) -> bool:
        async for _ in channel.history(limit=1):
            return True
        return False

    async def _ensure_role_assignment_panel(self):
        channel = self.bot.get_channel(ROLE_ASSIGNMENT_CHANNEL_ID)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(ROLE_ASSIGNMENT_CHANNEL_ID)
            except Exception as e:
                logger.error(f"Failed to fetch role assignment channel {ROLE_ASSIGNMENT_CHANNEL_ID}: {e}")
                return

        if not isinstance(channel, discord.TextChannel):
            logger.error(f"Configured role assignment channel {ROLE_ASSIGNMENT_CHANNEL_ID} is not a text channel")
            return

        saved_message_id = get_setting_value(ROLE_ASSIGNMENT_MESSAGE_KEY, default=None)
        if saved_message_id:
            try:
                await channel.fetch_message(int(saved_message_id))
                return
            except discord.NotFound:
                logger.info("Tracked role assignment panel was deleted. Will repost only if channel is empty.")
            except Exception as e:
                logger.error(f"Failed to fetch tracked role assignment panel: {e}")
                return

        if await self._channel_has_any_messages(channel):
            logger.info("Role assignment panel missing but channel is not empty. Skipping repost.")
            return

        try:
            await self._post_role_assignment_panel(channel)
        except Exception as e:
            logger.error(f"Failed to post role assignment panel: {e}")

    @tasks.loop(minutes=1)
    async def ensure_role_assignment_panel(self):
        await self._ensure_role_assignment_panel()

    @ensure_role_assignment_panel.before_loop
    async def before_ensure_role_assignment_panel(self):
        await self.bot.wait_until_ready()

    def cog_unload(self):
        self.ensure_role_assignment_panel.cancel()

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

    @app_commands.command(name="ensurerolepanel", description="Ensure the role assignment panel exists (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def ensure_role_panel_cmd(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await self._ensure_role_assignment_panel()
        await interaction.followup.send("✅ Role panel check completed.", ephemeral=True)

    @app_commands.command(name="editcheckinbalance", description="Add or remove check-in balance for a user (admin only)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        user="Discord user to edit",
        action="Choose whether to add or remove balance",
        amount="Amount to change (must be greater than 0)",
    )
    @app_commands.choices(
        action=[
            app_commands.Choice(name="add", value="add"),
            app_commands.Choice(name="remove", value="remove"),
        ]
    )
    async def edit_checkin_balance_cmd(
        self,
        interaction: discord.Interaction,
        user: discord.User,
        action: app_commands.Choice[str],
        amount: float,
    ):
        await interaction.response.defer(ephemeral=True)

        if amount <= 0:
            await interaction.followup.send("❌ Amount must be greater than 0.", ephemeral=True)
            return

        delta = amount if action.value == "add" else -amount
        result = edit_checkin_balance(user.id, delta)
        if result is None:
            await interaction.followup.send("❌ Failed to edit check-in balance.", ephemeral=True)
            return

        if not result.get("ok", False):
            if result.get("reason") == "insufficient_balance":
                current_balance = float(result.get("balance", 0.0))
                await interaction.followup.send(
                    f"❌ Cannot remove **${amount:,.2f}** from {user.mention}. "
                    f"Current balance is **${current_balance:,.2f}**.",
                    ephemeral=True,
                )
                return

            await interaction.followup.send("❌ Failed to edit check-in balance.", ephemeral=True)
            return

        delta_value = float(result["amount_delta"])
        action_word = "Added" if delta_value >= 0 else "Removed"
        embed = discord.Embed(
            title="✅ Check-In Balance Updated",
            description=f"{action_word} **${abs(delta_value):,.2f}** {'to' if delta_value >= 0 else 'from'} {user.mention}.",
            color=discord.Color.green(),
        )
        embed.add_field(name="💰 New Balance", value=f"**${result['balance']:,.2f}**", inline=True)
        embed.add_field(name="🔥 Current Streak", value=f"**{int(result['streak_days'])} days**", inline=True)
        embed.set_footer(text=f"Updated on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed, ephemeral=True)
        logger.info(
            f"[check_in] Admin {interaction.user} edited check-in balance by ${delta_value:.2f} "
            f"for discord_user_id={user.id}. New balance=${result['balance']:.2f}"
        )

    @app_commands.command(name="resolvecheckinhold", description="Resolve a stuck check-in withdrawal hold (admin only)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(
        user="Discord user whose hold should be resolved",
        action="release returns held funds; commit marks held funds as withdrawn",
        note="Optional note for audit trail",
    )
    @app_commands.choices(
        action=[
            app_commands.Choice(name="release", value="release"),
            app_commands.Choice(name="commit", value="commit"),
        ]
    )
    async def resolve_checkin_hold_cmd(
        self,
        interaction: discord.Interaction,
        user: discord.User,
        action: app_commands.Choice[str],
        note: str = None,
    ):
        await interaction.response.defer(ephemeral=True)

        note_value = note.strip() if isinstance(note, str) else None
        result = resolve_checkin_withdrawal_hold(user.id, action.value, note=note_value)
        if result is None:
            await interaction.followup.send("❌ Failed to resolve check-in hold.", ephemeral=True)
            return

        if not result.get("ok", False):
            reason = result.get("reason", "unknown")
            if reason == "no_hold":
                await interaction.followup.send(
                    f"ℹ️ {user.mention} has no active hold.",
                    ephemeral=True,
                )
                return
            await interaction.followup.send("❌ Failed to resolve check-in hold.", ephemeral=True)
            return

        resolved_amount = float(result.get("released_or_committed", 0.0))
        action_word = "Released" if action.value == "release" else "Committed"
        embed = discord.Embed(
            title="✅ Check-In Hold Resolved",
            description=f"{action_word} **${resolved_amount:,.2f}** for {user.mention}.",
            color=discord.Color.green(),
        )
        embed.add_field(name="💰 New Balance", value=f"**${float(result.get('balance', 0.0)):,.2f}**", inline=True)
        embed.add_field(name="💸 Total Withdrawn", value=f"**${float(result.get('total_withdrawn', 0.0)):,.2f}**", inline=True)
        embed.set_footer(text=f"Resolved on {datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S')} GMT")
        await interaction.followup.send(embed=embed, ephemeral=True)
        logger.info(
            f"[check_in] Admin {interaction.user} resolved hold for discord_user_id={user.id} "
            f"action={action.value} amount={resolved_amount:.2f}"
        )


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

    @app_commands.command(name="seedhistoricmonthlylogs", description="One-time post: Jan 2025 to Mar 2026 monthly winner logs (admin only)")
    @app_commands.default_permissions(administrator=True)
    async def seed_historic_monthly_logs(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        completion_key = "historical_monthly_logs_seeded_2025_01_to_2026_03"
        already_seeded = get_setting_value(completion_key, default="false")
        if str(already_seeded).lower() == "true":
            await interaction.followup.send(
                "ℹ️ Historical monthly logs seed already completed once. No action taken.",
                ephemeral=True,
            )
            return

        leaderboard = self.bot.get_cog('Leaderboard')
        if not leaderboard:
            await interaction.followup.send("❌ Leaderboard cog is not available.", ephemeral=True)
            return

        start_year, start_month = 2025, 1
        end_year, end_month = 2026, 3

        posted = 0
        failed = []

        y = start_year
        m = start_month
        while (y, m) <= (end_year, end_month):
            try:
                success = await leaderboard.post_monthly_winner_logs_for_month(y, m, force=True)
                if success:
                    posted += 1
            except Exception as e:
                logger.error(f"Failed posting seeded monthly log for {y}-{m:02d}: {e}")
                failed.append(f"{y}-{m:02d}")

            m += 1
            if m > 12:
                m = 1
                y += 1

            await asyncio.sleep(1)

        if not failed:
            save_setting_value(completion_key, "true")

        summary = (
            f"✅ Historical monthly logs seed completed.\n"
            f"Posted embeds: {posted}\n"
            f"Range: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}"
        )
        if failed:
            summary += f"\n⚠️ Failed months: {', '.join(failed[:10])}"
        else:
            summary += "\n🔒 One-time seed lock has been set."

        await interaction.followup.send(summary, ephemeral=True)

    @app_commands.command(name="populatejson", description="Build pastleaderboards.json from API history (admin only, one-time use)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.describe(start_year="Year to start from (default 2025)", start_month="Month to start from (default 1)")
    async def populatejson(self, interaction: discord.Interaction, start_year: int = 2025, start_month: int = 1):
        await interaction.response.defer(ephemeral=True)

        from utils import fetch_weighted_wager, get_month_range
        import json, io

        PRIZES = [500, 300, 225, 175, 125, 75, 40, 30, 25, 5]

        now = datetime.now(dt.UTC)
        # Generate months from start up to (but not including) the current in-progress month
        months = []
        y, m = start_year, start_month
        while (y < now.year) or (y == now.year and m < now.month):
            months.append((y, m))
            m += 1
            if m > 12:
                m = 1
                y += 1

        result = {}
        failed = []

        await interaction.followup.send(
            f"⏳ Fetching leaderboard data for **{len(months)} months** ({start_year}-{start_month:02d} → {now.year}-{now.month:02d})... This may take a minute.",
            ephemeral=True
        )

        for year, month in months:
            key = f"{year}-{month:02d}"
            try:
                start_date, end_date = get_month_range(year, month)
                data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)

                sorted_data = sorted(
                    [e for e in data if isinstance(e.get("weightedWagered"), (int, float)) and e.get("weightedWagered", 0) > 0],
                    key=lambda e: e.get("weightedWagered", 0),
                    reverse=True
                )

                month_result = {}
                for rank, entry in enumerate(sorted_data[:10], start=1):
                    uid = str(entry.get("uid", ""))
                    if not uid:
                        continue
                    month_result[uid] = {
                        "username": entry.get("username", ""),
                        "rank": rank,
                        "prize": PRIZES[rank - 1],
                        "weighted_wagered": round(float(entry.get("weightedWagered", 0)), 2)
                    }

                result[key] = month_result
                logger.info(f"[populatejson] {key}: {len(month_result)} top-10 entries")

            except Exception as e:
                logger.error(f"[populatejson] Failed for {key}: {e}")
                failed.append(key)

        json_bytes = json.dumps(result, indent=2).encode("utf-8")
        file = discord.File(io.BytesIO(json_bytes), filename="pastleaderboards.json")

        summary = f"✅ Done! Built data for **{len(result)}/{len(months)} months**."
        if failed:
            summary += f"\n⚠️ Failed: {', '.join(failed)}"
        summary += "\nUpload `pastleaderboards.json` to your GitHub data repo and let me know when it's there."

        await interaction.followup.send(summary, file=file, ephemeral=True)


async def setup(bot):
    await bot.add_cog(Admin(bot))
