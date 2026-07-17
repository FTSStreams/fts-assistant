import discord
from discord import app_commands
from discord.ext import commands
import os
from datetime import datetime
import datetime as dt
import logging

from db import (
    get_gtb_game_state,
    set_gtb_game_state,
    add_gtb_guess,
    get_gtb_guesses,
    clear_gtb_game,
    add_funds_to_vault,
)

logger = logging.getLogger(__name__)

GUILD_ID = int(os.getenv("GUILD_ID", "1008041420738789536"))
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))
GTB_COMMAND_CHANNEL_ID = int(os.getenv("GTB_COMMAND_CHANNEL_ID", "1527380205759500369"))
GTB_WINNER_LOG_CHANNEL_ID = int(os.getenv("GTB_WINNER_LOG_CHANNEL_ID", "1527380252672659467"))
GTB_ROLE_CLAIM_CHANNEL_ID = int(os.getenv("GTB_ROLE_CLAIM_CHANNEL_ID", "1440843895360590028"))
GTB_FIRST_PRIZE = float(os.getenv("GTB_FIRST_PRIZE", "3.00"))
GTB_SECOND_PRIZE = float(os.getenv("GTB_SECOND_PRIZE", "2.00"))
GTB_THIRD_PRIZE = float(os.getenv("GTB_THIRD_PRIZE", "1.00"))
CHECKIN_BALANCE_LEADERBOARD_CHANNEL_ID = int(os.getenv("CHECKIN_BALANCE_LEADERBOARD_CHANNEL_ID", "1501283696928362497"))


class GuessTheBalance(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.game_message_id = None

    async def _get_text_channel(self, channel_id: int):
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception as e:
                logger.error(f"Failed to fetch channel {channel_id}: {e}")
                return None
        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            return None
        return channel

    def _build_gtb_game_embed(self, status: str, guesses_dict: dict = None):
        if guesses_dict is None:
            guesses_dict = {}

        if status == "open":
            status_text = "🟢 OPEN"
            title = "🎯 **Guess the Balance is now open!** 🎯"
        else:
            status_text = "🔴 CLOSED"
            title = "🎯 **Guess the Balance is now closed!** 🎯"

        now_utc = datetime.now(dt.UTC)
        timestamp = int(now_utc.timestamp())

        sorted_guesses = sorted(guesses_dict.values(), key=lambda x: x[1], reverse=True)
        participants_text = "\n".join([f"{username} - ${amount:,}" for username, amount in sorted_guesses])

        if not participants_text:
            participants_text = "No guesses yet"

        embed = discord.Embed(
            title=title,
            description=(
                f"**Started:** <t:{timestamp}:f>\n\n"
                f"**Game Status:** {status_text}\n\n"
                f"**📜 Rules & Disclosure:**\n"
                f"• Use /gtbguess [amount] to submit your prediction.\n"
                f"• 1 guess per player\n\n"
                f"**Prize Multipliers:**\n"
                f"📈 Within $10: 2x multiplier\n"
                f"📈 Within $25: 1.5x multiplier\n"
                f"📈 Within $50: 1.25x multiplier\n\n"
                f"**Prize Pool (before multipliers):**\n"
                f"🥇 1st Place: ${GTB_FIRST_PRIZE:,.2f}\n"
                f"🥈 2nd Place: ${GTB_SECOND_PRIZE:,.2f}\n"
                f"🥉 3rd Place: ${GTB_THIRD_PRIZE:,.2f}\n\n"
                f"**Participants (sorted highest to lowest):**\n"
                f"{participants_text}\n\n"
                f"📍 **Track Guess The Balance + Logs:** <#{GTB_WINNER_LOG_CHANNEL_ID}>\n"
                f"🎭 **Claim the Emotional Support Viewer role:** <#{GTB_ROLE_CLAIM_CHANNEL_ID}>"
            ),
            color=discord.Color.blue(),
        )
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        return embed

    def _get_multiplier(self, difference: int):
        if difference <= 10:
            return 2.0
        elif difference <= 25:
            return 1.5
        elif difference <= 50:
            return 1.25
        else:
            return 1.0

    def _calculate_prize(self, base_prize: float, multiplier: float):
        final = base_prize * multiplier
        return int(final * 100) / 100

    @app_commands.command(name="gtbopen", description="Open a new GTB game (owner only)")
    async def gtbopen(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("❌ Only the owner can open a GTB game.", ephemeral=True)
            return

        if interaction.channel_id != GTB_COMMAND_CHANNEL_ID:
            await interaction.response.send_message(
                f"❌ /gtbopen can only be used in <#{GTB_COMMAND_CHANNEL_ID}>.",
                ephemeral=True,
            )
            return

        await interaction.response.defer()

        set_gtb_game_state("open", {})
        embed = self._build_gtb_game_embed("open", {})
        message = await interaction.followup.send(embed=embed)
        self.game_message_id = message.id

        logger.info(f"[GTB] Game opened by {interaction.user}")

    @app_commands.command(name="gtbguess", description="Submit your guess")
    async def gtbguess(self, interaction: discord.Interaction, amount: int):
        if interaction.channel_id != GTB_COMMAND_CHANNEL_ID:
            await interaction.response.send_message(
                f"❌ /gtbguess can only be used in <#{GTB_COMMAND_CHANNEL_ID}>.",
                ephemeral=True,
            )
            return

        if amount <= 0:
            await interaction.response.send_message("❌ Please provide a valid positive integer amount.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        game_state = get_gtb_game_state()
        if game_state is None or game_state.get("status") != "open":
            await interaction.followup.send("❌ No active GTB game is currently open.", ephemeral=True)
            return

        username = interaction.user.display_name
        add_gtb_guess(interaction.user.id, username, amount)

        guesses = get_gtb_guesses()
        embed = self._build_gtb_game_embed("open", guesses)

        if self.game_message_id:
            try:
                channel = await self._get_text_channel(GTB_COMMAND_CHANNEL_ID)
                if channel:
                    message = await channel.fetch_message(self.game_message_id)
                    await message.edit(embed=embed)
            except Exception as e:
                logger.error(f"[GTB] Failed to update game message: {e}")

        await interaction.followup.send(f"✅ Your guess of **${amount:,}** has been recorded!", ephemeral=True)
        logger.info(f"[GTB] {username} (ID: {interaction.user.id}) guessed ${amount}")

    @app_commands.command(name="gtbclose", description="Close the game (owner only)")
    async def gtbclose(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("❌ Only the owner can close a GTB game.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        game_state = get_gtb_game_state()
        if game_state is None or game_state.get("status") != "open":
            await interaction.followup.send("❌ No active GTB game is currently open.", ephemeral=True)
            return

        guesses = get_gtb_guesses()
        set_gtb_game_state("closed", guesses)

        embed = self._build_gtb_game_embed("closed", guesses)

        if self.game_message_id:
            try:
                channel = await self._get_text_channel(GTB_COMMAND_CHANNEL_ID)
                if channel:
                    message = await channel.fetch_message(self.game_message_id)
                    await message.edit(embed=embed)
            except Exception as e:
                logger.error(f"[GTB] Failed to update game message on close: {e}")

        await interaction.followup.send("✅ GTB game closed.", ephemeral=True)
        logger.info("[GTB] Game closed")

    @app_commands.command(name="gtbresult", description="Post results and award prizes (owner only)")
    async def gtbresult(self, interaction: discord.Interaction, balance: int):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("❌ Only the owner can post GTB results.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        game_state = get_gtb_game_state()
        if game_state is None or game_state.get("status") != "closed":
            await interaction.followup.send("❌ No closed GTB game found. Please close the game first.", ephemeral=True)
            return

        guesses = get_gtb_guesses()
        if not guesses:
            await interaction.followup.send("❌ No guesses recorded. Cannot post results.", ephemeral=True)
            return

        guess_data = []
        for user_id, (username, guess_amount) in guesses.items():
            difference = abs(balance - guess_amount)
            multiplier = self._get_multiplier(difference)
            guess_data.append((user_id, username, guess_amount, difference, multiplier))

        guess_data.sort(key=lambda x: x[3])
        winners = guess_data[:3]

        results_lines = []
        medals = ["🥇", "🥈", "🥉"]
        base_prizes = [GTB_FIRST_PRIZE, GTB_SECOND_PRIZE, GTB_THIRD_PRIZE]
        winner_mention_ids = []

        for idx, (user_id, username, guess_amount, difference, multiplier) in enumerate(winners):
            base_prize = base_prizes[idx]
            final_prize = self._calculate_prize(base_prize, multiplier)
            medal = medals[idx]
            results_lines.append(
                f"{medal} **{idx+1}{['st', 'nd', 'rd'][idx]} Place:** {username} (Guessed ${guess_amount:,}) - "
                f"Difference: ${difference} - {multiplier}x Multiplier - Wins ${final_prize:,.2f}"
            )
            winner_mention_ids.append(user_id)
            add_funds_to_vault(user_id, final_prize)

        embed = discord.Embed(
            title="🎯 **Guess the Balance - Results!**",
            description=(
                f"**End Balance:** ${balance:,}\n\n"
                + "\n".join(results_lines) + "\n\n"
                f"**See FTS Vault Leaderboard:** <#{CHECKIN_BALANCE_LEADERBOARD_CHANNEL_ID}>\n\n"
                f"**Claim Your Prize:**\n"
                f"• /balance (view your FTS Vault stats)\n"
                f"• /withdraw (withdraw all or a chosen amount from your FTS Vault to your Roobet ID)\n\n"
                f"📍 **Track Guess The Balance + Logs:** <#{GTB_WINNER_LOG_CHANNEL_ID}>\n"
                f"🎭 **Claim the Emotional Support Viewer role:** <#{GTB_ROLE_CLAIM_CHANNEL_ID}>"
            ),
            color=discord.Color.gold(),
        )
        embed.set_thumbnail(url="https://play.mfam.gg/img/roobet_logo.png")
        embed.set_footer(text="AutoTip Engine Live • Payouts added to vault")

        log_channel = await self._get_text_channel(GTB_WINNER_LOG_CHANNEL_ID)
        if log_channel:
            mentions = " ".join([f"<@{uid}>" for uid in winner_mention_ids])
            winner_message = f"🎉 Congratulations {mentions}!\n"
            await log_channel.send(winner_message, embed=embed)

        clear_gtb_game()
        self.game_message_id = None

        await interaction.followup.send("✅ Results posted and prizes awarded to top 3 players.", ephemeral=True)
        logger.info(f"[GTB] Results posted. Final balance: ${balance}")


async def setup(bot):
    await bot.add_cog(GuessTheBalance(bot))
    logger.info("[GTB] Cog loaded successfully")
