import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import send_tip, get_current_month_range, fetch_user_game_stats
from db import (
    get_all_active_slot_challenges, add_active_slot_challenge, remove_active_slot_challenge, update_challenge_message_id, log_slot_challenge
)
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
CHALLENGE_CHANNEL_ID = int(os.getenv("CHALLENGE_CHANNEL_ID"))
LOGS_CHANNEL_ID = 1386537169170071572  # Winner/cancel log channel
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))

class SlotChallenge(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_challenge.start()
        self.ensure_challenge_embed.start()
        self.payout_queue = asyncio.Queue()
        self.process_payout_queue_task = asyncio.create_task(self.process_payout_queue())
        self.update_multi_challenge_history.start()
    
    def get_data_manager(self):
        """Helper to get DataManager cog"""
        return self.bot.get_cog('DataManager')

    async def process_payout_queue(self):
        while True:
            challenge, winner, second, logs_channel = await self.payout_queue.get()
            tip_response = await send_tip(
                user_id=os.getenv("ROOBET_USER_ID"),
                to_username=winner["username"],
                to_user_id=winner["uid"],
                amount=challenge["prize"]
            )
            if tip_response.get("success"):
                embed = discord.Embed(
                    title="🏆 Slot Challenge Results! 🏆",
                    description=f"**1st Place:** {winner['username']}\nMultiplier: x{winner['multiplier']:.2f}",
                    color=discord.Color.green()
                )
                if second:
                    embed.description += f"\n\n**2nd Place:** {second['username']}\nMultiplier: x{second['multiplier']:.2f}"
                embed.add_field(name="Bet Size", value=f"${winner.get('bet', 0):.2f}", inline=True)
                embed.add_field(name="Payout", value=f"${winner.get('payout', 0):.2f}", inline=True)
                embed.add_field(name="Required Multiplier", value=f"x{challenge['required_multi']}", inline=True)
                embed.add_field(name="Prize", value=f"${challenge['prize']}", inline=True)
                embed.add_field(name="Game", value=challenge['game_name'], inline=True)
                if challenge.get('min_bet'):
                    embed.add_field(name="Minimum Bet", value=f"${challenge['min_bet']}", inline=True)
                # Use plain text for the start time in the footer
                try:
                    dt_obj = datetime.fromisoformat(str(challenge['start_time']))
                    start_time_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
                except Exception:
                    start_time_str = str(challenge['start_time'])
                embed.set_footer(text=f"Challenge start: {start_time_str}")
                if logs_channel:
                    await logs_channel.send(embed=embed)
                logger.info(f"Calling log_slot_challenge for COMPLETED: id={challenge['challenge_id']} game={challenge['game_name']} winner={winner['username']}")
                # Use the actual completion time for logging
                from datetime import datetime, timezone
                completion_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                log_slot_challenge(
                    challenge["challenge_id"],
                    challenge["game_name"],
                    challenge["game_identifier"],
                    winner["uid"],
                    winner["username"],
                    winner["multiplier"],
                    winner.get("bet", 0),
                    winner.get("payout", 0),
                    challenge["required_multi"],
                    challenge["prize"],
                    challenge.get("min_bet", 0),
                    completion_time
                )
            else:
                if logs_channel:
                    await logs_channel.send(f"❌ Failed to tip prize to {winner['username']}. Please check logs.")
            await asyncio.sleep(30)
            self.payout_queue.task_done()

    @app_commands.command(name="setchallenge", description="Set a slot challenge for a specific game and multiplier.")
    @app_commands.describe(game_identifier="Game identifier (e.g. pragmatic:vs10bbbbrnd)", game_name="Game name for display", required_multi="Required multiplier (e.g. 100)", prize="Prize amount in USD", emoji="Optional emoji for this challenge", min_bet="Minimum bet size in USD (optional)")
    async def set_challenge(self, interaction: discord.Interaction, game_identifier: str, game_name: str, required_multi: float, prize: float, emoji: str = None, min_bet: float = None):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to set a challenge.", ephemeral=True)
            return
        active = get_all_active_slot_challenges()
        if len(active) >= 10:
            await interaction.response.send_message("There are already 10 active slot challenges. Please cancel one before adding another.", ephemeral=True)
            return
        challenge_start_utc = datetime.now(dt.UTC).replace(microsecond=0).isoformat()
        # Remove any quotes from game_name before saving
        clean_game_name = game_name.replace('"', '').replace("'", "")
        challenge_id = add_active_slot_challenge(
            game_identifier, clean_game_name, required_multi, prize, challenge_start_utc,
            interaction.user.id, interaction.user.display_name, None, emoji, min_bet
        )
        # Update or create the single embed listing all challenges
        await self.update_challenges_embed()
        await interaction.response.send_message(f"Slot challenge set and announced. Challenge ID: {challenge_id}", ephemeral=True)

    async def update_challenges_embed(self):
        channel = self.bot.get_channel(CHALLENGE_CHANNEL_ID)
        if not channel:
            return
        active = get_all_active_slot_challenges()
        if not active:
            # Optionally delete the embed if no challenges remain
            return
        # Build a styled description for all challenges
        now_ts = int(datetime.now(dt.UTC).timestamp())
        desc = f"⏰ **Last Updated:** <t:{now_ts}:R>\n\n"
        desc += "First to hit the required multiplier wins the prize!\n"
        desc += "All prizes are paid out automatically by our secure system.\n"
        desc += "See <#{}> for payout logs!\n\n".format(LOGS_CHANNEL_ID)
        for challenge in active:
            try:
                dt_obj = challenge['start_time']
                if isinstance(dt_obj, str):
                    dt_obj = datetime.fromisoformat(dt_obj)
                unix_ts = int(dt_obj.timestamp())
                start_str = f'<t:{unix_ts}:f>'  # Discord timestamp (long date/time)
            except Exception:
                start_str = str(challenge['start_time'])
            emoji = challenge.get('emoji') or '🎰'
            min_bet_str = f"  **Min Bet:** `${challenge['min_bet']}`" if challenge.get('min_bet') else ""
            # Make the game name a hyperlink (no quotes)
            game_url = f"https://roobet.com/casino/game/{challenge['game_identifier']}"
            game_name_link = f"[{challenge['game_name']}]({game_url})"
            desc += f"**`#{challenge['challenge_id']}` | {emoji} {game_name_link}**\n"
            desc += f"**Multiplier:** `x{challenge['required_multi']}`  **Prize:** `${challenge['prize']}`{min_bet_str}\n"
            desc += f"**Start:** {start_str}\n\n"
        embed = discord.Embed(
            title="🎰 __Active Slot Challenges__ 🎰",
            description=desc,
            color=discord.Color.gold()
        )
        # Find the existing embed message (if any)
        message_id = None
        for challenge in active:
            if challenge['message_id']:
                message_id = challenge['message_id']
                break
        if message_id:
            try:
                msg = await channel.fetch_message(message_id)
                await msg.edit(embed=embed)
            except discord.errors.NotFound:
                # Message was deleted, send a new one and update all active challenges with new message id
                msg = await channel.send(embed=embed)
                for challenge in active:
                    update_challenge_message_id(challenge['challenge_id'], msg.id)
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit or send messages in challenge channel.")
        else:
            msg = await channel.send(embed=embed)
            for challenge in active:
                update_challenge_message_id(challenge['challenge_id'], msg.id)

    @app_commands.command(name="cancelchallenge", description="Cancel a specific slot challenge by its ID.")
    @app_commands.describe(challenge_id="The ID of the challenge to cancel.")
    async def cancel_challenge(self, interaction: discord.Interaction, challenge_id: int):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to cancel a challenge.", ephemeral=True)
            return
        active = get_all_active_slot_challenges()
        challenge = next((c for c in active if c['challenge_id'] == challenge_id), None)
        if not challenge:
            await interaction.response.send_message(f"No active slot challenge found with ID {challenge_id}.", ephemeral=True)
            return
        logger.info(f"Calling log_slot_challenge for CANCELLED: id={challenge['challenge_id']} game={challenge['game_name']} by={challenge['posted_by_username']}")
        log_slot_challenge(
            challenge["challenge_id"],
            challenge["game_name"],
            challenge["game_identifier"],
            challenge["posted_by"],
            challenge["posted_by_username"],
            None, None, None, None, None, challenge["start_time"]
        )
        remove_active_slot_challenge(challenge_id)
        await self.update_challenges_embed()
        await interaction.response.send_message(f"Slot challenge ID {challenge_id} cancelled.", ephemeral=True)
        # Log to logs channel
        logs_channel = self.bot.get_channel(LOGS_CHANNEL_ID)
        if logs_channel:
            embed = discord.Embed(
                title="❌ Slot Challenge Cancelled",
                description=f"Challenge ID {challenge_id} ({challenge['game_name']}) was cancelled by an admin.",
                color=discord.Color.red()
            )
            embed.add_field(name="Required Multiplier", value=f"x{challenge['required_multi']}", inline=True)
            embed.add_field(name="Prize", value=f"${challenge['prize']}", inline=True)
            embed.set_footer(text=f"Challenge start time (UTC): {challenge['start_time']}")
            await logs_channel.send(embed=embed)

    @tasks.loop(minutes=10)  # Now synchronized with DataManager schedule
    async def check_challenge(self):
        await asyncio.sleep(240)  # 4 minute offset (DataManager runs at 0:00, we run at 0:04)
        
        # Get data from DataManager instead of making API calls
        data_manager = self.get_data_manager()
        if not data_manager:
            logger.warning("[SlotChallenge] DataManager not available for challenge checking")
            return
            
        cached_data = data_manager.get_cached_data()
        if not cached_data:
            logger.warning("[SlotChallenge] No cached data available for challenge checking")
            return
            
        weighted_wager_data = cached_data.get('weighted_wager', [])
        
        active = get_all_active_slot_challenges()
        if not active:
            return
            
        completed_ids = set()
        logger.info(f"[SlotChallenge] Checking {len(active)} active challenges using cached data with {len(weighted_wager_data)} entries")
        
        for challenge in active:
            winners = []
            challenge_start_time = challenge['start_time']
            
            # Parse challenge start time to datetime for comparison
            if isinstance(challenge_start_time, str):
                try:
                    challenge_start_dt = datetime.fromisoformat(challenge_start_time.replace('Z', '+00:00'))
                except:
                    logger.error(f"[SlotChallenge] Could not parse start time for challenge {challenge['challenge_id']}")
                    continue
            else:
                challenge_start_dt = challenge_start_time
                
            # Filter cached data for this specific game and time period
            game_entries = []
            for entry in weighted_wager_data:
                hm = entry.get("highestMultiplier")
                if not (entry.get("uid") and entry.get("username") and hm):
                    continue
                    
                # Check if this entry is for the challenge game
                if hm.get("gameIdentifier") != challenge['game_identifier']:
                    continue
                    
                # For now, we'll include all entries since DataManager fetches current month
                # In a more sophisticated setup, we could filter by timestamp if available
                game_entries.append(entry)
            
            logger.info(f"[SlotChallenge] Challenge {challenge['game_name']} ({challenge['game_identifier']}): Found {len(game_entries)} relevant entries")
            
            # Check for winners in the filtered data
            for entry in game_entries:
                hm = entry.get("highestMultiplier")
                wagered = hm.get('wagered', 0)
                multiplier = hm.get('multiplier', 0)
                min_bet = challenge.get('min_bet')
                
                if (
                    multiplier >= challenge['required_multi']
                    and (min_bet is None or wagered >= min_bet)
                ):
                    winners.append({
                        "uid": entry['uid'],
                        "username": entry['username'],
                        "multiplier": multiplier,
                        "bet": wagered,
                        "payout": hm.get('payout', 0)
                    })
                    
            if winners:
                winners_sorted = sorted(winners, key=lambda x: x["multiplier"], reverse=True)
                winner = winners_sorted[0]
                second = winners_sorted[1] if len(winners_sorted) > 1 else None
                logs_channel = self.bot.get_channel(LOGS_CHANNEL_ID)
                self.payout_queue.put_nowait((challenge, winner, second, logs_channel))
                completed_ids.add(challenge["challenge_id"])
                logger.info(f"[SlotChallenge] Challenge {challenge['game_name']} completed by {winner['username']} with {winner['multiplier']}x")
                
        # Remove completed challenges
        for cid in completed_ids:
            remove_active_slot_challenge(cid)
        if completed_ids:
            await self.update_challenges_embed()

    def get_all_known_users(self, game_identifier, start_date):
        """
        Retrieve all users who wagered on a specific game since the challenge start date.
        Returns a list of dicts: [{"uid": ..., "username": ...}, ...]
        """
        from utils import fetch_weighted_wager
        data = fetch_weighted_wager(start_date, None)
        users = []
        seen = set()
        for entry in data:
            # Check if user has highestMultiplier for this game
            hm = entry.get("highestMultiplier")
            if (
                entry.get("uid") and entry.get("username") and hm and
                hm.get("gameIdentifier") == game_identifier and hm.get("wagered", 0) > 0
            ):
                key = (entry["uid"], entry["username"])
                if key not in seen:
                    users.append({"uid": entry["uid"], "username": entry["username"]})
                    seen.add(key)
        return users

    @check_challenge.before_loop
    async def before_challenge_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=14)
    async def ensure_challenge_embed(self):
        await asyncio.sleep(600)  # 10 minute offset
        await self.update_challenges_embed()

    @ensure_challenge_embed.before_loop
    async def before_ensure_challenge_embed(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="gamestats", description="Get a player's wager stats for a specific game.")
    @app_commands.describe(identifier="Game identifier (e.g. pragmatic:vs10bbbbrnd)", username="Username to search (case-sensitive, must match exactly)")
    async def gamestats(self, interaction: discord.Interaction, identifier: str, username: str = None):
        await interaction.response.defer(thinking=True)
        start_date, _ = get_current_month_range()
        user_list = await asyncio.to_thread(self.get_all_known_users, identifier, start_date)
        results = []
        for user in user_list:
            if username and user['username'] != username:
                continue
            stats = await asyncio.to_thread(fetch_user_game_stats, user['uid'], identifier, start_date)
            if stats:
                results.append({
                    "username": user['username'],
                    "multiplier": stats.get('weightedWagered', 0),
                    "bet": stats.get('wagered', 0),
                    "payout": None
                })
        if not results:
            await interaction.followup.send(f"No results found for game identifier `{identifier}`{f' and username `{username}`' if username else ''}.", ephemeral=True)
            return
        results.sort(key=lambda x: x["multiplier"], reverse=True)
        if username:
            r = results[0]
            desc = f"**Wager Stats for `{username}` on `{identifier}` this month:**\n\n"
            desc += f"`x{r['multiplier']}` | Bet: `${r['bet']}`\n"
        else:
            desc = f"**Top Wager Stats for `{identifier}` this month:**\n\n"
            for i, r in enumerate(results[:10], 1):
                desc += f"**#{i} {r['username']}** — `x{r['multiplier']}` | Bet: `${r['bet']}`\n"
        await interaction.followup.send(desc, ephemeral=True)

    @app_commands.command(name="challenge_results", description="Show top wager stats for each challenge since it started.")
    async def challenge_results(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        import json
        from db import get_all_active_slot_challenges, get_db_connection, release_db_connection
        from utils import fetch_weighted_wager
        logger = logging.getLogger(__name__)
        active = get_all_active_slot_challenges()
        # Only show live (active) challenges, not completed/logged ones
        all_challenges = []
        seen = set()
        for c in active:
            key = (c['game_identifier'], str(c['start_time']))
            if key not in seen:
                all_challenges.append({'game_identifier': c['game_identifier'], 'game_name': c['game_name'], 'start_time': c['start_time']})
                seen.add(key)
        desc = ""
        for challenge in all_challenges:
            start_date = challenge['start_time']
            end_date = None  # None means up to now
            # LOG: Challenge context
            logger.info(f"[CHALLENGE] {challenge['game_name']} ({challenge['game_identifier']}) Start: {start_date}")
            try:
                # Fetch only for this game identifier
                data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date, challenge['game_identifier'])
                logger.info(f"[API] fetch_weighted_wager(start_date={start_date}, end_date={end_date}, game_identifier={challenge['game_identifier']}) | Entries: {len(data)} | Data: {json.dumps(data, indent=2)[:1000]}")
            except Exception as e:
                desc += f"\n**{challenge['game_name']}**: Error fetching data: {e}\n"
                continue
            # LOG: Filtering for this game
            filtered = []
            for entry in data:
                hm = entry.get("highestMultiplier")
                logger.info(f"[ENTRY] username={entry.get('username')} uid={entry.get('uid')} highestMultiplier={json.dumps(hm)[:500]}")
                # DEBUG: Log all gameIds for this user
                if entry.get('username') == 'YOUR_ALT_USERNAME':
                    logger.info(f"[DEBUG] Alt user entry: {json.dumps(entry, indent=2)[:1000]}")
                if hm and hm.get("gameId") == challenge['game_identifier']:
                    filtered.append(entry)
            logger.info(f"[FILTER] gameId={challenge['game_identifier']} | Filtered Entries: {len(filtered)}")
            results = []
            for entry in filtered:
                hm = entry.get("highestMultiplier")
                results.append({
                    "username": entry.get("username", "Unknown"),
                    "multiplier": hm.get("multiplier", 0),
                    "bet": hm.get("wagered", 0),
                    "payout": hm.get("payout", 0)
                })
            if not results:
                desc += f"\n**{challenge['game_name']}** (`{challenge['game_identifier']}`): No results.\n"
                logger.info(f"[RESULTS] No results after filtering for multipliers.")
                continue
            results.sort(key=lambda x: x["multiplier"], reverse=True)
            logger.info(f"[RESULTS] Sorted: {json.dumps(results, indent=2)[:1000]}")
            desc += f"\n**{challenge['game_name']}** (`{challenge['game_identifier']}`)\n"
            for i, r in enumerate(results[:5], 1):
                desc += f"#{i} {r['username']} — `x{r['multiplier']}` | Bet: `${r['bet']}` | Payout: `${r['payout']}`\n"
            await asyncio.sleep(10)  # Add 10 second delay between each API call
        await interaction.followup.send(desc or "No challenge results found.", ephemeral=True)

    @tasks.loop(minutes=5)
    async def update_multi_challenge_history(self):
        await asyncio.sleep(600)  # 10 minute offset
        await self.refresh_multi_challenge_history_embed()

    async def refresh_multi_challenge_history_embed(self):
        await self.bot.wait_until_ready()
        channel = self.bot.get_channel(1387301442598998016)
        if not channel:
            logger.warning("Multi-challenge history channel not found.")
            return
        from db import get_all_completed_slot_challenges
        challenges = get_all_completed_slot_challenges()
        if not challenges:
            return
        now_ts = int(datetime.now(dt.UTC).timestamp())
        desc = f"⏰ **Last Updated:** <t:{now_ts}:R>\n\n"
        for c in challenges:
            import dateutil.parser
            try:
                dt_obj = c['challenge_start']
                if isinstance(dt_obj, str):
                    dt_obj = dateutil.parser.isoparse(dt_obj)
                unix_ts = int(dt_obj.timestamp())
                ts_str = f'<t:{unix_ts}:F>'
            except Exception:
                ts_str = str(c['challenge_start'])
            if c.get('game_identifier'):
                safe_game_name = c['game'].replace('_', '\\_')
                game_url = f"https://roobet.com/casino/game/{c['game_identifier']}"
                game_display = f"[{safe_game_name}]({game_url})"
            else:
                game_display = c['game'].replace('_', '\\_')
            username = c['winner_username'].strip()
            if len(username) > 3:
                username = f'`{username[:-3]}***`'
            else:
                username = '`***`'
            desc += (
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f":trophy: {game_display} | :moneybag: ${c['prize']:.2f} | :crown: {username}\n"
                f":heavy_multiplication_x: Achieved/Required Multi: x{c['multiplier']:.2f}/{c['required_multiplier']} | :dollar: Payout: ${c['payout']:.2f} (Base Bet: ${c['bet']:.2f})\n"
                f":date: {ts_str}\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            )
        history_message = None
        async for msg in channel.history(limit=10):
            if msg.author == self.bot.user and msg.embeds and msg.embeds[0].title == "Slot Challenge History":
                history_message = msg
                break
        embed = discord.Embed(title="Slot Challenge History", description=desc[:4096], color=discord.Color.gold())
        if history_message:
            try:
                await history_message.edit(embed=embed)
            except discord.NotFound:
                logger.warning("Multi-challenge history embed message was deleted. Reposting.")
                await channel.send(embed=embed)
        else:
            await channel.send(embed=embed)

    @app_commands.command(name="manualrefreshhistory", description="Manually refresh the Slot Challenge History embed (admin only, temporary)")
    async def manualrefreshhistory(self, interaction: discord.Interaction):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return
        await interaction.response.defer(thinking=True)
        await self.refresh_multi_challenge_history_embed()
        await interaction.followup.send("Slot Challenge History has been manually refreshed.", ephemeral=True)

    def cog_unload(self):
        self.check_challenge.cancel()
        self.ensure_challenge_embed.cancel()
        self.update_multi_challenge_history.cancel()
        if hasattr(self, 'process_payout_queue_task'):
            self.process_payout_queue_task.cancel()

async def setup(bot):
    await bot.add_cog(SlotChallenge(bot))
