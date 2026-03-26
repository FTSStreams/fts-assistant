import discord
from discord import app_commands
from discord.ext import commands, tasks
from utils import send_tip
from db import (
    get_all_active_slot_challenges, add_active_slot_challenge, remove_active_slot_challenge, log_slot_challenge,
    get_leaderboard_message_id, save_leaderboard_message_id, save_tip_log
)
import os
import logging
from datetime import datetime, timezone
import datetime as dt
import asyncio

logger = logging.getLogger(__name__)
GUILD_ID = int(os.getenv("GUILD_ID"))
CHALLENGE_CHANNEL_ID = int(os.getenv("CHALLENGE_CHANNEL_ID"))
HISTORY_CHANNEL_ID = 1387301442598998016  # Consolidated history channel for all events
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID"))
ACTIVE_CHALLENGES_CHANNEL_ID = 1385820512529158226

class SlotChallenge(commands.Cog):
    challenge = app_commands.Group(name="challenge", description="Slot challenge management commands")

    def __init__(self, bot):
        self.bot = bot
        self.check_challenge.start()
        self.ensure_challenge_embed.start()
        self.payout_queue = asyncio.Queue()
        self.process_payout_queue_task = asyncio.create_task(self.process_payout_queue())
        # History now uses individual posts instead of large embed updates

    async def process_payout_queue(self):
        while True:
            challenge, winner, second, history_channel = await self.payout_queue.get()
            tip_response = await send_tip(
                user_id=os.getenv("ROOBET_USER_ID"),
                to_username=winner["username"],
                to_user_id=winner["uid"],
                amount=challenge["prize"]
            )
            if tip_response.get("success"):
                # Censor usernames for public display
                winner_display_name = winner['username']
                if len(winner_display_name) > 3:
                    winner_display_name = winner_display_name[:-3] + "•••"
                else:
                    winner_display_name = "•••"
                
                # Create payout embed in the current event-log style.
                embed = discord.Embed(
                    title="🏆 Slot Challenge Payout",
                    color=discord.Color.green()
                )
                
                description = ""
                
                # Add timestamps for challenge duration
                try:
                    start_dt = datetime.fromisoformat(str(challenge['start_time']).replace('Z', '+00:00'))
                    end_dt = datetime.now(timezone.utc)
                    start_ts = int(start_dt.timestamp())
                    end_ts = int(end_dt.timestamp())
                    description += f"**Challenge Duration:** <t:{start_ts}:F> → <t:{end_ts}:F>\n\n"
                except Exception:
                    description += f"**Challenge Duration:** Started {challenge['start_time']}\n\n"

                description += "***Challenge Details:***\n\n"
                description += f"🎰 **Game:** {challenge['game_name']}\n"
                description += f"📈 **Multi Required:** x{int(challenge['required_multi'])}\n"
                description += f"💰 **Prize:** ${challenge.get('prize', 0):.2f} USD\n"
                if challenge.get('min_bet'):
                    description += f"🪙 **Minimum Bet:** ${challenge['min_bet']:.2f}\n"
                description += f"🧾 **Challenge ID:** #{challenge['challenge_id']}\n\n"

                description += "***Winner:***\n\n"
                description += f"🆔 **ID:** {winner_display_name}\n"
                description += f"✅ **Multiplier Achieved:** x{winner.get('multiplier', 0):.2f}\n"
                description += f"💰 **Bet:** ${winner.get('bet', 0):.2f} | **Payout:** ${winner.get('payout', 0):.2f}\n"
                description += f"💸 **Prize Sent:** ${challenge.get('prize', 0):.2f}\n\n"
                description += (
                    f"Track Current Active Challenges -> <#{ACTIVE_CHALLENGES_CHANNEL_ID}>"
                )
                
                embed.description = description
                embed.set_footer(text="AutoTip Engine Live • Payout Sent Successfully")
                
                # Send with role ping
                if history_channel:
                    ping_role_id = os.getenv("SLOT_CHALLENGE_PING_ROLE_ID")
                    content = f"<@&{ping_role_id}>" if ping_role_id else None
                    await history_channel.send(content=content, embed=embed)
                logger.info(f"Calling log_slot_challenge for COMPLETED: id={challenge['challenge_id']} game={challenge['game_name']} winner={winner['username']}")
                # Use the actual completion time for logging
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
                
                # Also log to manualtips for tipstats inclusion
                save_tip_log(
                    winner["uid"],
                    winner["username"], 
                    challenge["prize"],
                    "slot_challenge",
                    month=datetime.now(timezone.utc).month,
                    year=datetime.now(timezone.utc).year
                )
            else:
                # Send failed payout embed
                if history_channel:
                    embed = discord.Embed(
                        title="⚠️ Challenge Complete - Payment Issue",
                        color=discord.Color.orange()
                    )
                    
                    # Build single-line description
                    description = f"**Game:** {challenge['game_name']}\n"
                    description += f"**Winner:** {winner['username']}\n"
                    description += f"**Multiplier Achieved:** x{winner.get('multiplier', 0):.2f}\n"
                    description += f"**Prize Amount:** ${challenge.get('prize', 0):.2f}\n\n"
                    description += f"❌ **Payment Failed:** Insufficient account balance\n"
                    description += f"📋 **Next Steps:** Please create a ticket in <#1296221508145905674>\n\n"
                    description += f"**Challenge ID:** #{challenge['challenge_id']}"
                    
                    embed.description = description
                    
                    ping_role_id = os.getenv("SLOT_CHALLENGE_PING_ROLE_ID")
                    content = f"<@&{ping_role_id}>" if ping_role_id else None
                    await history_channel.send(content=content, embed=embed)
            await asyncio.sleep(30)
            self.payout_queue.task_done()

    @challenge.command(name="create", description="Set a slot challenge for a specific game and multiplier.")
    @app_commands.describe(game_identifier="Game identifier (e.g. pragmatic:vs10bbbbrnd)", game_name="Game name for display", required_multi="Required multiplier (e.g. 100)", prize="Prize amount in USD", emoji="Optional emoji for this challenge", min_bet="Minimum bet size in USD (optional)")
    async def set_challenge(self, interaction: discord.Interaction, game_identifier: str, game_name: str, required_multi: float, prize: float, emoji: str = None, min_bet: float = None):
        if interaction.user.id != BOT_OWNER_ID:
            await interaction.response.send_message("You do not have permission to set a challenge.", ephemeral=True)
            return
        
        # Input validation
        if required_multi <= 0:
            await interaction.response.send_message("❌ Required multiplier must be greater than 0.", ephemeral=True)
            return
        if prize <= 0:
            await interaction.response.send_message("❌ Prize amount must be greater than 0.", ephemeral=True)
            return
        if min_bet is not None and min_bet <= 0:
            await interaction.response.send_message("❌ Minimum bet must be greater than 0.", ephemeral=True)
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
        
        # Send new challenge notification to history channel
        history_channel = self.bot.get_channel(HISTORY_CHANNEL_ID)
        if history_channel:
            embed = discord.Embed(
                title="🎰 New Slot Challenge Started!",
                color=discord.Color.blue()
            )
            
            # Build single-line description
            description = f"**Game:** {clean_game_name}\n"
            description += f"**Required Multiplier:** x{int(required_multi)}\n"
            description += f"**Prize:** ${prize:.2f}\n"
            if min_bet:
                description += f"**Minimum Bet:** ${min_bet:.2f}\n"
            description += f"\n"
            
            # Add timestamp for start time
            try:
                start_dt = datetime.fromisoformat(challenge_start_utc)
                start_ts = int(start_dt.timestamp())
                description += f"⏰ **Challenge Active:** Ready to start!\n"
                description += f"**Started:** <t:{start_ts}:F>\n\n"
            except Exception:
                description += f"**Started:** {challenge_start_utc}\n\n"
            
            description += f"**Challenge ID:** #{challenge_id}\n"
            description += f"🍀 **Status:** Good luck!"
            
            embed.description = description
            
            ping_role_id = os.getenv("SLOT_CHALLENGE_PING_ROLE_ID")
            content = f"<@&{ping_role_id}>" if ping_role_id else None
            await history_channel.send(content=content, embed=embed)
        
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
        desc += "📜 **Rules & Disclosure:**\n"
        desc += "• First to hit the required multiplier wins the prize.\n"
        desc += "• All prizes are paid automatically by our secure system.\n"
        desc += f"• See <#{HISTORY_CHANNEL_ID}> for event logs.\n\n"
        desc += "💵 **All amounts displayed are in USD.**\n\n"
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
            desc += f"**#{challenge['challenge_id']} — {emoji} `{challenge['game_name']}`**\n"
            desc += f"🎯 **Multiplier:** `x{float(challenge['required_multi']):,.2f}`\n"
            desc += f"🎁 **Prize:** `${float(challenge['prize']):,.2f}`\n"
            if challenge.get('min_bet'):
                desc += f"💵 **Min Bet:** `${float(challenge['min_bet']):,.2f}`\n"
            desc += f"🕒 **Start:** {start_str}\n\n"
        embed = discord.Embed(
            title="🎰 **Active Slot Challenges** 🎰",
            description=desc,
            color=discord.Color.gold()
        )
        embed.set_footer(text="AutoTip Engine • Auto-pays ~15 minutes after challenge completion.")
        
        # Use consistent message ID tracking like leaderboards
        message_id = get_leaderboard_message_id(key="active_challenges_message_id")
        if message_id:
            try:
                msg = await channel.fetch_message(message_id)
                await msg.edit(embed=embed)
                logger.info("[SlotChallenge] Active challenges message updated.")
            except discord.errors.NotFound:
                logger.warning(f"Active challenges message ID {message_id} not found, sending new message.")
                try:
                    msg = await channel.send(embed=embed)
                    save_leaderboard_message_id(msg.id, key="active_challenges_message_id")
                    logger.info("[SlotChallenge] New active challenges message sent.")
                except discord.errors.Forbidden:
                    logger.error("Bot lacks permission to send messages in challenge channel.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to edit messages in challenge channel.")
        else:
            logger.info("[SlotChallenge] No active challenges message ID found, sending new message.")
            try:
                msg = await channel.send(embed=embed)
                save_leaderboard_message_id(msg.id, key="active_challenges_message_id")
                logger.info("[SlotChallenge] New active challenges message sent.")
            except discord.errors.Forbidden:
                logger.error("Bot lacks permission to send messages in challenge channel.")

    @challenge.command(name="remove", description="Cancel a specific slot challenge by its ID.")
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
            None, None, None, None, None, challenge.get("min_bet"), challenge["start_time"]
        )
        remove_active_slot_challenge(challenge_id)
        await self.update_challenges_embed()
        await interaction.response.send_message(f"Slot challenge ID {challenge_id} cancelled.", ephemeral=True)
        
        # Send cancellation notification to history channel
        history_channel = self.bot.get_channel(HISTORY_CHANNEL_ID)
        if history_channel:
            embed = discord.Embed(
                title="❌ Slot Challenge Cancelled",
                color=discord.Color.red()
            )
            
            # Build single-line description
            description = f"**Game:** {challenge['game_name']}\n"
            description += f"**Required Multiplier:** x{int(challenge['required_multi'])}\n"
            description += f"**Prize:** ${challenge.get('prize', 0):.2f}\n\n"
            description += f"**Reason:** Admin Cancellation\n\n"
            
            # Add timestamps for challenge duration
            try:
                start_dt = datetime.fromisoformat(str(challenge['start_time']).replace('Z', '+00:00'))
                end_dt = datetime.now(timezone.utc)
                start_ts = int(start_dt.timestamp())
                end_ts = int(end_dt.timestamp())
                description += f"**Challenge Duration:** <t:{start_ts}:F> → <t:{end_ts}:F>\n"
            except Exception:
                description += f"**Challenge Duration:** Started {challenge['start_time']}\n"
            
            description += f"**Challenge ID:** #{challenge_id}"
            
            embed.description = description
            
            ping_role_id = os.getenv("SLOT_CHALLENGE_PING_ROLE_ID")
            content = f"<@&{ping_role_id}>" if ping_role_id else None
            await history_channel.send(content=content, embed=embed)

    @tasks.loop(minutes=10)  # Now synchronized with DataManager schedule
    async def check_challenge(self):
        logger.info("[SlotChallenge] Starting challenge check cycle, waiting 2 minutes...")
        await asyncio.sleep(120)  # 2 minute offset (DataManager runs at 0:00, we run at 0:02)
        
        active = get_all_active_slot_challenges()
        if not active:
            return
            
        completed_ids = set()
        logger.info(f"[SlotChallenge] Checking {len(active)} active challenges using individual API calls")
        
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
                
            # Make single API call to get ALL users and multipliers for this specific game
            start_date_str = challenge_start_dt.isoformat()
            try:
                from utils import fetch_weighted_wager
                game_data = await asyncio.to_thread(fetch_weighted_wager, start_date_str, None, challenge['game_identifier'])
                
                # Check each user's highest multiplier for this specific game
                best_multi = 0.0
                best_user = None
                for entry in game_data:
                    hm = entry.get("highestMultiplier")
                    if not (entry.get("uid") and entry.get("username") and hm):
                        continue
                        
                    # Verify this is for the correct game (should be, due to API filter)
                    if hm.get("gameId") != challenge['game_identifier']:
                        continue
                        
                    wagered = hm.get('wagered', 0)
                    multiplier = hm.get('multiplier', 0)
                    min_bet = challenge.get('min_bet')
                    
                    # Ensure required_multi is a float for comparison
                    required_multi = float(challenge['required_multi'])
                    # Ensure min_bet is a float if it exists
                    if min_bet is not None:
                        min_bet = float(min_bet)
                    
                    # Check if this multiplier meets the challenge requirements
                    meets_multi = multiplier >= required_multi
                    # Use rounding for bet comparison to avoid floating-point precision issues
                    meets_bet = (min_bet is None or round(wagered, 2) >= round(min_bet, 2))
                    
                    if multiplier > best_multi:
                        best_multi = multiplier
                        best_user = entry['username']
                    
                    if meets_multi and meets_bet:
                        winners.append({
                            "uid": entry['uid'],
                            "username": entry['username'],
                            "multiplier": multiplier,
                            "bet": wagered,
                            "payout": hm.get('payout', 0)
                        })
                
                outcome = f"winner: {winners[0]['username']} x{winners[0]['multiplier']}" if winners else f"no winner (best: {best_user} x{best_multi:.2f})" if best_user else "no users"
                logger.info(f"[SlotChallenge] {challenge['game_name']}: {len(game_data)} users — {outcome}")
                        
            except Exception as e:
                logger.error(f"[SlotChallenge] Error fetching game data for challenge {challenge['challenge_id']}: {e}")
                continue
            
            # Add 10 second delay between challenge checks
            await asyncio.sleep(10)
            if winners:
                winners_sorted = sorted(winners, key=lambda x: x["multiplier"], reverse=True)
                winner = winners_sorted[0]
                second = winners_sorted[1] if len(winners_sorted) > 1 else None
                history_channel = self.bot.get_channel(HISTORY_CHANNEL_ID)
                self.payout_queue.put_nowait((challenge, winner, second, history_channel))
                completed_ids.add(challenge["challenge_id"])
                logger.info(f"[SlotChallenge] Challenge {challenge['game_name']} completed by {winner['username']} with {winner['multiplier']}x")
                
        # Remove completed challenges
        for cid in completed_ids:
            remove_active_slot_challenge(cid)
        if completed_ids:
            await self.update_challenges_embed()
        
        # Always update challenge embed after checking (ensures fresh data)
        logger.info("[SlotChallenge] Updating challenge embed after check cycle")
        await self.update_challenges_embed()

    @check_challenge.before_loop
    async def before_challenge_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=1)  # Run every hour as backup only
    async def ensure_challenge_embed(self):
        logger.info("[SlotChallenge] Running hourly backup embed check...")
        await self.update_challenges_embed()

    @ensure_challenge_embed.before_loop
    async def before_ensure_challenge_embed(self):
        await self.bot.wait_until_ready()

    @challenge.command(name="results", description="Show top wager stats for each challenge since it started.")
    async def challenge_results(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        from db import get_all_active_slot_challenges, get_db_connection, release_db_connection
        from utils import fetch_weighted_wager
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
            logger.info(f"[SlotChallenge] Fetching challenge results for {challenge['game_name']} ({challenge['game_identifier']})")
            try:
                # Fetch only for this game identifier
                data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date, challenge['game_identifier'])
                logger.info(f"[SlotChallenge] Found {len(data)} users for {challenge['game_name']}")
            except Exception as e:
                desc += f"\n**{challenge['game_name']}**: Error fetching data: {e}\n"
                logger.error(f"[SlotChallenge] API error for {challenge['game_name']}: {e}")
                continue
            
            # Filter and process results
            results = []
            for entry in data:
                hm = entry.get("highestMultiplier")
                if hm and hm.get("gameId") == challenge['game_identifier']:
                    wagered = hm.get('wagered', 0)
                    multiplier = hm.get('multiplier', 0)
                    payout = hm.get('payout', 0)
                    username = entry.get("username", "Unknown")
                    
                    # Log user data for verification
                    logger.info(f"[SlotChallenge] User: {username} | Bet: ${wagered:.2f} | Payout: ${payout:.2f} | Multi: x{multiplier}")
                    
                    results.append({
                        "username": username,
                        "multiplier": multiplier,
                        "bet": wagered,
                        "payout": payout
                    })
            
            if not results:
                desc += f"\n**{challenge['game_name']}** (`{challenge['game_identifier']}`): No results.\n"
                logger.info(f"[SlotChallenge] No results found for {challenge['game_name']}")
                continue
            
            results.sort(key=lambda x: x["multiplier"], reverse=True)
            logger.info(f"[SlotChallenge] {challenge['game_name']} results: {len(results)} players, top multiplier: {results[0]['multiplier']}x")
            desc += f"\n**{challenge['game_name']}** (`{challenge['game_identifier']}`)\n"
            for i, r in enumerate(results[:5], 1):
                desc += f"#{i} {r['username']} — `x{r['multiplier']}` | Bet: `${r['bet']}` | Payout: `${r['payout']}`\n"
            await asyncio.sleep(10)  # Add 10 second delay between each API call
        
        # Handle Discord's 2000 character limit
        if not desc:
            await interaction.followup.send("No challenge results found.", ephemeral=True)
        elif len(desc) <= 2000:
            await interaction.followup.send(desc, ephemeral=True)
        else:
            # Split the message into chunks
            chunks = []
            current_chunk = ""
            lines = desc.split('\n')
            
            for line in lines:
                if len(current_chunk + line + '\n') > 2000:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                        current_chunk = line + '\n'
                    else:
                        # Single line too long, truncate it
                        chunks.append(line[:1900] + "... (truncated)")
                        current_chunk = ""
                else:
                    current_chunk += line + '\n'
            
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            # Send the first chunk
            await interaction.followup.send(chunks[0], ephemeral=True)
            
            # Send remaining chunks
            for chunk in chunks[1:]:
                await interaction.followup.send(chunk, ephemeral=True)

    def cog_unload(self):
        self.check_challenge.cancel()
        self.ensure_challenge_embed.cancel()
        if hasattr(self, 'process_payout_queue_task'):
            self.process_payout_queue_task.cancel()

async def setup(bot):
    await bot.add_cog(SlotChallenge(bot))
