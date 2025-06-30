import discord
from discord.ext import commands, tasks
from utils import fetch_total_wager, fetch_weighted_wager, get_current_month_range, fetch_user_game_stats
from db import get_all_active_slot_challenges
import os
import logging
from datetime import datetime
import datetime as dt
import asyncio
import json
import base64
import requests

logger = logging.getLogger(__name__)

class DataManager(commands.Cog):
    """Centralized data manager that fetches all API data and uploads to GitHub"""
    
    def __init__(self, bot):
        self.bot = bot
        self.cached_data = {}
        self.last_fetch_time = None
        
        # Start the main data fetching task
        self.fetch_and_upload_all_data.start()
        logger.info("[DataManager] Initialized - main data fetching task started")
    
    def get_cached_data(self, data_type=None):
        """Get cached data for other cogs to use"""
        if data_type:
            return self.cached_data.get(data_type, {})
        return self.cached_data
    
    def is_data_fresh(self, max_age_minutes=10):
        """Check if cached data is still fresh"""
        if not self.last_fetch_time:
            return False
        age = datetime.now(dt.UTC) - self.last_fetch_time
        return age.total_seconds() < (max_age_minutes * 60)
    
    @tasks.loop(minutes=10)  # Fetch all data every 10 minutes
    async def fetch_and_upload_all_data(self):
        """Main task that fetches all data and uploads JSON files"""
        try:
            logger.info("[DataManager] Starting data fetch and upload cycle")
            
            # Get current month range
            start_date, end_date = get_current_month_range()
            
            # Fetch all API data at once
            logger.info("[DataManager] Fetching total wager data")
            total_wager_data = await asyncio.to_thread(fetch_total_wager, start_date, end_date)
            
            logger.info("[DataManager] Fetching weighted wager data")
            weighted_wager_data = await asyncio.to_thread(fetch_weighted_wager, start_date, end_date)
            
            logger.info("[DataManager] Fetching active slot challenges")
            active_challenges = get_all_active_slot_challenges()
            
            # Cache all data
            self.cached_data = {
                'total_wager': total_wager_data,
                'weighted_wager': weighted_wager_data,
                'active_challenges': active_challenges,
                'period': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'start_timestamp': int(datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S%z').timestamp()),
                    'end_timestamp': int(datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S%z').timestamp())
                },
                'last_updated': datetime.now(dt.UTC).isoformat(),
                'last_updated_timestamp': int(datetime.now(dt.UTC).timestamp())
            }
            self.last_fetch_time = datetime.now(dt.UTC)
            
            logger.info(f"[DataManager] Data fetched - Total: {len(total_wager_data)}, Weighted: {len(weighted_wager_data)}, Challenges: {len(active_challenges)}")
            
            # Generate and upload all JSON files
            await self.generate_and_upload_json_files()
            
            logger.info("[DataManager] Data fetch and upload cycle completed successfully")
            
        except Exception as e:
            logger.error(f"[DataManager] Error in fetch_and_upload_all_data: {e}")
            import traceback
            logger.error(f"[DataManager] Traceback: {traceback.format_exc()}")
    
    async def generate_and_upload_json_files(self):
        """Generate all JSON files and upload them to GitHub"""
        try:
            logger.info("[DataManager] Generating JSON files...")
            
            # Generate main leaderboard JSON
            main_leaderboard_json = self.generate_main_leaderboard_json()
            logger.info("[DataManager] Main leaderboard JSON generated")
            
            # Generate multiplier leaderboard JSON
            multi_leaderboard_json = self.generate_multiplier_leaderboard_json()
            logger.info("[DataManager] Multiplier leaderboard JSON generated")
            
            # Generate slot challenges JSON
            challenges_json = self.generate_challenges_json()
            logger.info("[DataManager] Slot challenges JSON generated")
            
            # Upload all files to GitHub
            files_to_upload = [
                ("latestLBResults.json", main_leaderboard_json),
                ("LatestMultiLBResults.json", multi_leaderboard_json),
                ("ActiveSlotChallenges.json", challenges_json)
            ]
            
            logger.info(f"[DataManager] Uploading {len(files_to_upload)} files to GitHub...")
            
            for filename, data in files_to_upload:
                self.upload_to_github(filename, data)
                
            logger.info("[DataManager] All JSON files uploaded successfully")
            
        except Exception as e:
            logger.error(f"[DataManager] Error generating/uploading JSON: {e}")
    
    def generate_main_leaderboard_json(self):
        """Generate main leaderboard JSON"""
        total_wager_data = self.cached_data.get('total_wager', [])
        weighted_wager_data = self.cached_data.get('weighted_wager', [])
        
        # Create total wager lookup
        total_wager_dict = {entry.get("uid"): entry.get("wagered", 0) for entry in total_wager_data}
        
        # Sort weighted wager data
        weighted_wager_data.sort(
            key=lambda x: x.get("weightedWagered", 0) if isinstance(x.get("weightedWagered"), (int, float)) and x.get("weightedWagered") >= 0 else 0,
            reverse=True
        )
        
        # Build leaderboard results
        leaderboard_results = []
        for i in range(10):
            if i < len(weighted_wager_data):
                entry = weighted_wager_data[i]
                uid = entry.get("uid")
                username = entry.get("username", "Unknown")
                total_wagered = total_wager_dict.get(uid, 0) if uid in total_wager_dict else 0
                weighted_wagered = entry.get("weightedWagered", 0) if isinstance(entry.get("weightedWagered"), (int, float)) else 0
                
                leaderboard_results.append({
                    "rank": i + 1,
                    "uid": uid,
                    "username": username,
                    "wagered": total_wagered,
                    "weightedWagered": weighted_wagered
                })
        
        return leaderboard_results
    
    def generate_multiplier_leaderboard_json(self):
        """Generate multiplier leaderboard JSON"""
        weighted_wager_data = self.cached_data.get('weighted_wager', [])
        period = self.cached_data.get('period', {})
        
        # Filter and sort by highestMultiplier
        multi_data = [entry for entry in weighted_wager_data if entry.get("highestMultiplier") and entry["highestMultiplier"].get("multiplier", 0) > 0]
        multi_data.sort(key=lambda x: x["highestMultiplier"]["multiplier"], reverse=True)
        
        PRIZE_DISTRIBUTION = [75, 50, 25, 10, 5]
        
        leaderboard_json = {
            "leaderboard_type": "multiplier",
            "period": period,
            "last_updated": self.cached_data.get('last_updated'),
            "last_updated_timestamp": self.cached_data.get('last_updated_timestamp'),
            "entries": []
        }
        
        # Add top 5 entries
        for i in range(5):
            if i < len(multi_data):
                entry = multi_data[i]
                username = entry.get("username", "Unknown")
                # Apply username masking
                if len(username) > 3:
                    masked_username = username[:-3] + "***"
                else:
                    masked_username = "***"
                
                leaderboard_json["entries"].append({
                    "rank": i + 1,
                    "username": masked_username,
                    "multiplier": entry["highestMultiplier"].get("multiplier", 0),
                    "game": entry["highestMultiplier"].get("gameTitle", "Unknown"),
                    "game_identifier": entry["highestMultiplier"].get("gameIdentifier", None),
                    "wagered": entry["highestMultiplier"].get("wagered", 0),
                    "payout": entry["highestMultiplier"].get("payout", 0),
                    "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
                })
            else:
                # Add empty slot for consistent structure
                leaderboard_json["entries"].append({
                    "rank": i + 1,
                    "username": "N/A",
                    "multiplier": 0,
                    "game": "Unknown",
                    "game_identifier": None,
                    "wagered": 0,
                    "payout": 0,
                    "prize": PRIZE_DISTRIBUTION[i] if i < len(PRIZE_DISTRIBUTION) else 0
                })
        
        return leaderboard_json
    
    def generate_challenges_json(self):
        """Generate active challenges JSON"""
        active_challenges = self.cached_data.get('active_challenges', [])
        
        challenges_json = {
            "data_type": "active_slot_challenges",
            "last_updated": self.cached_data.get('last_updated'),
            "last_updated_timestamp": self.cached_data.get('last_updated_timestamp'),
            "total_challenges": len(active_challenges),
            "challenges": []
        }
        
        # Add challenge data
        for challenge in active_challenges:
            challenge_data = {
                "challenge_id": challenge["challenge_id"],
                "game_identifier": challenge["game_identifier"],
                "game_name": challenge["game_name"],
                "required_multiplier": challenge["required_multi"],  # Fixed: use required_multi not prize
                "prize": challenge["prize"],
                "start_time": challenge["start_time"].isoformat() if isinstance(challenge["start_time"], datetime) else challenge["start_time"],
                "posted_by": challenge["posted_by"],
                "posted_by_username": challenge["posted_by_username"],
                "emoji": challenge.get("emoji"),
                "min_bet": challenge.get("min_bet")
            }
            
            # Add start timestamp if we can parse the time
            try:
                if isinstance(challenge["start_time"], str):
                    start_dt = datetime.fromisoformat(challenge["start_time"].replace('Z', '+00:00'))
                    challenge_data["start_timestamp"] = int(start_dt.timestamp())
                elif isinstance(challenge["start_time"], datetime):
                    challenge_data["start_timestamp"] = int(challenge["start_time"].timestamp())
            except:
                pass
                
            challenges_json["challenges"].append(challenge_data)
        
        return challenges_json
    
    def upload_to_github(self, filename, data):
        """Upload a single file to GitHub"""
        GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
        REPO_OWNER = "FTSStreams"
        REPO_NAME = "wagerData"
        BRANCH = "main"
        API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{filename}"
        
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        try:
            # Convert to JSON and encode as base64
            json_content = json.dumps(data, indent=2)
            content = base64.b64encode(json_content.encode()).decode()
            
            # Get the current file SHA if it exists
            resp = requests.get(API_URL, headers=headers)
            if resp.status_code == 200:
                sha = resp.json()["sha"]
            else:
                sha = None
            
            data_payload = {
                "message": f"Update {filename}",
                "content": content,
                "branch": BRANCH
            }
            if sha:
                data_payload["sha"] = sha
            
            put_resp = requests.put(API_URL, headers=headers, json=data_payload)
            if put_resp.status_code in (200, 201):
                logger.info(f"[DataManager] {filename} uploaded to GitHub successfully")
            else:
                logger.error(f"[DataManager] Failed to upload {filename}: {put_resp.status_code} {put_resp.text}")
                
        except Exception as e:
            logger.error(f"[DataManager] Error uploading {filename} to GitHub: {e}")
    
    def cog_unload(self):
        self.fetch_and_upload_all_data.cancel()

    @fetch_and_upload_all_data.before_loop
    async def before_fetch_loop(self):
        await self.bot.wait_until_ready()
        logger.info("[DataManager] Bot ready, waiting 30 seconds before first data fetch...")
        # Wait 30 seconds after bot start before first fetch (reduced from 2 minutes)
        await asyncio.sleep(30)

async def setup(bot):
    await bot.add_cog(DataManager(bot))
