import discord
from discord.ext import commands, tasks
from utils import fetch_total_wager, fetch_weighted_wager, get_current_month_range, fetch_user_game_stats
from db import get_all_active_slot_challenges, get_all_completed_slot_challenges, get_db_connection, release_db_connection
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
            
            # Generate all-time tips JSON
            all_time_tips_json = self.generate_all_time_tips_json()
            logger.info("[DataManager] All-time tips JSON generated")
            
            # Generate challenge history JSON
            challenge_history_json = self.generate_challenge_history_json()
            logger.info("[DataManager] Challenge history JSON generated")
            
            # Generate all wager data JSON (since Jan 1, 2025)
            all_wager_data_json = self.generate_all_wager_data_json()
            logger.info("[DataManager] All wager data JSON generated")
            
            # Upload all files to GitHub
            files_to_upload = [
                ("latestLBResults.json", main_leaderboard_json),
                ("LatestMultiLBResults.json", multi_leaderboard_json),
                ("ActiveSlotChallenges.json", challenges_json),
                ("allTimeTips.json", all_time_tips_json),
                ("challengeHistory.json", challenge_history_json),
                ("allWagerData.json", all_wager_data_json)
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
    
    def generate_all_time_tips_json(self):
        """Generate all-time tips JSON aggregating manual, milestone, and slot challenge tips"""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Get aggregated tip data by user and tip type
                cur.execute("""
                    SELECT 
                        user_id,
                        username,
                        tip_type,
                        SUM(amount) as total_amount,
                        COUNT(*) as tip_count,
                        MIN(tipped_at) as first_tip,
                        MAX(tipped_at) as latest_tip
                    FROM manualtips 
                    GROUP BY user_id, username, tip_type
                    ORDER BY SUM(amount) DESC;
                """)
                tip_data = cur.fetchall()
                
                # Get overall totals by tip type
                cur.execute("""
                    SELECT 
                        tip_type,
                        SUM(amount) as total_amount,
                        COUNT(*) as tip_count
                    FROM manualtips
                    GROUP BY tip_type;
                """)
                tip_type_totals = cur.fetchall()
                
                # Get top recipients overall
                cur.execute("""
                    SELECT 
                        user_id,
                        username,
                        SUM(amount) as total_received,
                        COUNT(*) as total_tips
                    FROM manualtips
                    GROUP BY user_id, username
                    ORDER BY SUM(amount) DESC
                    LIMIT 20;
                """)
                top_recipients = cur.fetchall()
                
        except Exception as e:
            logger.error(f"Error querying tip data: {e}")
            return {"error": "Failed to generate tips data"}
        finally:
            release_db_connection(conn)
        
        # Build the JSON response
        tips_json = {
            "data_type": "all_time_tips",
            "last_updated": datetime.now(dt.UTC).isoformat(),
            "last_updated_timestamp": int(datetime.now(dt.UTC).timestamp()),
            "summary": {
                "total_tips_sent": 0,
                "total_amount_sent": 0,
                "by_type": {}
            },
            "top_recipients": [],
            "detailed_data": []
        }
        
        # Process tip type totals
        for tip_type, total_amount, tip_count in tip_type_totals:
            tips_json["summary"]["by_type"][tip_type] = {
                "total_amount": float(total_amount),
                "tip_count": tip_count
            }
            tips_json["summary"]["total_tips_sent"] += tip_count
            tips_json["summary"]["total_amount_sent"] += float(total_amount)
        
        # Process top recipients (with username censoring for public repo)
        for user_id, username, total_received, total_tips in top_recipients:
            # Apply username censoring like other public embeds
            censored_username = username
            if len(username) > 3:
                censored_username = username[:-3] + "***"
            else:
                censored_username = "***"
                
            tips_json["top_recipients"].append({
                "user_id": user_id,
                "username": censored_username,
                "total_received": float(total_received),
                "total_tips": total_tips
            })
        
        # Process detailed data by user and tip type (also censored)
        user_tips = {}
        for user_id, username, tip_type, total_amount, tip_count, first_tip, latest_tip in tip_data:
            # Apply username censoring
            censored_username = username
            if len(username) > 3:
                censored_username = username[:-3] + "***"
            else:
                censored_username = "***"
                
            if user_id not in user_tips:
                user_tips[user_id] = {
                    "user_id": user_id,
                    "username": censored_username,
                    "tips_by_type": {},
                    "total_received": 0
                }
            
            user_tips[user_id]["tips_by_type"][tip_type] = {
                "amount": float(total_amount),
                "count": tip_count,
                "first_tip": first_tip.isoformat() if first_tip else None,
                "latest_tip": latest_tip.isoformat() if latest_tip else None
            }
            user_tips[user_id]["total_received"] += float(total_amount)
        
        # Convert to list and sort by total received
        tips_json["detailed_data"] = sorted(
            user_tips.values(), 
            key=lambda x: x["total_received"], 
            reverse=True
        )
        
        return tips_json
    
    def generate_challenge_history_json(self):
        """Generate challenge history JSON from completed slot challenges"""
        try:
            completed_challenges = get_all_completed_slot_challenges()
        except Exception as e:
            logger.error(f"Error fetching completed challenges: {e}")
            return {"error": "Failed to fetch challenge history"}
        
        # Build the JSON response
        history_json = {
            "data_type": "challenge_history",
            "last_updated": datetime.now(dt.UTC).isoformat(),
            "last_updated_timestamp": int(datetime.now(dt.UTC).timestamp()),
            "total_completed_challenges": len(completed_challenges),
            "total_prizes_paid": 0,
            "challenges": []
        }
        
        # Process each completed challenge
        for challenge in completed_challenges:
            # Apply username censoring for public repo
            winner_username = challenge.get("winner_username", "Unknown")
            censored_winner = winner_username
            if len(winner_username) > 3:
                censored_winner = winner_username[:-3] + "***"
            else:
                censored_winner = "***"
            
            # Add to total prizes paid
            history_json["total_prizes_paid"] += float(challenge.get("prize", 0))
            
            # Build challenge data
            challenge_data = {
                "challenge_id": challenge.get("challenge_id"),
                "game_name": challenge.get("game"),
                "game_identifier": challenge.get("game_identifier"),
                "required_multiplier": float(challenge.get("required_multiplier", 0)),
                "achieved_multiplier": float(challenge.get("multiplier", 0)),
                "min_bet_requirement": float(challenge.get("min_bet", 0)) if challenge.get("min_bet") else None,
                "winner": {
                    "username": censored_winner,
                    "user_id": challenge.get("winner_uid"),
                    "bet_amount": float(challenge.get("bet", 0)),
                    "payout": float(challenge.get("payout", 0)),
                    "multiplier_achieved": float(challenge.get("multiplier", 0))
                },
                "prize_amount": float(challenge.get("prize", 0)),
                "challenge_start": challenge.get("challenge_start").isoformat() if challenge.get("challenge_start") else None
            }
            
            # Add start timestamp if we can parse the time
            try:
                if challenge.get("challenge_start"):
                    if isinstance(challenge["challenge_start"], str):
                        start_dt = datetime.fromisoformat(challenge["challenge_start"].replace('Z', '+00:00'))
                        challenge_data["challenge_start_timestamp"] = int(start_dt.timestamp())
                    elif hasattr(challenge["challenge_start"], 'timestamp'):
                        challenge_data["challenge_start_timestamp"] = int(challenge["challenge_start"].timestamp())
            except:
                pass
                
            # Create game URL if identifier exists
            if challenge.get("game_identifier"):
                challenge_data["game_url"] = f"https://roobet.com/casino/game/{challenge['game_identifier']}"
            
            history_json["challenges"].append(challenge_data)
        
        # Sort challenges by start time (most recent first)
        history_json["challenges"].sort(
            key=lambda x: x.get("challenge_start_timestamp", 0), 
            reverse=True
        )
        
        return history_json
    
    def generate_all_wager_data_json(self):
        """Generate comprehensive wager data JSON with both lifetime (since Jan 1, 2025) and current month data"""
        try:
            # Get current month range (already available from cached data)
            current_month_start, current_month_end = get_current_month_range()
            
            # Set lifetime start date to January 1, 2025
            lifetime_start_date = "2025-01-01T00:00:00+00:00"
            lifetime_end_date = datetime.now(dt.UTC).isoformat()
            
            logger.info(f"[DataManager] Fetching lifetime wager data from {lifetime_start_date} to {lifetime_end_date}")
            logger.info(f"[DataManager] Fetching current month wager data from {current_month_start} to {current_month_end}")
            
            # Fetch lifetime data (since Jan 1, 2025)
            lifetime_total_wager = fetch_total_wager(lifetime_start_date, lifetime_end_date)
            lifetime_weighted_wager = fetch_weighted_wager(lifetime_start_date, lifetime_end_date)
            
            # Fetch current month data
            month_total_wager = fetch_total_wager(current_month_start, current_month_end)
            month_weighted_wager = fetch_weighted_wager(current_month_start, current_month_end)
            
            # Build the comprehensive JSON response
            wager_json = {
                "data_type": "comprehensive_wager_data",
                "last_updated": datetime.now(dt.UTC).isoformat(),
                "last_updated_timestamp": int(datetime.now(dt.UTC).timestamp()),
                "periods": {
                    "lifetime": {
                        "start_date": lifetime_start_date,
                        "end_date": lifetime_end_date,
                        "description": "All wager data since January 1, 2025"
                    },
                    "current_month": {
                        "start_date": current_month_start,
                        "end_date": current_month_end,
                        "description": f"Current month wager data"
                    }
                },
                "summary": {
                    "lifetime": {
                        "total_users": len(lifetime_total_wager),
                        "total_wagered": 0,
                        "total_weighted_wagered": 0,
                        "highest_wagerer": None,
                        "highest_weighted_wagerer": None
                    },
                    "current_month": {
                        "total_users": len(month_total_wager),
                        "total_wagered": 0,
                        "total_weighted_wagered": 0,
                        "highest_wagerer": None,
                        "highest_weighted_wagerer": None
                    }
                },
                "data": {
                    "lifetime": {
                        "total_wager_data": [],
                        "weighted_wager_data": []
                    },
                    "current_month": {
                        "total_wager_data": [],
                        "weighted_wager_data": []
                    }
                }
            }
            
            # Helper function to process wager data with censoring
            def process_total_wager_data(data, period_key):
                total_wagered = 0
                highest_wagerer = None
                highest_amount = 0
                processed_data = []
                
                for entry in data:
                    wagered = entry.get("wagered", 0)
                    username = entry.get("username", "Unknown")
                    
                    # Apply username censoring for public repo
                    censored_username = username
                    if len(username) > 3:
                        censored_username = username[:-3] + "***"
                    else:
                        censored_username = "***"
                    
                    # Track highest wagerer
                    if wagered > highest_amount:
                        highest_amount = wagered
                        highest_wagerer = {
                            "username": censored_username,
                            "amount": wagered
                        }
                    
                    total_wagered += wagered
                    
                    processed_data.append({
                        "username": censored_username,
                        "user_id": entry.get("uid"),
                        "wagered": wagered,
                        "sessions": entry.get("sessions", 0),
                        "payout": entry.get("payout", 0),
                        "net": entry.get("net", 0)
                    })
                
                # Sort by wagered amount (descending)
                processed_data.sort(key=lambda x: x.get("wagered", 0), reverse=True)
                
                # Update summary
                wager_json["summary"][period_key]["total_wagered"] = total_wagered
                wager_json["summary"][period_key]["highest_wagerer"] = highest_wagerer
                
                return processed_data
            
            def process_weighted_wager_data(data, period_key):
                total_weighted_wagered = 0
                highest_weighted_wagerer = None
                highest_amount = 0
                processed_data = []
                
                for entry in data:
                    weighted_wagered = entry.get("weightedWagered", 0)
                    username = entry.get("username", "Unknown")
                    
                    # Apply username censoring for public repo
                    censored_username = username
                    if len(username) > 3:
                        censored_username = username[:-3] + "***"
                    else:
                        censored_username = "***"
                    
                    # Track highest weighted wagerer
                    if weighted_wagered > highest_amount:
                        highest_amount = weighted_wagered
                        highest_weighted_wagerer = {
                            "username": censored_username,
                            "amount": weighted_wagered
                        }
                    
                    total_weighted_wagered += weighted_wagered
                    
                    # Get highest multiplier data if available
                    highest_multi_data = entry.get("highestMultiplier", {})
                    
                    processed_data.append({
                        "username": censored_username,
                        "user_id": entry.get("uid"),
                        "weighted_wagered": weighted_wagered,
                        "sessions": entry.get("sessions", 0),
                        "highest_multiplier": {
                            "multiplier": highest_multi_data.get("multiplier", 0),
                            "game": highest_multi_data.get("gameTitle", "Unknown"),
                            "game_identifier": highest_multi_data.get("gameIdentifier"),
                            "wagered": highest_multi_data.get("wagered", 0),
                            "payout": highest_multi_data.get("payout", 0)
                        }
                    })
                
                # Sort by weighted wagered amount (descending)
                processed_data.sort(key=lambda x: x.get("weighted_wagered", 0), reverse=True)
                
                # Update summary
                wager_json["summary"][period_key]["total_weighted_wagered"] = total_weighted_wagered
                wager_json["summary"][period_key]["highest_weighted_wagerer"] = highest_weighted_wagerer
                
                return processed_data
            
            # Process lifetime data
            wager_json["data"]["lifetime"]["total_wager_data"] = process_total_wager_data(lifetime_total_wager, "lifetime")
            wager_json["data"]["lifetime"]["weighted_wager_data"] = process_weighted_wager_data(lifetime_weighted_wager, "lifetime")
            
            # Process current month data
            wager_json["data"]["current_month"]["total_wager_data"] = process_total_wager_data(month_total_wager, "current_month")
            wager_json["data"]["current_month"]["weighted_wager_data"] = process_weighted_wager_data(month_weighted_wager, "current_month")
            
            logger.info(f"[DataManager] Comprehensive wager data generated - Lifetime users: {len(lifetime_total_wager)}, Month users: {len(month_total_wager)}")
            logger.info(f"[DataManager] Lifetime totals - Wagered: ${wager_json['summary']['lifetime']['total_wagered']:,.2f}, Weighted: ${wager_json['summary']['lifetime']['total_weighted_wagered']:,.2f}")
            logger.info(f"[DataManager] Month totals - Wagered: ${wager_json['summary']['current_month']['total_wagered']:,.2f}, Weighted: ${wager_json['summary']['current_month']['total_weighted_wagered']:,.2f}")
            
            return wager_json
            
        except Exception as e:
            logger.error(f"Error generating comprehensive wager data JSON: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                "error": "Failed to generate comprehensive wager data",
                "message": str(e),
                "data_type": "comprehensive_wager_data",
                "last_updated": datetime.now(dt.UTC).isoformat()
            }
    
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
