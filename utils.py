import requests
import logging
from datetime import datetime
import datetime as dt
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import os
import time
import aiohttp

ROOBET_API_TOKEN = os.getenv("ROOBET_API_TOKEN")
TIPPING_API_TOKEN = os.getenv("TIPPING_API_TOKEN")
ROOBET_USER_ID = os.getenv("ROOBET_USER_ID")
AFFILIATE_API_URL = "https://roobetconnect.com/affiliate/v2/stats"
TIPPING_API_URL = "https://roobet.com/_api/tipping/send"

logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_total_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.now(dt.UTC).isoformat(),
    }
    try:
        response = requests.get(AFFILIATE_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        data = data.get("data", []) if isinstance(data, dict) else data
        if not isinstance(data, list):
            logger.warning(f"Unexpected total wager response format: {data}")
            return []
        logger.info(f"Total Wager API Response: {len(data)} entries")
        return data
    except requests.RequestException as e:
        logger.error(f"Total Wager API Request Failed: {e}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing Total Wager JSON response: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_weighted_wager(start_date, end_date):
    headers = {"Authorization": f"Bearer {ROOBET_API_TOKEN}"}
    params = {
        "userId": ROOBET_USER_ID,
        "startDate": start_date,
        "endDate": end_date,
        "timestamp": datetime.now(dt.UTC).isoformat(),
        "categories": "slots,provably fair",
        "gameIdentifiers": "-housegames:dice"
    }
    try:
        response = requests.get(AFFILIATE_API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        data = data.get("data", []) if isinstance(data, dict) else data
        if not isinstance(data, list):
            logger.warning(f"Unexpected weighted wager response format: {data}")
            return []
        logger.info(f"Weighted Wager API Response: {len(data)} entries")
        return data
    except requests.RequestException as e:
        logger.error(f"Weighted Wager API Request Failed: {e}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing Weighted Wager JSON response: {e}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception(lambda e: isinstance(e, requests.HTTPError) and e.response.status_code == 429)
)
async def send_tip(user_id, to_username, to_user_id, amount, show_in_chat=True, balance_type="crypto"):
    headers = {"Authorization": f"Bearer {TIPPING_API_TOKEN}"}
    nonce = str(int(time.time() * 1000))  # Use current timestamp in ms as a simple nonce
    payload = {
        "userId": user_id,
        "toUserName": to_username,
        "toUserId": to_user_id,
        "amount": amount,
        "showInChat": show_in_chat,
        "balanceType": balance_type,
        "nonce": nonce
    }
    logger.debug(f"Sending tip request for {to_username}: Payload={payload}")
    logger.debug(f"[DEBUG] Tip payload for {to_username}: {payload}")
    logger.debug(f"[DEBUG] Tip headers: {headers}")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(TIPPING_API_URL, json=payload, headers=headers, timeout=10) as response:
                if response.status == 200:
                    logger.info(f"Tip sent to {to_username}: ${amount}")
                    return await response.json()
                else:
                    try:
                        error_response = await response.json()
                        logger.error(f"Tipping API Request Failed for {to_username}: {response.status}, Response: {error_response}")
                    except Exception:
                        logger.error(f"Tipping API Request Failed for {to_username}: {response.status}")
                    return {"success": False, "message": f"HTTP {response.status}"}
        except Exception as e:
            logger.error(f"Exception in send_tip for {to_username}: {e}")
            return {"success": False, "message": str(e)}

def get_current_month_range():
    now = datetime.now(dt.UTC)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now.month == 12:
        end = start.replace(year=now.year + 1, month=1) - dt.timedelta(seconds=1)
    else:
        end = start.replace(month=now.month + 1) - dt.timedelta(seconds=1)
    return start.isoformat(), end.isoformat()
