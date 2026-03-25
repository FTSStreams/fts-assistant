import os
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from datetime import datetime
import datetime as dt

load_dotenv()

logger = logging.getLogger(__name__)

try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, os.getenv("DATABASE_URL"))
    if db_pool is None:
        raise psycopg2.Error("Failed to initialize database connection pool")
except psycopg2.Error as e:
    logger.critical(f"Failed to initialize database connection pool: {e}")
    raise

def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Failed to get DB connection: {e}")
        raise

def release_db_connection(conn):
    db_pool.putconn(conn)

def close_db_pool():
    db_pool.closeall()

def save_leaderboard_message_id(message_id, key="leaderboard_message_id"):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                (key, str(message_id), str(message_id))
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving leaderboard message ID: {e}")
    finally:
        release_db_connection(conn)

def get_leaderboard_message_id(key="leaderboard_message_id"):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", (key,))
            result = cur.fetchone()
            return int(result[0]) if result else None
    except Exception as e:
        logger.error(f"Error retrieving leaderboard message ID: {e}")
        return None
    finally:
        release_db_connection(conn)

def save_tip_log(user_id, username, amount, tip_type, month=None, year=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO manualtips (user_id, username, amount, tip_type, month, year) VALUES (%s, %s, %s, %s, %s, %s);",
                (user_id, username, amount, tip_type, month, year)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving tip log to database: {e}")
    finally:
        release_db_connection(conn)

def save_announced_goals(goals, year_month=None):
    # Ensure all goals are saved as integers (not strings)
    goals_int = set(int(g) for g in goals)
    if year_month is None:
        now = datetime.now()
        year_month = f"{now.year}_{now.month:02d}"
    key = f"announced_goals_{year_month}"
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                (key, ",".join(str(g) for g in goals_int), ",".join(str(g) for g in goals_int))
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving announced goals: {e}")
    finally:
        release_db_connection(conn)

def load_announced_goals(year_month=None):
    if year_month is None:
        now = datetime.now()
        year_month = f"{now.year}_{now.month:02d}"
    key = f"announced_goals_{year_month}"
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", (key,))
            result = cur.fetchone()
            if result and result[0]:
                return set(int(x) for x in result[0].split(",") if x)
            return set()
    except Exception as e:
        logger.error(f"Error loading announced goals: {e}")
        return set()
    finally:
        release_db_connection(conn)

def load_sent_tips(month, year):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, tier FROM milestonetips WHERE month = %s AND year = %s;", (month, year))
            tips = {(row[0], row[1]) for row in cur.fetchall()}
        return tips
    except Exception as e:
        logger.error(f"Error loading tips from database: {e}")
        return set()
    finally:
        release_db_connection(conn)

def save_tip(user_id, tier, month, year, tipped_at=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # First check if this tip already exists
            cur.execute(
                "SELECT COUNT(*) FROM milestonetips WHERE user_id = %s AND tier = %s AND month = %s AND year = %s;",
                (user_id, tier, month, year)
            )
            existing_count = cur.fetchone()[0]
            
            if existing_count > 0:
                logger.warning(f"Tip already exists for user {user_id}, tier {tier}, month {month}, year {year}")
                return False
            
            # Insert the new tip
            cur.execute(
                "INSERT INTO milestonetips (user_id, tier, month, year, tipped_at) VALUES (%s, %s, %s, %s, %s);",
                (user_id, tier, month, year, tipped_at or datetime.now())
            )
            conn.commit()
            logger.info(f"Successfully inserted tip: user_id={user_id}, tier={tier}, month={month}, year={year}")
            return True
    except Exception as e:
        logger.error(f"Error saving tip to database: {e}")
        logger.error(f"Failed tip details: user_id={user_id}, tier={tier}, month={month}, year={year}")
        return False
    finally:
        release_db_connection(conn)

def get_active_slot_challenge():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM active_slot_challenge LIMIT 1;")
            row = cur.fetchone()
            if row:
                return {
                    "id": row[0],
                    "game_identifier": row[1],
                    "game_name": row[2],
                    "required_multi": row[3],
                    "prize": row[4],
                    "start_time": row[5],
                    "posted_by": row[6],
                    "posted_by_username": row[7]
                }
            return None
    except Exception as e:
        logger.error(f"Error fetching active slot challenge: {e}")
        return None
    finally:
        release_db_connection(conn)

def set_active_slot_challenge(game_identifier, game_name, required_multi, prize, start_time, posted_by, posted_by_username):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM active_slot_challenge;")
            cur.execute(
                "INSERT INTO active_slot_challenge (game_identifier, game_name, required_multi, prize, start_time, posted_by, posted_by_username) VALUES (%s, %s, %s, %s, %s, %s, %s);",
                (game_identifier, game_name, required_multi, prize, start_time, posted_by, posted_by_username)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting active slot challenge: {e}")
    finally:
        release_db_connection(conn)

def clear_active_slot_challenge():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM active_slot_challenge;")
            conn.commit()
    except Exception as e:
        logger.error(f"Error clearing active slot challenge: {e}")
    finally:
        release_db_connection(conn)

def log_slot_challenge(challenge_id, game, game_identifier, winner_uid, winner_username, multiplier, bet, payout, required_multiplier, prize, min_bet, challenge_start):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO slot_challenge_logs (
                    challenge_id, game, game_identifier, winner_uid, winner_username, multiplier, bet, payout, required_multiplier, prize, min_bet, challenge_start
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (challenge_id, game, game_identifier, winner_uid, winner_username, multiplier, bet, payout, required_multiplier, prize, min_bet, challenge_start)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error logging slot challenge: {e}")
    finally:
        release_db_connection(conn)

def get_all_active_slot_challenges():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM active_slot_challenge ORDER BY start_time ASC;")
            rows = cur.fetchall()
            return [
                {
                    "challenge_id": row[0],
                    "game_identifier": row[1],
                    "game_name": row[2],
                    "required_multi": row[3],
                    "prize": row[4],
                    "start_time": row[5],
                    "posted_by": row[6],
                    "posted_by_username": row[7],
                    "emoji": row[8] if len(row) > 8 else None,
                    "min_bet": row[9] if len(row) > 9 else None
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching all active slot challenges: {e}")
        return []
    finally:
        release_db_connection(conn)

def add_active_slot_challenge(game_identifier, game_name, required_multi, prize, start_time, posted_by, posted_by_username, message_id=None, emoji=None, min_bet=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Remove message_id from INSERT since that column doesn't exist in the table anymore
            cur.execute(
                "INSERT INTO active_slot_challenge (game_identifier, game_name, required_multi, prize, start_time, posted_by, posted_by_username, emoji, min_bet) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING challenge_id;",
                (game_identifier, game_name, required_multi, prize, start_time, posted_by, posted_by_username, emoji, min_bet)
            )
            challenge_id = cur.fetchone()[0]
            conn.commit()
            return challenge_id
    except Exception as e:
        logger.error(f"Error adding active slot challenge: {e}")
        return None
    finally:
        release_db_connection(conn)

def remove_active_slot_challenge(challenge_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM active_slot_challenge WHERE challenge_id = %s;", (challenge_id,))
            conn.commit()
    except Exception as e:
        logger.error(f"Error removing active slot challenge: {e}")
    finally:
        release_db_connection(conn)

def update_challenge_message_id(challenge_id, message_id):
    # This function is deprecated - message IDs are now tracked in settings table
    # Use save_leaderboard_message_id() with appropriate key instead
    logger.warning("update_challenge_message_id() is deprecated - message IDs now tracked in settings table")
    pass

def get_all_completed_slot_challenges():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT challenge_id, game, game_identifier, winner_uid, winner_username, multiplier, bet, payout, required_multiplier, prize, min_bet, challenge_start
                FROM slot_challenge_logs
                ORDER BY challenge_start DESC;
            """)
            rows = cur.fetchall()
            return [
                {
                    "challenge_id": row[0],
                    "game": row[1],
                    "game_identifier": row[2],
                    "winner_uid": row[3],
                    "winner_username": row[4],
                    "multiplier": row[5],
                    "bet": row[6],
                    "payout": row[7],
                    "required_multiplier": row[8],
                    "prize": row[9],
                    "min_bet": row[10],
                    "challenge_start": row[11],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching completed slot challenges: {e}")
        return []
    finally:
        release_db_connection(conn)

def save_monthly_totals(year, month, total_wager, weighted_wager):
    """Save monthly totals for the month-to-month chart"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO monthly_totals (year, month, total_wager, total_weighted_wager, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (year, month) 
                DO UPDATE SET 
                    total_wager = EXCLUDED.total_wager,
                    total_weighted_wager = EXCLUDED.total_weighted_wager,
                    created_at = EXCLUDED.created_at;
                """,
                (year, month, total_wager, weighted_wager, datetime.now(dt.UTC))
            )
            conn.commit()
            logger.info(f"Saved monthly totals for {year}-{month:02d}: Total=${total_wager:,.2f}, Weighted=${weighted_wager:,.2f}")
    except Exception as e:
        logger.error(f"Error saving monthly totals: {e}")
    finally:
        release_db_connection(conn)

def get_monthly_totals():
    """Get all stored monthly totals for the chart"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT year, month, total_wager, total_weighted_wager, created_at 
                FROM monthly_totals 
                ORDER BY year, month;
                """
            )
            results = cur.fetchall()
            
            monthly_data = []
            for year, month, total_wager, weighted_wager, created_at in results:
                monthly_data.append({
                    'year': year,
                    'month': month,
                    'total_wager': float(total_wager) if total_wager else 0.0,
                    'weighted_wager': float(weighted_wager) if weighted_wager else 0.0,
                    'created_at': created_at
                })
            
            return monthly_data
    except Exception as e:
        logger.error(f"Error loading monthly totals: {e}")
        return []
    finally:
        release_db_connection(conn)

def backfill_monthly_totals_for_date(year, month, total_wager, weighted_wager):
    """Backfill monthly totals for a specific year/month"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if data already exists
            cur.execute(
                "SELECT COUNT(*) FROM monthly_totals WHERE year = %s AND month = %s",
                (year, month)
            )
            exists = cur.fetchone()[0] > 0
            
            if not exists:
                cur.execute(
                    """
                    INSERT INTO monthly_totals (year, month, total_wager, total_weighted_wager, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (year, month, total_wager, weighted_wager, datetime.now(dt.UTC))
                )
                conn.commit()
                logger.info(f"Backfilled monthly totals for {year}-{month:02d}: Total=${total_wager:,.2f}, Weighted=${weighted_wager:,.2f}")
                return True
            else:
                logger.info(f"Monthly totals for {year}-{month:02d} already exist, skipping backfill")
                return False
    except Exception as e:
        logger.error(f"Error backfilling monthly totals for {year}-{month:02d}: {e}")
        return False
    finally:
        release_db_connection(conn)


# ─── Roo Vs Flip ──────────────────────────────────────────────────────────────

def ensure_roovsflip_tables():
    """Create Roo Vs Flip tables if they don't exist."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS roovsflip_queue (
                    position INTEGER PRIMARY KEY,
                    game_name TEXT NOT NULL,
                    game_identifier TEXT NOT NULL,
                    emoji TEXT DEFAULT '🎮',
                    req_multi FLOAT NOT NULL,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS roovsflip_queue_draft (
                    position INTEGER PRIMARY KEY,
                    game_name TEXT NOT NULL,
                    game_identifier TEXT NOT NULL,
                    emoji TEXT DEFAULT '🎮',
                    req_multi FLOAT NOT NULL,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS roovsflip_payouts (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    winner_uid TEXT NOT NULL,
                    winner_username TEXT NOT NULL,
                    prize_amount FLOAT NOT NULL,
                    paid_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE(year, month, winner_uid)
                );
            """)
            conn.commit()
            logger.info("[RooVsFlip] Tables ensured.")
    except Exception as e:
        logger.error(f"Error ensuring Roo Vs Flip tables: {e}")
    finally:
        release_db_connection(conn)


def get_roovsflip_queue():
    """Return all queued games ordered by position."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT position, game_name, game_identifier, emoji, req_multi, added_at "
                "FROM roovsflip_queue ORDER BY position ASC;"
            )
            rows = cur.fetchall()
            return [
                {
                    "position": row[0],
                    "game_name": row[1],
                    "game_identifier": row[2],
                    "emoji": row[3] or "🎮",
                    "req_multi": float(row[4]),
                    "added_at": row[5],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching Roo Vs Flip queue: {e}")
        return []
    finally:
        release_db_connection(conn)


def set_roovsflip_queue_slot(position, game_name, game_identifier, emoji, req_multi):
    """Set or overwrite a specific queue slot."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO roovsflip_queue (position, game_name, game_identifier, emoji, req_multi, added_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (position) DO UPDATE SET
                    game_name = EXCLUDED.game_name,
                    game_identifier = EXCLUDED.game_identifier,
                    emoji = EXCLUDED.emoji,
                    req_multi = EXCLUDED.req_multi,
                    added_at = EXCLUDED.added_at;
                """,
                (position, game_name, game_identifier, emoji, req_multi),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting Roo Vs Flip queue slot: {e}")
    finally:
        release_db_connection(conn)


def clear_roovsflip_queue_slot(position=None):
    """Remove a single slot (by position) or clear the entire queue."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if position is not None:
                cur.execute("DELETE FROM roovsflip_queue WHERE position = %s;", (position,))
            else:
                cur.execute("DELETE FROM roovsflip_queue;")
            conn.commit()
    except Exception as e:
        logger.error(f"Error clearing Roo Vs Flip queue slot: {e}")
    finally:
        release_db_connection(conn)


def is_roovsflip_paid(year, month):
    """Return True if any payout record exists for the given year/month."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM roovsflip_payouts WHERE year = %s AND month = %s;",
                (year, month),
            )
            result = cur.fetchone()
            return (result[0] > 0) if result else False
    except Exception as e:
        logger.error(f"Error checking Roo Vs Flip payout: {e}")
        return False
    finally:
        release_db_connection(conn)


def record_roovsflip_payout(year, month, winner_uid, winner_username, prize_amount):
    """Record a single winner payout (or a sentinel 'NO_WINNERS' row)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO roovsflip_payouts (year, month, winner_uid, winner_username, prize_amount)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (year, month, winner_uid) DO NOTHING;
                """,
                (year, month, winner_uid, winner_username, prize_amount),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error recording Roo Vs Flip payout: {e}")
    finally:
        release_db_connection(conn)


def get_roovsflip_event_start():
    """
    Return the ISO timestamp of when the current event started.
    Defaults to the 1st of the current UTC month if no value is stored.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = 'roovsflip_event_start';")
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except Exception as e:
        logger.error(f"Error getting Roo Vs Flip event start: {e}")
    finally:
        release_db_connection(conn)
    # Default: midnight on 1st of current UTC month
    now = datetime.now(dt.UTC)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()


def set_roovsflip_event_start(iso_str):
    """Persist a new event start timestamp to the settings table."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES ('roovsflip_event_start', %s) "
                "ON CONFLICT (key) DO UPDATE SET value = %s;",
                (iso_str, iso_str),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting Roo Vs Flip event start: {e}")
    finally:
        release_db_connection(conn)


def get_roovsflip_draft_queue():
    """Return all queued games for NEXT month (draft queue) ordered by position."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT position, game_name, game_identifier, emoji, req_multi, added_at "
                "FROM roovsflip_queue_draft ORDER BY position ASC;"
            )
            rows = cur.fetchall()
            return [
                {
                    "position": row[0],
                    "game_name": row[1],
                    "game_identifier": row[2],
                    "emoji": row[3] or "🎮",
                    "req_multi": float(row[4]),
                    "added_at": row[5],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Error fetching Roo Vs Flip draft queue: {e}")
        return []
    finally:
        release_db_connection(conn)


def set_roovsflip_draft_queue_slot(position, game_name, game_identifier, emoji, req_multi):
    """Set or overwrite a specific draft queue slot."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO roovsflip_queue_draft (position, game_name, game_identifier, emoji, req_multi, added_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (position) DO UPDATE SET
                    game_name = EXCLUDED.game_name,
                    game_identifier = EXCLUDED.game_identifier,
                    emoji = EXCLUDED.emoji,
                    req_multi = EXCLUDED.req_multi,
                    added_at = EXCLUDED.added_at;
                """,
                (position, game_name, game_identifier, emoji, req_multi),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting Roo Vs Flip draft queue slot: {e}")
    finally:
        release_db_connection(conn)


def clear_roovsflip_draft_queue_slot(position=None):
    """Remove a single draft slot (by position) or clear the entire draft queue."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if position is not None:
                cur.execute("DELETE FROM roovsflip_queue_draft WHERE position = %s;", (position,))
            else:
                cur.execute("DELETE FROM roovsflip_queue_draft;")
            conn.commit()
    except Exception as e:
        logger.error(f"Error clearing Roo Vs Flip draft queue slot: {e}")
    finally:
        release_db_connection(conn)


def copy_roovsflip_draft_to_active():
    """Copy draft queue to active queue at monthly rollover, then clear draft."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Clear active queue first
            cur.execute("DELETE FROM roovsflip_queue;")
            # Copy all draft slots to active
            cur.execute(
                """
                INSERT INTO roovsflip_queue (position, game_name, game_identifier, emoji, req_multi, added_at)
                SELECT position, game_name, game_identifier, emoji, req_multi, NOW() FROM roovsflip_queue_draft;
                """
            )
            conn.commit()
            logger.info("[RooVsFlip] Draft queue copied to active.")
    except Exception as e:
        logger.error(f"Error copying draft queue to active: {e}")
    finally:
        release_db_connection(conn)
