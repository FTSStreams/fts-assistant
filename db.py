import os
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from datetime import datetime
import datetime as dt
from decimal import Decimal, ROUND_DOWN
import uuid
import secrets

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

def save_setting_value(key, value):
    """Save a generic string setting value."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                (key, str(value), str(value))
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving setting '{key}': {e}")
    finally:
        release_db_connection(conn)

def get_setting_value(key, default=None):
    """Load a generic string setting value."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", (key,))
            result = cur.fetchone()
            if result and result[0] is not None:
                return result[0]
            return default
    except Exception as e:
        logger.error(f"Error loading setting '{key}': {e}")
        return default
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

def get_user_slot_challenge_stats(user_id, month=None, year=None):
    """Return all-time and optional month-specific slot challenge completion stats for one user."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS completed_count,
                    COALESCE(SUM(prize), 0) AS total_earned
                FROM slot_challenge_logs
                WHERE winner_uid = %s
                  AND multiplier IS NOT NULL;
                """,
                (str(user_id),)
            )
            all_time_row = cur.fetchone() or (0, 0)

            current_month_completed = 0
            current_month_earned = 0.0
            if month is not None and year is not None:
                month_start = datetime(year, month, 1, 0, 0, 0, tzinfo=dt.UTC)
                if month == 12:
                    month_end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=dt.UTC)
                else:
                    month_end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=dt.UTC)

                cur.execute(
                    """
                    SELECT COUNT(*), COALESCE(SUM(prize), 0)
                    FROM slot_challenge_logs
                    WHERE winner_uid = %s
                      AND multiplier IS NOT NULL
                      AND challenge_start >= %s
                      AND challenge_start < %s;
                    """,
                    (str(user_id), month_start, month_end)
                )
                month_row = cur.fetchone() or (0, 0)
                current_month_completed = int(month_row[0] or 0)
                current_month_earned = float(month_row[1] or 0)

            return {
                "completed_all_time": int(all_time_row[0] or 0),
                "completed_current_month": int(current_month_completed),
                "earned_all_time": float(all_time_row[1] or 0),
                "earned_current_month": float(current_month_earned),
            }
    except Exception as e:
        logger.error(f"Error fetching slot challenge stats for user {user_id}: {e}")
        return {
            "completed_all_time": 0,
            "completed_current_month": 0,
            "earned_all_time": 0.0,
            "earned_current_month": 0.0,
        }
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
            cur.execute(
                "ALTER TABLE roovsflip_queue "
                "ADD COLUMN IF NOT EXISTS emoji TEXT DEFAULT '🎮';"
            )
            cur.execute(
                "ALTER TABLE roovsflip_queue_draft "
                "ADD COLUMN IF NOT EXISTS emoji TEXT DEFAULT '🎮';"
            )
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


def _normalize_roovsflip_positions(table_name):
    """Reindex queue positions to be contiguous starting from 1."""
    if table_name not in ("roovsflip_queue", "roovsflip_queue_draft"):
        raise ValueError("Invalid Roo Vs Flip table name")

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Shift away from the PK range first to avoid transient conflicts.
            cur.execute(f"UPDATE {table_name} SET position = position + 1000;")
            cur.execute(
                f"""
                WITH ordered AS (
                    SELECT position, ROW_NUMBER() OVER (ORDER BY position ASC, added_at ASC) AS new_position
                    FROM {table_name}
                )
                UPDATE {table_name} t
                SET position = ordered.new_position
                FROM ordered
                WHERE t.position = ordered.position;
                """
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error normalizing Roo Vs Flip positions for {table_name}: {e}")
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
    _normalize_roovsflip_positions("roovsflip_queue")


def clear_roovsflip_queue_slot(position=None):
    """Remove a single slot (by position) or clear the entire queue."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if position is not None:
                cur.execute("DELETE FROM roovsflip_queue WHERE position = %s;", (position,))
                cur.execute("UPDATE roovsflip_queue SET position = position - 1 WHERE position > %s;", (position,))
            else:
                cur.execute("DELETE FROM roovsflip_queue;")
            conn.commit()
    except Exception as e:
        logger.error(f"Error clearing Roo Vs Flip queue slot: {e}")
    finally:
        release_db_connection(conn)
    _normalize_roovsflip_positions("roovsflip_queue")


def swap_roovsflip_queue_positions(position_1, position_2):
    """Swap two active queue positions. Returns (success, message)."""
    if position_1 == position_2:
        return False, "Positions must be different."

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT position FROM roovsflip_queue WHERE position IN (%s, %s);",
                (position_1, position_2),
            )
            found_positions = {row[0] for row in cur.fetchall()}

            if position_1 not in found_positions and position_2 not in found_positions: 
                return False, "Neither position currently has a game to swap."
            if position_1 not in found_positions:
                return False, f"Position {position_1} is empty."
            if position_2 not in found_positions:
                return False, f"Position {position_2} is empty."

            temp_position = -9999
            cur.execute(
                "UPDATE roovsflip_queue SET position = %s WHERE position = %s;",
                (temp_position, position_1),
            )
            cur.execute(
                "UPDATE roovsflip_queue SET position = %s WHERE position = %s;",
                (position_1, position_2),
            )
            cur.execute(
                "UPDATE roovsflip_queue SET position = %s WHERE position = %s;",
                (position_2, temp_position),
            )
            conn.commit()
            return True, "Swap completed."
    except Exception as e:
        logger.error(f"Error swapping Roo Vs Flip queue positions: {e}")
        return False, "Database error while swapping positions."
    finally:
        release_db_connection(conn)


def is_roovsflip_paid(year, month):
    """Return True only when the month has been fully finalized."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM roovsflip_payouts
                WHERE year = %s AND month = %s AND winner_uid = 'PAID_COMPLETE';
                """,
                (year, month),
            )
            result = cur.fetchone()
            return (result[0] > 0) if result else False
    except Exception as e:
        logger.error(f"Error checking Roo Vs Flip payout: {e}")
        return False
    finally:
        release_db_connection(conn)


def is_roovsflip_winner_paid(year, month, winner_uid):
    """Return True if this winner already has a payout row for the month."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM roovsflip_payouts
                WHERE year = %s AND month = %s AND winner_uid = %s;
                """,
                (year, month, winner_uid),
            )
            result = cur.fetchone()
            return (result[0] > 0) if result else False
    except Exception as e:
        logger.error(f"Error checking Roo Vs Flip winner payout: {e}")
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
    If no value is stored, initialize it to the 1st of the current UTC month.
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

    # Missing setting: initialize once so future month-boundary checks are stable.
    now = datetime.now(dt.UTC)
    default_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    logger.warning(
        "[RooVsFlip] Missing roovsflip_event_start in settings; "
        f"initializing to {default_start}."
    )
    set_roovsflip_event_start(default_start)
    return default_start


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
    _normalize_roovsflip_positions("roovsflip_queue_draft")


def clear_roovsflip_draft_queue_slot(position=None):
    """Remove a single draft slot (by position) or clear the entire draft queue."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if position is not None:
                cur.execute("DELETE FROM roovsflip_queue_draft WHERE position = %s;", (position,))
                cur.execute("UPDATE roovsflip_queue_draft SET position = position - 1 WHERE position > %s;", (position,))
            else:
                cur.execute("DELETE FROM roovsflip_queue_draft;")
            conn.commit()
    except Exception as e:
        logger.error(f"Error clearing Roo Vs Flip draft queue slot: {e}")
    finally:
        release_db_connection(conn)
    _normalize_roovsflip_positions("roovsflip_queue_draft")


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
    _normalize_roovsflip_positions("roovsflip_queue")


def _ensure_checkin_tables(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_checkins (
            discord_user_id BIGINT PRIMARY KEY,
            streak_days INTEGER NOT NULL DEFAULT 0,
            balance NUMERIC(12, 2) NOT NULL DEFAULT 0,
            last_checkin_date DATE,
            withdrawal_hold_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            withdrawal_hold_created_at TIMESTAMPTZ,
            total_earned NUMERIC(12, 2) NOT NULL DEFAULT 0,
            total_withdrawn NUMERIC(12, 2) NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_checkins (
            id BIGSERIAL PRIMARY KEY,
            discord_user_id BIGINT NOT NULL,
            checkin_date DATE NOT NULL,
            streak_days INTEGER NOT NULL,
            reward_amount NUMERIC(12, 2) NOT NULL,
            balance_after NUMERIC(12, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(discord_user_id, checkin_date)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checkin_withdrawals (
            withdrawal_id UUID PRIMARY KEY,
            discord_user_id BIGINT NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            status TEXT NOT NULL,
            roobet_uid TEXT,
            roobet_username TEXT,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checkin_coinflip_logs (
            id BIGSERIAL PRIMARY KEY,
            discord_user_id BIGINT NOT NULL,
            wager_amount NUMERIC(12, 2) NOT NULL,
            player_choice TEXT NOT NULL,
            outcome TEXT NOT NULL,
            payout_multiplier NUMERIC(6, 3) NOT NULL,
            payout_amount NUMERIC(12, 2) NOT NULL,
            net_amount NUMERIC(12, 2) NOT NULL,
            balance_before NUMERIC(12, 2) NOT NULL,
            balance_after NUMERIC(12, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checkin_random_drops (
            id BIGSERIAL PRIMARY KEY,
            drop_date DATE NOT NULL UNIQUE,
            scheduled_for TIMESTAMPTZ NOT NULL,
            reward_amount NUMERIC(12, 2) NOT NULL DEFAULT 1.50,
            max_claims INTEGER NOT NULL DEFAULT 3,
            status TEXT NOT NULL DEFAULT 'scheduled',
            message_channel_id BIGINT,
            message_id BIGINT,
            posted_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS checkin_random_drop_claims (
            id BIGSERIAL PRIMARY KEY,
            drop_id BIGINT NOT NULL REFERENCES checkin_random_drops(id) ON DELETE CASCADE,
            discord_user_id BIGINT NOT NULL,
            claimed_amount NUMERIC(12, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(drop_id, discord_user_id)
        );
        """
    )


def _get_checkin_random_drop_claims(cur, drop_id):
    cur.execute(
        """
        SELECT id, discord_user_id, claimed_amount, created_at
        FROM checkin_random_drop_claims
        WHERE drop_id = %s
        ORDER BY created_at ASC, id ASC;
        """,
        (int(drop_id),),
    )
    claims = []
    for claim_id, discord_user_id, claimed_amount, created_at in cur.fetchall():
        claims.append(
            {
                "id": int(claim_id),
                "discord_user_id": int(discord_user_id),
                "claimed_amount": float(Decimal(claimed_amount or 0)),
                "created_at": created_at,
            }
        )
    return claims


def _split_random_drop_pool(total_amount_dec, participant_count):
    if participant_count <= 0:
        return []

    total_cents = int((total_amount_dec * Decimal("100")).to_integral_value(rounding=ROUND_DOWN))
    base_cents = total_cents // participant_count
    remainder_cents = total_cents % participant_count

    payouts = []
    for index in range(participant_count):
        cents = base_cents + (1 if index < remainder_cents else 0)
        payouts.append((Decimal(cents) / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_DOWN))
    return payouts


def _settle_checkin_random_drop(cur, drop, now_utc):
    claims = list(drop.get("claims", []))
    drop_id = int(drop["id"])

    if not claims:
        cur.execute(
            """
            UPDATE checkin_random_drops
            SET
                status = 'expired',
                completed_at = COALESCE(completed_at, %s),
                updated_at = NOW()
            WHERE id = %s;
            """,
            (now_utc, drop_id),
        )
    else:
        pool_amount = Decimal(str(drop.get("reward_amount", 0))).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        payouts = _split_random_drop_pool(pool_amount, len(claims))

        for claim, payout_amount in zip(claims, payouts):
            cur.execute(
                """
                UPDATE checkin_random_drop_claims
                SET claimed_amount = %s
                WHERE id = %s;
                """,
                (payout_amount, int(claim["id"])),
            )

            row = _get_or_create_checkin_row(cur, int(claim["discord_user_id"]))
            current_balance = Decimal(row[1] or 0)
            total_earned = Decimal(row[5] or 0)
            new_balance = (current_balance + payout_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            new_total_earned = (total_earned + payout_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    balance = %s,
                    total_earned = %s,
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (new_balance, new_total_earned, str(claim["discord_user_id"])),
            )

        cur.execute(
            """
            UPDATE checkin_random_drops
            SET
                status = 'completed',
                completed_at = COALESCE(completed_at, %s),
                updated_at = NOW()
            WHERE id = %s;
            """,
            (now_utc, drop_id),
        )

    cur.execute(
        """
        SELECT
            id,
            drop_date,
            scheduled_for,
            reward_amount,
            max_claims,
            status,
            message_channel_id,
            message_id,
            posted_at,
            completed_at,
            created_at,
            updated_at
        FROM checkin_random_drops
        WHERE id = %s;
        """,
        (drop_id,),
    )
    return _serialize_checkin_random_drop(cur, cur.fetchone())


def _serialize_checkin_random_drop(cur, row):
    if not row:
        return None

    drop = {
        "id": int(row[0]),
        "drop_date": row[1],
        "scheduled_for": row[2],
        "reward_amount": float(Decimal(row[3] or 0)),
        "max_claims": int(row[4] or 0),
        "status": row[5],
        "message_channel_id": int(row[6]) if row[6] is not None else None,
        "message_id": int(row[7]) if row[7] is not None else None,
        "posted_at": row[8],
        "completed_at": row[9],
        "created_at": row[10],
        "updated_at": row[11],
    }
    drop["claims"] = _get_checkin_random_drop_claims(cur, drop["id"])
    drop["claims_count"] = len(drop["claims"])
    return drop


def get_or_create_daily_checkin_random_drop(now=None, reward_amount=1.50, max_claims=3):
    now_utc = now or datetime.now(dt.UTC)
    today = now_utc.date()
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            cur.execute(
                """
                SELECT
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM checkin_random_drops
                WHERE drop_date = %s
                FOR UPDATE;
                """,
                (today,),
            )
            row = cur.fetchone()
            if row:
                drop = _serialize_checkin_random_drop(cur, row)
                conn.commit()
                return drop

            scheduled_seconds = secrets.randbelow(24 * 60 * 60)
            scheduled_for = datetime.combine(today, dt.time.min, tzinfo=dt.UTC) + dt.timedelta(seconds=scheduled_seconds)
            reward_dec = Decimal(str(reward_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            cur.execute(
                """
                INSERT INTO checkin_random_drops (
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status
                )
                VALUES (%s, %s, %s, %s, 'scheduled')
                RETURNING
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at;
                """,
                (today, scheduled_for, reward_dec, int(max_claims)),
            )
            row = cur.fetchone()
            drop = _serialize_checkin_random_drop(cur, row)
            conn.commit()
            return drop
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating daily check-in random drop for {today}: {e}")
        return None
    finally:
        try:
            conn.autocommit = True
        except Exception:
            conn.rollback()
            conn.autocommit = True
        release_db_connection(conn)


def get_checkin_random_drop_by_message(message_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            cur.execute(
                """
                SELECT
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM checkin_random_drops
                WHERE message_id = %s;
                """,
                (str(message_id),),
            )
            row = cur.fetchone()
            return _serialize_checkin_random_drop(cur, row)
    except Exception as e:
        logger.error(f"Error loading check-in random drop for message {message_id}: {e}")
        return None
    finally:
        release_db_connection(conn)


def mark_checkin_random_drop_posted(drop_id, channel_id, message_id):
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            cur.execute(
                """
                UPDATE checkin_random_drops
                SET
                    status = 'active',
                    message_channel_id = %s,
                    message_id = %s,
                    posted_at = COALESCE(posted_at, NOW()),
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at;
                """,
                (str(channel_id), str(message_id), int(drop_id)),
            )
            row = cur.fetchone()
            drop = _serialize_checkin_random_drop(cur, row)
            conn.commit()
            return drop
    except Exception as e:
        conn.rollback()
        logger.error(f"Error marking check-in random drop {drop_id} as posted: {e}")
        return None
    finally:
        try:
            conn.autocommit = True
        except Exception:
            conn.rollback()
            conn.autocommit = True
        release_db_connection(conn)


def expire_stale_checkin_random_drops(now=None, expiry_minutes=3):
    now_utc = now or datetime.now(dt.UTC)
    today = now_utc.date()
    expiry_delta = dt.timedelta(minutes=max(1, int(expiry_minutes)))
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            cur.execute(
                """
                SELECT
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM checkin_random_drops
                WHERE status IN ('scheduled', 'active')
                FOR UPDATE;
                """,
            )
            rows = cur.fetchall()
            expired_drops = []
            for row in rows:
                expired_drop = _serialize_checkin_random_drop(cur, row)
                should_close = False

                if expired_drop["drop_date"] < today:
                    should_close = True
                elif expired_drop["status"] == "active":
                    posted_at = expired_drop.get("posted_at")
                    if posted_at is not None and now_utc >= (posted_at + expiry_delta):
                        should_close = True

                if not should_close:
                    continue

                if expired_drop["status"] == "active":
                    closed_drop = _settle_checkin_random_drop(cur, expired_drop, now_utc)
                    expired_drops.append(closed_drop)
                    continue

                cur.execute(
                    """
                    UPDATE checkin_random_drops
                    SET
                        status = 'expired',
                        completed_at = COALESCE(completed_at, NOW()),
                        updated_at = NOW()
                    WHERE id = %s;
                    """,
                    (expired_drop["id"],),
                )
                expired_drop["status"] = "expired"
                if expired_drop["completed_at"] is None:
                    expired_drop["completed_at"] = now_utc
                expired_drops.append(expired_drop)
            conn.commit()
            return expired_drops
    except Exception as e:
        conn.rollback()
        logger.error(f"Error expiring stale check-in random drops before {today}: {e}")
        return []
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def process_checkin_random_drop_claim(message_id, discord_user_id, now=None, expiry_minutes=3):
    now_utc = now or datetime.now(dt.UTC)
    expiry_delta = dt.timedelta(minutes=max(1, int(expiry_minutes)))
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            cur.execute(
                """
                SELECT
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM checkin_random_drops
                WHERE message_id = %s
                FOR UPDATE;
                """,
                (str(message_id),),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return {"status": "not_found"}

            drop = _serialize_checkin_random_drop(cur, row)
            if drop["status"] != "active":
                conn.commit()
                return {"status": drop["status"], "drop": drop}

            posted_at = drop.get("posted_at")
            if posted_at is not None and now_utc >= (posted_at + expiry_delta):
                closed_drop = _settle_checkin_random_drop(cur, drop, now_utc)
                conn.commit()
                return {"status": closed_drop.get("status", "expired"), "drop": closed_drop}

            cur.execute(
                """
                SELECT 1
                FROM checkin_random_drop_claims
                WHERE drop_id = %s AND discord_user_id = %s;
                """,
                (drop["id"], str(discord_user_id)),
            )
            if cur.fetchone():
                conn.commit()
                return {"status": "already_claimed", "drop": drop}

            cur.execute(
                """
                INSERT INTO checkin_random_drop_claims (drop_id, discord_user_id, claimed_amount)
                VALUES (%s, %s, %s);
                """,
                (drop["id"], str(discord_user_id), Decimal("0.00")),
            )

            refreshed_claims = _get_checkin_random_drop_claims(cur, drop["id"])
            new_claim_count = len(refreshed_claims)
            pool_amount = Decimal(str(drop.get("reward_amount", 0))).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            estimated_share = Decimal("0.00")
            if new_claim_count > 0:
                estimated_share = (pool_amount / Decimal(new_claim_count)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            cur.execute(
                """
                SELECT
                    id,
                    drop_date,
                    scheduled_for,
                    reward_amount,
                    max_claims,
                    status,
                    message_channel_id,
                    message_id,
                    posted_at,
                    completed_at,
                    created_at,
                    updated_at
                FROM checkin_random_drops
                WHERE id = %s;
                """,
                (drop["id"],),
            )
            updated_drop = _serialize_checkin_random_drop(cur, cur.fetchone())
            conn.commit()
            return {
                "status": "claimed",
                "claim_position": new_claim_count,
                "estimated_share": float(estimated_share),
                "pool_amount": float(pool_amount),
                "drop": updated_drop,
                "completed": False,
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing check-in random drop claim for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def _get_or_create_checkin_row(cur, discord_user_id):
    cur.execute(
        """
        INSERT INTO user_checkins (discord_user_id)
        VALUES (%s)
        ON CONFLICT (discord_user_id) DO NOTHING;
        """,
        (str(discord_user_id),),
    )
    cur.execute(
        """
        SELECT
            streak_days,
            balance,
            last_checkin_date,
            withdrawal_hold_amount,
            withdrawal_hold_created_at,
            total_earned,
            total_withdrawn
        FROM user_checkins
        WHERE discord_user_id = %s
        FOR UPDATE;
        """,
        (str(discord_user_id),),
    )
    return cur.fetchone()


def process_daily_checkin(discord_user_id):
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            row = _get_or_create_checkin_row(cur, discord_user_id)

            streak_days = int(row[0] or 0)
            balance = Decimal(row[1] or 0)
            last_checkin_date = row[2]
            total_earned = Decimal(row[5] or 0)
            total_withdrawn = Decimal(row[6] or 0)

            today = datetime.now(dt.UTC).date()
            yesterday = today - dt.timedelta(days=1)

            if last_checkin_date == today:
                conn.commit()
                return {
                    "claimed_today": True,
                    "reward": 0.0,
                    "streak_days": streak_days,
                    "balance": float(balance),
                    "last_checkin_date": str(last_checkin_date) if last_checkin_date else None,
                    "total_earned": float(total_earned),
                    "total_withdrawn": float(total_withdrawn),
                }

            if last_checkin_date == yesterday:
                streak_days += 1
            else:
                streak_days = 1

            reward = (Decimal("0.01") * Decimal(streak_days)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if reward > Decimal("1.00"):
                reward = Decimal("1.00")

            new_balance = (balance + reward).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            new_total_earned = (total_earned + reward).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            cur.execute(
                """
                INSERT INTO daily_checkins (discord_user_id, checkin_date, streak_days, reward_amount, balance_after)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (discord_user_id, checkin_date) DO NOTHING
                RETURNING id;
                """,
                (str(discord_user_id), today, streak_days, reward, new_balance),
            )
            inserted = cur.fetchone()
            if not inserted:
                conn.rollback()
                conn.autocommit = False
                with conn.cursor() as retry_cur:
                    _ensure_checkin_tables(retry_cur)
                    retry_cur.execute(
                        """
                        SELECT
                            streak_days,
                            balance,
                            last_checkin_date,
                            total_earned,
                            total_withdrawn
                        FROM user_checkins
                        WHERE discord_user_id = %s;
                        """,
                        (str(discord_user_id),),
                    )
                    retry_row = retry_cur.fetchone() or (0, 0, None, 0, 0)
                    conn.commit()
                    return {
                        "claimed_today": True,
                        "reward": 0.0,
                        "streak_days": int(retry_row[0] or 0),
                        "balance": float(Decimal(retry_row[1] or 0)),
                        "last_checkin_date": str(retry_row[2]) if retry_row[2] else None,
                        "total_earned": float(Decimal(retry_row[3] or 0)),
                        "total_withdrawn": float(Decimal(retry_row[4] or 0)),
                    }

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    streak_days = %s,
                    balance = %s,
                    last_checkin_date = %s,
                    total_earned = %s,
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (streak_days, new_balance, today, new_total_earned, str(discord_user_id)),
            )
            conn.commit()
            return {
                "claimed_today": False,
                "reward": float(reward),
                "streak_days": streak_days,
                "balance": float(new_balance),
                "last_checkin_date": str(today),
                "total_earned": float(new_total_earned),
                "total_withdrawn": float(total_withdrawn),
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing daily check-in for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def reserve_checkin_withdrawal(
    discord_user_id,
    minimum_amount=1.00,
    hold_timeout_minutes=15,
    requested_amount=None,
    daily_withdraw_limit=25.00,
):
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            row = _get_or_create_checkin_row(cur, discord_user_id)

            streak_days = int(row[0] or 0)
            balance = Decimal(row[1] or 0)
            hold_amount = Decimal(row[3] or 0)
            hold_created_at = row[4]

            if hold_amount > 0:
                hold_age_minutes = 0.0
                if hold_created_at is not None:
                    hold_age_minutes = max(
                        0.0,
                        (datetime.now(dt.UTC) - hold_created_at).total_seconds() / 60.0,
                    )

                conn.commit()
                if hold_age_minutes >= float(hold_timeout_minutes):
                    return {
                        "status": "manual_review_required",
                        "balance": float(balance),
                        "streak_days": streak_days,
                        "hold_amount": float(hold_amount),
                        "hold_age_minutes": hold_age_minutes,
                    }

                return {
                    "status": "in_progress",
                    "balance": float(balance),
                    "streak_days": streak_days,
                    "hold_amount": float(hold_amount),
                }

            minimum_amount_dec = Decimal(str(minimum_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            withdraw_amount = None
            if requested_amount is not None:
                requested_amount_dec = Decimal(str(requested_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                if requested_amount_dec <= 0:
                    conn.commit()
                    return {
                        "status": "invalid_amount",
                        "balance": float(balance),
                        "streak_days": streak_days,
                    }
                if requested_amount_dec < minimum_amount_dec:
                    conn.commit()
                    return {
                        "status": "below_minimum_request",
                        "balance": float(balance),
                        "streak_days": streak_days,
                        "minimum_amount": float(minimum_amount_dec),
                    }
                withdraw_amount = requested_amount_dec

            if balance < minimum_amount_dec:
                conn.commit()
                return {
                    "status": "below_minimum",
                    "balance": float(balance),
                    "streak_days": streak_days,
                    "minimum_amount": float(minimum_amount_dec),
                }

            if withdraw_amount is not None and withdraw_amount > balance:
                conn.commit()
                return {
                    "status": "insufficient_funds",
                    "balance": float(balance),
                    "streak_days": streak_days,
                    "requested_amount": float(withdraw_amount),
                }

            if withdraw_amount is None:
                withdraw_amount = balance.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            daily_limit_dec = Decimal(str(daily_withdraw_limit)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if daily_limit_dec > 0:
                now_utc = datetime.now(dt.UTC)
                day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                next_day_start = day_start + dt.timedelta(days=1)
                cur.execute(
                    """
                    SELECT COALESCE(SUM(amount), 0)
                    FROM checkin_withdrawals
                    WHERE discord_user_id = %s
                        AND status = 'success'
                        AND created_at >= %s
                        AND created_at < %s;
                    """,
                    (str(discord_user_id), day_start, next_day_start),
                )
                withdrawn_today = Decimal(cur.fetchone()[0] or 0)
                if (withdrawn_today + withdraw_amount) > daily_limit_dec:
                    conn.commit()
                    return {
                        "status": "daily_limit_reached",
                        "balance": float(balance),
                        "streak_days": streak_days,
                        "daily_limit": float(daily_limit_dec),
                        "withdrawn_today": float(withdrawn_today),
                    }

            new_balance = (balance - withdraw_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            withdrawal_id = str(uuid.uuid4())

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    balance = %s,
                    withdrawal_hold_amount = %s,
                    withdrawal_hold_created_at = NOW(),
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (new_balance, withdraw_amount, str(discord_user_id)),
            )
            cur.execute(
                """
                INSERT INTO checkin_withdrawals (withdrawal_id, discord_user_id, amount, status)
                VALUES (%s, %s, %s, 'pending');
                """,
                (withdrawal_id, str(discord_user_id), withdraw_amount),
            )
            conn.commit()
            return {
                "status": "ready",
                "withdrawal_id": withdrawal_id,
                "withdraw_amount": float(withdraw_amount),
                "balance": float(new_balance),
                "streak_days": streak_days,
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error reserving check-in withdrawal for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def finalize_checkin_withdrawal(
    discord_user_id,
    outcome,
    withdrawal_id=None,
    roobet_uid=None,
    roobet_username=None,
    error_message=None,
):
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            row = _get_or_create_checkin_row(cur, discord_user_id)

            balance = Decimal(row[1] or 0)
            hold_amount = Decimal(row[3] or 0)
            total_withdrawn = Decimal(row[6] or 0)
            withdrawal_exists = False

            if withdrawal_id:
                cur.execute(
                    """
                    SELECT status
                    FROM checkin_withdrawals
                    WHERE withdrawal_id = %s
                        AND discord_user_id = %s
                    FOR UPDATE;
                    """,
                    (str(withdrawal_id), str(discord_user_id)),
                )
                withdrawal_row = cur.fetchone()
                withdrawal_exists = withdrawal_row is not None

            if hold_amount <= 0:
                if withdrawal_exists:
                    cur.execute(
                        """
                        UPDATE checkin_withdrawals
                        SET
                            status = CASE WHEN %s = 'unknown' THEN 'unknown' ELSE status END,
                            error_message = COALESCE(%s, error_message),
                            updated_at = NOW()
                        WHERE withdrawal_id = %s;
                        """,
                        (str(outcome), error_message, str(withdrawal_id)),
                    )
                conn.commit()
                return {
                    "had_hold": False,
                    "balance": float(balance),
                    "total_withdrawn": float(total_withdrawn),
                }

            if outcome == "success":
                new_balance = balance
                new_total_withdrawn = (total_withdrawn + hold_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            elif outcome == "failed":
                new_balance = (balance + hold_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                new_total_withdrawn = total_withdrawn
            else:
                # Unknown outcome fails closed: keep hold in place until manual resolution.
                if withdrawal_exists:
                    cur.execute(
                        """
                        UPDATE checkin_withdrawals
                        SET
                            status = 'unknown',
                            error_message = %s,
                            updated_at = NOW()
                        WHERE withdrawal_id = %s;
                        """,
                        (error_message or "Unknown payout outcome", str(withdrawal_id)),
                    )
                conn.commit()
                return {
                    "had_hold": True,
                    "pending_review": True,
                    "balance": float(balance),
                    "total_withdrawn": float(total_withdrawn),
                }

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    balance = %s,
                    withdrawal_hold_amount = 0,
                    withdrawal_hold_created_at = NULL,
                    total_withdrawn = %s,
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (new_balance, new_total_withdrawn, str(discord_user_id)),
            )

            if withdrawal_exists:
                cur.execute(
                    """
                    UPDATE checkin_withdrawals
                    SET
                        status = %s,
                        roobet_uid = %s,
                        roobet_username = %s,
                        error_message = %s,
                        updated_at = NOW()
                    WHERE withdrawal_id = %s;
                    """,
                    (
                        "success" if outcome == "success" else "failed",
                        str(roobet_uid) if roobet_uid is not None else None,
                        roobet_username,
                        error_message,
                        str(withdrawal_id),
                    ),
                )
            conn.commit()
            return {
                "had_hold": True,
                "balance": float(new_balance),
                "total_withdrawn": float(new_total_withdrawn),
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error finalizing check-in withdrawal for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def edit_checkin_balance(discord_user_id, amount_delta):
    conn = get_db_connection()
    try:
        delta_dec = Decimal(str(amount_delta)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if delta_dec == 0:
            return None

        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            row = _get_or_create_checkin_row(cur, discord_user_id)

            streak_days = int(row[0] or 0)
            balance = Decimal(row[1] or 0)
            total_earned = Decimal(row[5] or 0)
            total_withdrawn = Decimal(row[6] or 0)

            new_balance = (balance + delta_dec).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if new_balance < 0:
                conn.commit()
                return {
                    "ok": False,
                    "reason": "insufficient_balance",
                    "balance": float(balance),
                    "delta": float(delta_dec),
                }

            new_total_earned = total_earned
            if delta_dec > 0:
                new_total_earned = (total_earned + delta_dec).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    balance = %s,
                    total_earned = %s,
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (new_balance, new_total_earned, str(discord_user_id)),
            )
            conn.commit()
            return {
                "ok": True,
                "amount_delta": float(delta_dec),
                "balance": float(new_balance),
                "streak_days": streak_days,
                "total_earned": float(new_total_earned),
                "total_withdrawn": float(total_withdrawn),
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error editing check-in balance for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def get_checkin_account_summary(discord_user_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            row = _get_or_create_checkin_row(cur, discord_user_id)
            conn.commit()

            streak_days = int(row[0] or 0)
            balance = float(Decimal(row[1] or 0))
            last_checkin_date = row[2]
            total_earned = float(Decimal(row[5] or 0))
            total_withdrawn = float(Decimal(row[6] or 0))

            today = datetime.now(dt.UTC).date()
            claimed_today = (last_checkin_date == today)
            next_reward = min(1.00, round((streak_days + 1) * 0.01, 2))

            return {
                "streak_days": streak_days,
                "balance": balance,
                "last_checkin_date": str(last_checkin_date) if last_checkin_date else None,
                "claimed_today": claimed_today,
                "next_reward": next_reward,
                "total_earned": total_earned,
                "total_withdrawn": total_withdrawn,
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading check-in account summary for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def process_coinflip_bet(discord_user_id, wager_amount, player_choice):
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)

            choice = str(player_choice or "").strip().lower()
            if choice not in {"heads", "tails"}:
                conn.commit()
                return {"status": "invalid_choice"}

            wager_dec = Decimal(str(wager_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if wager_dec <= 0:
                conn.commit()
                return {"status": "invalid_wager"}

            row = _get_or_create_checkin_row(cur, discord_user_id)
            balance_before = Decimal(row[1] or 0).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            if balance_before < wager_dec:
                conn.commit()
                return {
                    "status": "insufficient_funds",
                    "balance": float(balance_before),
                }

            outcome = "heads" if secrets.randbelow(2) == 0 else "tails"
            won = (choice == outcome)

            payout_multiplier = Decimal("1.95") if won else Decimal("0.00")
            payout_amount = (wager_dec * payout_multiplier).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            net_amount = (payout_amount - wager_dec).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            balance_after = (balance_before + net_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    balance = %s,
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (balance_after, str(discord_user_id)),
            )

            cur.execute(
                """
                INSERT INTO checkin_coinflip_logs (
                    discord_user_id,
                    wager_amount,
                    player_choice,
                    outcome,
                    payout_multiplier,
                    payout_amount,
                    net_amount,
                    balance_before,
                    balance_after
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    str(discord_user_id),
                    wager_dec,
                    choice,
                    outcome,
                    payout_multiplier,
                    payout_amount,
                    net_amount,
                    balance_before,
                    balance_after,
                ),
            )

            conn.commit()
            return {
                "status": "ok",
                "won": won,
                "player_choice": choice,
                "outcome": outcome,
                "wager_amount": float(wager_dec),
                "payout_multiplier": float(payout_multiplier),
                "payout_amount": float(payout_amount),
                "net_amount": float(net_amount),
                "balance_before": float(balance_before),
                "balance_after": float(balance_after),
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing coinflip bet for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def get_top_checkin_balances(limit=10):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            cur.execute(
                """
                SELECT
                    discord_user_id,
                    balance,
                    streak_days,
                    total_earned,
                    total_withdrawn,
                    last_checkin_date
                FROM user_checkins
                WHERE balance > 0
                ORDER BY balance DESC, streak_days DESC, updated_at ASC
                LIMIT %s;
                """,
                (int(limit),),
            )
            rows = cur.fetchall()
            conn.commit()

            result = []
            for row in rows:
                result.append(
                    {
                        "discord_user_id": int(row[0]),
                        "balance": float(Decimal(row[1] or 0)),
                        "streak_days": int(row[2] or 0),
                        "total_earned": float(Decimal(row[3] or 0)),
                        "total_withdrawn": float(Decimal(row[4] or 0)),
                        "last_checkin_date": str(row[5]) if row[5] else None,
                    }
                )
            return result
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading top check-in balances: {e}")
        return []
    finally:
        conn.autocommit = True
        release_db_connection(conn)


def resolve_checkin_withdrawal_hold(discord_user_id, action, note=None):
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_checkin_tables(cur)
            row = _get_or_create_checkin_row(cur, discord_user_id)

            balance = Decimal(row[1] or 0)
            hold_amount = Decimal(row[3] or 0)
            total_withdrawn = Decimal(row[6] or 0)

            if hold_amount <= 0:
                conn.commit()
                return {
                    "ok": False,
                    "reason": "no_hold",
                    "balance": float(balance),
                }

            action_normalized = str(action or "").strip().lower()
            if action_normalized not in {"release", "commit"}:
                conn.commit()
                return {
                    "ok": False,
                    "reason": "invalid_action",
                    "balance": float(balance),
                }

            if action_normalized == "release":
                new_balance = (balance + hold_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                new_total_withdrawn = total_withdrawn
            else:
                new_balance = balance
                new_total_withdrawn = (total_withdrawn + hold_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            cur.execute(
                """
                UPDATE user_checkins
                SET
                    balance = %s,
                    withdrawal_hold_amount = 0,
                    withdrawal_hold_created_at = NULL,
                    total_withdrawn = %s,
                    updated_at = NOW()
                WHERE discord_user_id = %s;
                """,
                (new_balance, new_total_withdrawn, str(discord_user_id)),
            )

            cur.execute(
                """
                WITH latest_row AS (
                    SELECT withdrawal_id
                    FROM checkin_withdrawals
                    WHERE discord_user_id = %s
                        AND status IN ('pending', 'unknown')
                    ORDER BY updated_at DESC
                    LIMIT 1
                )
                UPDATE checkin_withdrawals
                SET
                    status = %s,
                    error_message = COALESCE(%s, error_message),
                    updated_at = NOW()
                WHERE withdrawal_id IN (SELECT withdrawal_id FROM latest_row);
                """,
                (
                    str(discord_user_id),
                    "failed" if action_normalized == "release" else "success",
                    note,
                ),
            )

            conn.commit()
            return {
                "ok": True,
                "action": action_normalized,
                "released_or_committed": float(hold_amount),
                "balance": float(new_balance),
                "total_withdrawn": float(new_total_withdrawn),
            }
    except Exception as e:
        conn.rollback()
        logger.error(f"Error resolving check-in withdrawal hold for {discord_user_id}: {e}")
        return None
    finally:
        conn.autocommit = True
        release_db_connection(conn)
