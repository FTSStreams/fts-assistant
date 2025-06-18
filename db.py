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

def save_leaderboard_message_id(message_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                ("leaderboard_message_id", str(message_id), str(message_id))
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving leaderboard message ID: {e}")
    finally:
        release_db_connection(conn)

def get_leaderboard_message_id():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", ("leaderboard_message_id",))
            result = cur.fetchone()
            return int(result[0]) if result else None
    except Exception as e:
        logger.error(f"Error retrieving leaderboard message ID: {e}")
        return None
    finally:
        release_db_connection(conn)

def save_tip_log(user_id, username, amount, tip_type):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO manualtips (user_id, username, amount, tip_type) VALUES (%s, %s, %s, %s);",
                (user_id, username, amount, tip_type)
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
            cur.execute(
                "INSERT INTO milestonetips (user_id, tier, month, year, tipped_at) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (user_id, tier, month, year, tipped_at or datetime.now())
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving tip to database: {e}")
    finally:
        release_db_connection(conn)
