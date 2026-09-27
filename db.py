"""
Database layer for FloodGuard AI.

Swaps the old in-memory lists (READINGS, ALERTS, _SEEN_ALERT_KEYS) and JSON
files (subscribers.json, water_levels.json) for real Postgres tables, so
data survives restarts, redeploys, and Render's free tier spin-downs.

Requires DATABASE_URL to be set (a Postgres connection string). Get one
from Supabase: Project Settings -> Database -> Connection string (URI),
"Transaction pooler" mode is recommended for a small web app like this.

If DATABASE_URL is missing, every function below raises a clear error
rather than silently falling back to memory, since a silent fallback is
exactly the kind of bug that looks fine in testing and loses real data
in production.
"""

import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


def _dsn():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it in Render's Environment tab "
            "(a Postgres connection string from Supabase or similar)."
        )
    return dsn


@contextmanager
def get_conn():
    conn = psycopg2.connect(_dsn())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist yet. Safe to call every startup."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS subscribers (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    community TEXT NOT NULL,
                    phone TEXT,
                    email TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS water_levels (
                    community TEXT PRIMARY KEY,
                    level_m DOUBLE PRECISION NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS readings (
                    id SERIAL PRIMARY KEY,
                    ts_str TEXT,
                    community TEXT,
                    rainfall_mm DOUBLE PRECISION,
                    water_level_m DOUBLE PRECISION,
                    data_quality_flag TEXT,
                    flood_event_flag BOOLEAN
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    time_str TEXT,
                    community TEXT,
                    rainfall_mm DOUBLE PRECISION,
                    water_level_m DOUBLE PRECISION,
                    risk_level TEXT,
                    notes TEXT,
                    source TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_alert_keys (
                    community TEXT NOT NULL,
                    ts_str TEXT NOT NULL,
                    PRIMARY KEY (community, ts_str)
                );
            """)


# ---------------------------------------------------------------------------
# Subscribers
# ---------------------------------------------------------------------------
def get_subscribers():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT name, community, phone, email FROM subscribers;")
            return [dict(r) for r in cur.fetchall()]


def subscriber_exists(community, phone, email):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM subscribers WHERE lower(trim(community)) = lower(trim(%s)) "
                "AND phone = %s AND email = %s LIMIT 1;",
                (community, phone, email),
            )
            return cur.fetchone() is not None


def add_subscriber(name, community, phone, email):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO subscribers (name, community, phone, email) VALUES (%s, %s, %s, %s);",
                (name, community, phone, email),
            )


def subscriber_counts():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT community, COUNT(*) AS contacts FROM subscribers "
                "GROUP BY community ORDER BY community;"
            )
            return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Water levels
# ---------------------------------------------------------------------------
def get_water_levels():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT community, level_m FROM water_levels;")
            return {row[0]: row[1] for row in cur.fetchall()}


def set_water_level(community, level_m):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO water_levels (community, level_m) VALUES (%s, %s) "
                "ON CONFLICT (community) DO UPDATE SET level_m = EXCLUDED.level_m;",
                (community, level_m),
            )


# ---------------------------------------------------------------------------
# Readings
# ---------------------------------------------------------------------------
def add_reading(ts_str, community, rainfall_mm, water_level_m, data_quality_flag, flood_event_flag):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO readings (ts_str, community, rainfall_mm, water_level_m, "
                "data_quality_flag, flood_event_flag) VALUES (%s, %s, %s, %s, %s, %s);",
                (ts_str, community, rainfall_mm, water_level_m, data_quality_flag, bool(flood_event_flag)),
            )


def get_readings():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT ts_str AS timestamp, community, rainfall_mm, water_level_m, "
                "data_quality_flag, flood_event_flag FROM readings ORDER BY id;"
            )
            return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------
def add_alert(time_str, community, rainfall_mm, water_level_m, risk_level, notes, source):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO alerts (time_str, community, rainfall_mm, water_level_m, "
                "risk_level, notes, source) VALUES (%s, %s, %s, %s, %s, %s, %s);",
                (time_str, community, rainfall_mm, water_level_m, risk_level, notes or "", source),
            )


def get_alerts():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT time_str AS time, community, rainfall_mm, water_level_m, "
                "risk_level, notes, source FROM alerts ORDER BY id DESC;"
            )
            return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Seen alert keys (dedupe CSV re-uploads)
# ---------------------------------------------------------------------------
def is_seen_key(community, ts_str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM seen_alert_keys WHERE community = %s AND ts_str = %s;",
                (community, ts_str),
            )
            return cur.fetchone() is not None


def add_seen_key(community, ts_str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO seen_alert_keys (community, ts_str) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING;",
                (community, ts_str),
            )
