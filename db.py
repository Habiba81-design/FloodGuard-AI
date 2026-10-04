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
                CREATE TABLE IF NOT EXISTS places (
                    name TEXT PRIMARY KEY,
                    lat DOUBLE PRECISION NOT NULL,
                    lon DOUBLE PRECISION NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pending_signups (
                    id SERIAL PRIMARY KEY,
                    place TEXT NOT NULL,
                    lat DOUBLE PRECISION NOT NULL,
                    lon DOUBLE PRECISION NOT NULL,
                    channel TEXT NOT NULL,
                    contact TEXT NOT NULL,
                    code TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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


# ---------------------------------------------------------------------------
# Current-status readings and clearing
# ---------------------------------------------------------------------------
def delete_reading_for(community):
    """Drop a community's previous reading(s) so only the newest one is kept."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM readings WHERE community = %s;", (community,))


def clear_readings_and_alerts():
    """Wipe every stored reading and alert. Contacts, water levels and the
    record of warnings already sent (seen_alert_keys) are left alone, so
    nobody is re-sent a warning they already received."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM readings;")
            cur.execute("DELETE FROM alerts;")


# ---------------------------------------------------------------------------
# Places (anywhere people sign up for) and self-service sign-up
# ---------------------------------------------------------------------------
def add_place(name, lat, lon):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO places (name, lat, lon) VALUES (%s, %s, %s) "
                "ON CONFLICT (name) DO NOTHING;",
                (name, lat, lon),
            )


def get_places():
    """{name: (lat, lon)} for every place people have signed up for."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name, lat, lon FROM places;")
            return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def count_places():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM places;")
            return cur.fetchone()[0]


def count_subscribers():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM subscribers;")
            return cur.fetchone()[0]


def subscriber_signed_up(place, phone, email):
    """True if this exact contact already receives alerts for this place."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM subscribers WHERE lower(trim(community)) = lower(trim(%s)) "
                "AND COALESCE(phone, '') = %s AND COALESCE(email, '') = %s LIMIT 1;",
                (place, phone, email),
            )
            return cur.fetchone() is not None


def remove_subscriber_contact(phone, email):
    """Unsubscribe a phone number or email from every place. Returns how many
    subscriptions were removed, and deletes places left with no subscribers
    (original communities are kept by the app, not here)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            if phone:
                cur.execute("DELETE FROM subscribers WHERE phone = %s;", (phone,))
            else:
                cur.execute("DELETE FROM subscribers WHERE lower(email) = lower(%s);", (email,))
            removed = cur.rowcount
            cur.execute(
                "DELETE FROM places WHERE name NOT IN "
                "(SELECT DISTINCT community FROM subscribers);"
            )
            return removed


def recent_codes_sent(contact, minutes=60):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM pending_signups WHERE contact = %s "
                "AND created_at > now() - (%s || ' minutes')::interval;",
                (contact, str(minutes)),
            )
            return cur.fetchone()[0]


def create_pending_signup(place, lat, lon, channel, contact, code):
    """Replaces any earlier unconfirmed sign-up for the same contact."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pending_signups (place, lat, lon, channel, contact, code) "
                "VALUES (%s, %s, %s, %s, %s, %s);",
                (place, lat, lon, channel, contact, code),
            )


def get_pending_signup(contact, max_age_minutes=10):
    """Newest unexpired pending sign-up for this contact, or None."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, place, lat, lon, channel, contact, code, attempts FROM pending_signups "
                "WHERE contact = %s AND created_at > now() - (%s || ' minutes')::interval "
                "ORDER BY id DESC LIMIT 1;",
                (contact, str(max_age_minutes)),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def bump_pending_attempts(pending_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE pending_signups SET attempts = attempts + 1 WHERE id = %s;", (pending_id,))


def delete_pending_signups(contact):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pending_signups WHERE contact = %s;", (contact,))
