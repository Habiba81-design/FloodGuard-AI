"""
FloodGuard AI prototype pipeline.
Raw source upload, then real cleaning and missing data analysis, then
threshold and flood event detection, then risk classification, then a live
community risk dashboard with automatic forward-looking forecast checks and
real bulk outbound alerts, all in one Gradio app.

Run locally:
    pip install -r requirements.txt
    python app.py

Deploy on Render as a Web Service:
    Build command: pip install -r requirements.txt
    Start command: python app.py

Required environment variables (set on Render's Environment tab):
    ADMIN_PASSWORD          password gating every admin action below
    DATABASE_URL            Postgres connection string (e.g. from Supabase),
                            so readings, alerts, contacts, and water levels
                            survive restarts and redeploys instead of living
                            in memory / ephemeral JSON files.

Outbound alerts (optional, only active once configured):
    Email: SMTP_USER and SMTP_PASSWORD (a Gmail address and app password).
        Optionally SMTP_HOST, SMTP_PORT, ALERT_FROM_EMAIL.
    SMS: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER.
    Without these, contact lists and the dashboard still work, alerts are
    just logged instead of actually sent.

Important limitation on Render's free tier: a free Web Service spins down
after about 15 minutes with no incoming visits, which pauses the 6 hour
background check along with everything else, and it only resumes on the
next visit to the site. For a guaranteed check regardless of
traffic, either upgrade to an always-on paid instance, or use a free
uptime service (such as UptimeRobot) to ping the site every few minutes to
keep it awake.
"""

import json
import os
import smtplib
import ssl
import threading
import time as time_module
import urllib.parse
import urllib.request
from datetime import datetime
from email.message import EmailMessage

import gradio as gr
import pandas as pd

import db

try:
    from twilio.rest import Client as TwilioClient
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False

# ---------------------------------------------------------------------------
# Real Postgres-backed persistence (see db.py). Readings, alerts, contacts,
# and water levels all live in the database now, so they survive restarts,
# redeploys, and Render's free tier spin-downs. init_db() creates the tables
# on first run and is a no-op after that.
# ---------------------------------------------------------------------------
db.init_db()

FLOOD_WATER_THRESHOLD_M = 3.2       # matches Section 6 exploratory analysis output
FLOOD_RAINFALL_THRESHOLD_MM = 80.0  # 24h rainfall associated with historical flood events

CRITICAL_TERMS = [
    "overflow", "breach", "levee break", "trapped", "rising fast",
    "submerged", "washed away", "impassable", "evacuate", "collapsed",
]

COLUMN_ALIASES = {
    "timestamp": ["timestamp", "date", "datetime", "time"],
    "community": ["community", "community_id", "location", "town", "area"],
    "rainfall_mm": ["rainfall_mm", "rainfall", "rain_mm", "precip_mm", "rainfall_mm_24h"],
    "water_level_m": ["water_level_m", "water_level", "gauge_m", "level_m"],
}

# Approximate coordinates for each monitored community, used to auto-fetch
# rainfall. Small towns are not always precisely geocoded, these are close
# enough for a rainfall reading at this resolution.
COMMUNITY_COORDS = {
    "Alajo": (5.600, -0.217),
    "Mepe": (6.083, 0.433),
    "Anloga": (5.792, 0.900),
    "Sokpoe": (5.95, 0.62),
    "New Legon": (5.680, -0.170),
}
COMMUNITIES = list(COMMUNITY_COORDS.keys()) + ["All communities"]


def _subscriber_counts_table():
    rows = db.subscriber_counts()
    if not rows:
        return pd.DataFrame(columns=["community", "contacts"])
    return pd.DataFrame(rows)


def _water_levels_table():
    latest = db.get_water_levels()
    rows = [{"community": c, "last_reported_water_level_m": latest.get(c, "not yet set")}
            for c in COMMUNITY_COORDS]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Admin gate. Every action that changes contact lists, water levels, or
# forces a check requires this password, set once by whoever runs the app.
# ---------------------------------------------------------------------------
def _check_admin_password(password):
    real = os.environ.get("ADMIN_PASSWORD")
    if not real:
        return False, "Admin password is not set on the server yet. Set ADMIN_PASSWORD in Render's Environment tab first."
    if password != real:
        return False, "Incorrect admin password."
    return True, None


def bulk_import_contacts(password, community, file):
    ok, err = _check_admin_password(password)
    if not ok:
        return err, _subscriber_counts_table()
    if file is None:
        return "Upload a CSV of contacts first.", _subscriber_counts_table()

    try:
        raw = pd.read_csv(file)
    except Exception as e:
        return f"Could not read the file as CSV: {e}", _subscriber_counts_table()

    lower_cols = {c.lower().strip(): c for c in raw.columns}
    name_col = lower_cols.get("name")
    phone_col = lower_cols.get("phone") or lower_cols.get("phone_number") or lower_cols.get("mobile")
    email_col = lower_cols.get("email") or lower_cols.get("email_address")

    if not phone_col and not email_col:
        return "The CSV needs at least a 'phone' or 'email' column.", _subscriber_counts_table()

    added, skipped = 0, 0
    seen_in_this_upload = set()
    for _, row in raw.iterrows():
        phone = str(row[phone_col]).strip() if phone_col and pd.notna(row.get(phone_col)) else ""
        email = str(row[email_col]).strip() if email_col and pd.notna(row.get(email_col)) else ""
        name = str(row[name_col]).strip() if name_col and pd.notna(row.get(name_col)) else ""
        if not phone and not email:
            skipped += 1
            continue
        key = (community.strip().lower(), phone, email)
        if key in seen_in_this_upload or db.subscriber_exists(community, phone, email):
            skipped += 1
            continue
        db.add_subscriber(name, community, phone, email)
        seen_in_this_upload.add(key)
        added += 1

    return (
        f"Imported {added} new contact(s) into **{community}** "
        f"({skipped} skipped as duplicates or empty rows).",
        _subscriber_counts_table(),
    )


def update_water_level(password, community, level):
    ok, err = _check_admin_password(password)
    if not ok:
        return err, _water_levels_table()
    db.set_water_level(community, float(level or 0))
    return (
        f"Updated **{community}**'s water level to {level}m. "
        f"This is what the automatic 24h check will use until it is updated again.",
        _water_levels_table(),
    )


# ---------------------------------------------------------------------------
# Outbound sending. Both functions fail safely: if credentials are not set,
# or the send fails, they return (False, reason) instead of raising, so a
# missing SMTP or Twilio setup never crashes the app.
# ---------------------------------------------------------------------------
def _send_email_brevo(to_email, subject, body):
    """Send via Brevo's HTTPS API. Works on Render's free tier, which blocks
    SMTP ports. Needs BREVO_API_KEY and ALERT_FROM_EMAIL (a sender you have
    verified in Brevo)."""
    api_key = os.environ.get("BREVO_API_KEY")
    from_email = os.environ.get("ALERT_FROM_EMAIL")
    if not api_key or not from_email:
        return False, "Brevo not configured (missing BREVO_API_KEY / ALERT_FROM_EMAIL)."
    payload = json.dumps({
        "sender": {"name": "FloodGuard AI", "email": from_email},
        "to": [{"email": to_email}],
        "subject": subject,
        "textContent": body,
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.brevo.com/v3/smtp/email",
        data=payload,
        headers={"api-key": api_key, "content-type": "application/json", "accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return (True, "sent") if resp.status in (200, 201, 202) else (False, f"Brevo returned {resp.status}")
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")[:200]
        except Exception:
            detail = ""
        return False, f"Brevo error {e.code}: {detail}"
    except Exception as e:
        return False, str(e)


def _send_email(to_email, subject, body):
    # Prefer the HTTPS API (works on Render free tier); fall back to SMTP.
    if os.environ.get("BREVO_API_KEY"):
        return _send_email_brevo(to_email, subject, body)

    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASSWORD")
    from_email = os.environ.get("ALERT_FROM_EMAIL", user)

    if not user or not password:
        return False, "Email not configured (missing BREVO_API_KEY, or SMTP_USER / SMTP_PASSWORD)."

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(host, port, timeout=10) as server:
            server.starttls(context=context)
            server.login(user, password)
            server.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, str(e)


def _send_sms(to_phone, body):
    if not TWILIO_AVAILABLE:
        return False, "Twilio package not installed."

    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_FROM_NUMBER")

    if not (sid and token and from_number):
        return False, "SMS not configured (missing TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER)."

    try:
        client = TwilioClient(sid, token)
        client.messages.create(body=body, from_=from_number, to=to_phone)
        return True, "sent"
    except Exception as e:
        return False, str(e)


_LEVEL_PLAIN = {
    "CRITICAL": (
        "Flooding is happening now or is very likely within hours.",
        "Move to higher ground immediately if you are in a low-lying area. "
        "Avoid walking or driving through flood water. Keep phones charged "
        "and follow any instructions from NADMO or local authorities.",
    ),
    "HIGH": (
        "There is a serious risk of flooding in the next day or so.",
        "Prepare now: move valuables and important documents to higher "
        "ground, check on elderly or disabled neighbours, and stay ready "
        "to move if conditions worsen.",
    ),
    "MODERATE": (
        "Water levels or rainfall are above normal, but not yet dangerous.",
        "Keep an eye on the weather and avoid low-lying roads after heavy "
        "rain. No need to evacuate at this stage.",
    ),
    "LOW": (
        "No unusual flood risk detected right now.",
        "No action needed. This message is only sent for HIGH or CRITICAL "
        "risk, so you are seeing this as part of a test.",
    ),
}


_LEVEL_PLAIN = {
    "CRITICAL": {
        "headline": "Flooding is very likely within hours.",
        "meaning": "It means the water level and rainfall together have crossed normal threshold so flooding is very possible.",
        "steps": [
            "If you live in a low-lying area or near the river, move yourself and your family to higher ground now.",
            "Do not walk or drive through flood water, even if it looks shallow. Moving water can be much stronger and deeper than it appears.",
            "Keep your phone charged and switch on, so you can receive further updates.",
            "Check on elderly, disabled, or sick neighbours who may need help moving.",
            "Follow any instructions given by NADMO or local authorities, even if they differ from this message.",
        ],
    },
    "HIGH": {
        "headline": "There is a serious risk of flooding in the next day or so.",
        "meaning": "This means conditions are close to the levels that have caused flooding before. It is not certain flooding will happen, but you should prepare as if it might.",
        "steps": [
            "Move valuables, important documents, and anything hard to replace to a higher shelf or upper floor.",
            "Check that you have a torch, some cash, and any medicines you need, in case you have to leave quickly.",
            "Check on elderly or disabled neighbours and agree on a plan with them.",
            "Avoid low-lying roads, especially after dark, until the risk passes.",
            "Stay ready to move to higher ground if conditions get worse.",
        ],
    },
    "MODERATE": {
        "headline": "Water levels or rainfall are above normal, but not yet dangerous.",
        "meaning": "This is a heads-up, not an emergency. Conditions are worth watching, but they have not reached levels that have caused flooding before.",
        "steps": [
            "Keep an eye on the weather over the next day.",
            "Avoid driving or walking through low-lying roads shortly after heavy rain.",
            "No need to move belongings or evacuate at this stage.",
        ],
    },
    "LOW": {
        "headline": "No unusual flood risk detected right now.",
        "meaning": "Rainfall and water levels are within their normal range for your area.",
        "steps": [
            "No action is needed.",
            "You are only receiving this message because it was sent as part of a test.",
        ],
    },
}


def _plain_language_message(community, level, rainfall_mm, water_level_m, reasoning,
                              forecast=False, window_start=None, window_end=None):
    info = _LEVEL_PLAIN.get(level, {"headline": "", "meaning": "", "steps": []})
    rainfall_txt = f"{rainfall_mm:.0f} millimetres" if rainfall_mm is not None else "not available"
    water_txt = f"{water_level_m:.1f} metres" if water_level_m is not None else "not available"

    if forecast:
        if window_start:
            when_txt = f"between {window_start} and {window_end}" if window_end and window_end != window_start else f"around {window_start}"
            measured_para = (
                f"What we expect: heavy rain is forecast {when_txt}. Over the "
                f"whole of the next 24 hours, the total rainfall is expected "
                f"to be about {rainfall_txt}. Right now, the river or water "
                f"level being tracked for {community} is {water_txt}."
            )
        else:
            measured_para = (
                f"What we expect: rainfall over the next 24 hours is forecast "
                f"to be about {rainfall_txt}. Right now, the river or water "
                f"level being tracked for {community} is {water_txt}."
            )
        lead = "This is an early warning based on the weather forecast.\n\n"
    else:
        measured_para = (
            f"What was reported: rainfall of about {rainfall_txt}, and a "
            f"river or water level of {water_txt}, for {community}."
        )
        lead = ""

    steps_txt = "\n".join(f"{i}. {s}" for i, s in enumerate(info["steps"], start=1))

    return (
        f"FLOOD ALERT for {community}\n"
        f"Risk level: {level}\n\n"
        f"{info['headline']}\n\n"
        f"{lead}"
        f"{measured_para}\n\n"
        f"What this means: {info['meaning']}\n\n"
        f"What to do:\n"
        f"{steps_txt}\n\n"
        f"Please also follow any guidance from local authorities and NADMO. "
        f"If you are unsure what to do, ask a neighbour, a community leader, "
        f"or call NADMO's emergency line if your area has one.\n\n"
        f"---\n"
        f"This message was sent automatically by FloodGuard AI. "
        f"For those who want the numbers behind this alert: {reasoning}"
    )


def _dispatch_outbound(community, level, reasoning, rainfall_mm=None, water_level_m=None,
                        forecast=False, window_start=None, window_end=None):
    """Bulk send a real email/SMS to every contact imported for this
    community, or for All communities. Returns a short markdown summary.
    forecast=True means the numbers behind this alert are a weather
    forecast for what's coming, not a report of current conditions, and the
    message is worded accordingly. window_start/window_end, if known, name
    the specific hours the heaviest rain is expected."""
    if level not in ("HIGH", "CRITICAL"):
        return ""

    targets = [
        s for s in db.get_subscribers()
        if s["community"].strip().lower() == community.strip().lower()
        or s["community"].strip().lower() == "all communities"
    ]

    if not targets:
        return f"\n\nNo contacts imported for {community} yet, so no outbound alert was sent."

    message = _plain_language_message(community, level, rainfall_mm, water_level_m, reasoning,
                                       forecast=forecast, window_start=window_start, window_end=window_end)

    sent_email, sent_sms, failed = 0, 0, 0
    reasons = set()
    for s in targets:
        if s.get("email"):
            subject = f"⚠️ Flood Alert: {level} risk in {community}" if level in ("HIGH", "CRITICAL") else f"Flood update: {community}"
            ok, why = _send_email(s["email"], subject, message)
            sent_email += 1 if ok else 0
            failed += 0 if ok else 1
            if not ok:
                reasons.add(f"email: {why}")
        if s.get("phone"):
            ok, why = _send_sms(s["phone"], message)
            sent_sms += 1 if ok else 0
            failed += 0 if ok else 1
            if not ok:
                reasons.add(f"SMS: {why}")

    summary = f"\n\n**Bulk outbound alert:** {sent_email} email(s) and {sent_sms} SMS sent to contacts in {community}."
    if failed:
        summary += f" {failed} delivery attempt(s) failed. Reason(s): " + "; ".join(sorted(reasons))
    return summary


# ---------------------------------------------------------------------------
# Shared risk logic, used by manual entries, CSV uploads, and the scheduled
# automatic check.
# ---------------------------------------------------------------------------
def _risk_level(rainfall_mm, water_level_m, notes="", rainfall_label="reported"):
    rainfall_mm = float(rainfall_mm or 0)
    water_level_m = float(water_level_m or 0)
    notes_lower = (notes or "").lower()

    hit_terms = [t for t in CRITICAL_TERMS if t in notes_lower]

    if hit_terms or (water_level_m >= FLOOD_WATER_THRESHOLD_M and rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM):
        level = "CRITICAL"
    elif water_level_m >= FLOOD_WATER_THRESHOLD_M * 0.8 or rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM * 0.65:
        level = "HIGH"
    elif water_level_m >= FLOOD_WATER_THRESHOLD_M * 0.6 or rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM * 0.3:
        level = "MODERATE"
    else:
        level = "LOW"

    reasoning = (
        f"Rule based check: water level {water_level_m}m against a {FLOOD_WATER_THRESHOLD_M}m threshold, "
        f"{rainfall_label} rainfall {rainfall_mm}mm against an {FLOOD_RAINFALL_THRESHOLD_MM}mm threshold"
        + (f", critical terms detected: {', '.join(hit_terms)}" if hit_terms else "")
        + "."
    )
    return level, reasoning


def _raise_alert(time_str, community, rainfall_mm, water_level_m, level, notes, source):
    db.add_alert(time_str, community, rainfall_mm, water_level_m, level, notes, source)


def _alerts_table():
    rows = db.get_alerts()
    if not rows:
        return pd.DataFrame(columns=["time", "community", "rainfall_mm", "water_level_m", "risk_level", "notes", "source"])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Automatic rainfall fetch (Open-Meteo, free, no API key or signup needed)
# and the forward-looking forecast check that ties everything together.
# ---------------------------------------------------------------------------
def _fetch_forecast_rainfall_mm(lat, lon):
    """Rainfall FORECAST for the next 24 hours starting from right now, at
    these coordinates. This looks forward, not backward, so an alert means
    'this is expected to happen', not 'this already happened'. Also picks
    out the specific block of hours when the rain is actually expected, so
    people know *when* to prepare, not just that a wet day is coming.
    Returns (total_mm, window_start, window_end, error); window_start and
    window_end are human readable strings like 'Mon 3:00 PM', or None if no
    meaningful rain is expected in the window. error is None on success, a
    short string on failure, never raises.

    Ghana (Africa/Accra) has no daylight saving and sits at UTC+0, so the
    server's own UTC clock lines up with local time here without extra
    conversion.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation",
        "forecast_days": 3,
        "timezone": "Africa/Accra",
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
        times = data.get("hourly", {}).get("time", [])
        values = data.get("hourly", {}).get("precipitation", [])
        if not times or not values:
            return None, None, None, "No forecast data returned by the weather API."

        current_hour = datetime.utcnow().strftime("%Y-%m-%dT%H:00")
        try:
            start = times.index(current_hour)
        except ValueError:
            start = 0  # fall back to the start of the returned forecast

        window_times = times[start:start + 24]
        window_values = values[start:start + 24]
        if not window_values:
            return None, None, None, "Forecast window was empty."
        total_mm = round(sum(window_values), 1)

        # Find the specific hours when rain is actually expected (>=1mm/h),
        # so the alert can say *when*, not just *how much*.
        rain_hour_idxs = [i for i, v in enumerate(window_values) if v is not None and v >= 1.0]
        window_start_str, window_end_str = None, None
        if rain_hour_idxs:
            first_dt = datetime.fromisoformat(window_times[rain_hour_idxs[0]])
            last_dt = datetime.fromisoformat(window_times[rain_hour_idxs[-1]])
            window_start_str = first_dt.strftime("%a %-I:%M %p")
            window_end_str = last_dt.strftime("%a %-I:%M %p")

        return total_mm, window_start_str, window_end_str, None
    except Exception as e:
        return None, None, None, str(e)


def run_scheduled_check():
    """Check every monitored community once: fetch a rainfall FORECAST for
    the next 24 hours, combine with the last manually reported water level,
    classify, raise an alert, and dispatch a bulk outbound alert if HIGH or
    CRITICAL. This is the forward-looking early-warning path: it is meant to
    give people notice before flooding happens, not just confirm it already
    did. Used both by the background loop and the admin 'Run check now'
    button."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    latest_water_levels = db.get_water_levels()
    for community, (lat, lon) in COMMUNITY_COORDS.items():
        rainfall_mm, window_start, window_end, err = _fetch_forecast_rainfall_mm(lat, lon)
        water_level_m = latest_water_levels.get(community, 0.0)
        notes = "Automatic forecast-based check."
        if window_start:
            notes += f" Heaviest rain expected {window_start} to {window_end}."
        if err:
            notes += f" Forecast fetch failed, treated as 0mm: {err}"
        level, reasoning = _risk_level(rainfall_mm or 0, water_level_m, "", rainfall_label="forecast (next 24h)")
        _raise_alert(now, community, rainfall_mm or 0, water_level_m, level, notes, "scheduled")
        if level in ("HIGH", "CRITICAL"):
            # Skip re-sending if we already warned about this exact rain
            # window for this community, so people don't get the same
            # forecast alert every few hours while it's still pending.
            dedupe_key = f"forecast:{window_start or 'unknown'}"
            if not db.is_seen_key(community, dedupe_key):
                db.add_seen_key(community, dedupe_key)
                _dispatch_outbound(community, level, reasoning, rainfall_mm, water_level_m,
                                    forecast=True, window_start=window_start, window_end=window_end)
        rain_display = f"{rainfall_mm}mm forecast" if rainfall_mm is not None else "unavailable"
        window_display = f", heaviest rain {window_start}-{window_end}" if window_start else ""
        lines.append(f"- **{community}**: {rain_display}{window_display}, current water level {water_level_m}m -> **{level}**")
    return "\n".join(lines)


def run_check_now(password):
    ok, err = _check_admin_password(password)
    if not ok:
        return err, _alerts_table()
    summary = run_scheduled_check()
    return f"Ran the check manually just now.\n\n{summary}", _alerts_table()


def _scheduler_loop():
    while True:
        try:
            run_scheduled_check()
        except Exception:
            pass
        # Every 6 hours, not 24: the forecast only looks 24h ahead, so a
        # once-a-day check can miss rain that's due in, say, 20 hours until
        # the next run. Checking every 6h keeps real lead time within the
        # 12-24h window instead of drifting with when the loop happens to
        # wake up. The dedupe key in run_scheduled_check stops the same
        # rain window from re-sending a fresh alert every 6h.
        time_module.sleep(6 * 60 * 60)


_scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
_scheduler_thread.start()


# ---------------------------------------------------------------------------
# Step 1: Data acquisition and integration (CSV upload, flexible column matching)
# ---------------------------------------------------------------------------
def _match_columns(df):
    """Map whatever headers the uploaded CSV has onto our canonical schema."""
    lower_cols = {c.lower().strip(): c for c in df.columns}
    mapping = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in lower_cols:
                mapping[lower_cols[alias]] = canonical
                break
    return mapping


def process_csv(file):
    """`file` is a filepath string, as returned by gr.File(type="filepath")."""
    if file is None:
        return (
            "Upload a CSV to run the pipeline, or click Load sample data below.",
            None, None,
        )

    try:
        raw = pd.read_csv(file)
    except Exception as e:
        return (f"Could not read the file as CSV: {e}", None, None)

    mapping = _match_columns(raw)
    missing_required = [c for c in COLUMN_ALIASES if c not in mapping.values()]
    df = raw.rename(columns=mapping)

    for col in COLUMN_ALIASES:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[["timestamp", "community", "rainfall_mm", "water_level_m"]].copy()

    # Real cleaning
    df["community"] = df["community"].astype(str).str.strip()
    df["community"] = df["community"].replace({"nan": pd.NA, "": pd.NA})
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", dayfirst=True)
    df["rainfall_mm"] = (
        df["rainfall_mm"].astype(str).str.extract(r"([\d.]+)")[0].astype(float)
    )
    df["water_level_m"] = (
        df["water_level_m"].astype(str).str.extract(r"([\d.]+)")[0].astype(float)
    )

    # Outlier flags (physically implausible values for this domain)
    df["outlier_flag"] = (
        (df["rainfall_mm"] > 500) | (df["rainfall_mm"] < 0)
        | (df["water_level_m"] > 15) | (df["water_level_m"] < 0)
    )

    # Duplicate detection (same community and timestamp)
    df["duplicate_flag"] = df.duplicated(subset=["community", "timestamp"], keep="first")

    # Flood event detection against Phase 1 thresholds
    df["flood_event_flag"] = (
        (df["water_level_m"] >= FLOOD_WATER_THRESHOLD_M)
        | (df["rainfall_mm"] >= FLOOD_RAINFALL_THRESHOLD_MM)
    )

    def quality_flag(row):
        if row["outlier_flag"]:
            return "outlier"
        if row["duplicate_flag"]:
            return "duplicate"
        if pd.isna(row["water_level_m"]) or pd.isna(row["rainfall_mm"]) or pd.isna(row["community"]):
            return "missing_field"
        return "clean"

    df["data_quality_flag"] = df.apply(quality_flag, axis=1)

    # Missing data report (the Section 10 success metric)
    n = len(df)
    missing_pct = {
        col: round(100 * df[col].isna().mean(), 1)
        for col in ["timestamp", "community", "rainfall_mm", "water_level_m"]
    }
    missing_df = pd.DataFrame(
        {"field": list(missing_pct.keys()), "missing_pct": list(missing_pct.values())}
    )
    avg_missing = round(sum(missing_pct.values()) / len(missing_pct), 1)

    n_outliers = int(df["outlier_flag"].sum())
    n_dupes = int(df["duplicate_flag"].sum())
    n_flood_events = int(df["flood_event_flag"].sum())

    # Automatically raise and dispatch outbound alerts for rows that cross
    # a threshold, skipping rows already alerted (same community + timestamp)
    # so re-uploading the same file does not resend the same alert.
    n_new_alerts, n_notified = 0, 0
    for _, row in df[df["flood_event_flag"]].iterrows():
        ts_str = str(row["timestamp"])
        community = row["community"]
        if pd.isna(community):
            continue
        if db.is_seen_key(community, ts_str):
            continue
        db.add_seen_key(community, ts_str)

        level, reasoning = _risk_level(row["rainfall_mm"], row["water_level_m"])
        _raise_alert(ts_str, community, row["rainfall_mm"], row["water_level_m"], level, "", "csv_upload")
        n_new_alerts += 1
        if level in ("HIGH", "CRITICAL"):
            summary = _dispatch_outbound(community, level, reasoning, row["rainfall_mm"], row["water_level_m"])
            if "email(s)" in summary:
                n_notified += 1

    status_lines = [
        f"**Rows processed:** {n}",
        f"**Average missing data across key fields:** {avg_missing}% "
        f"({'under' if avg_missing < 10 else 'over'} the 10% Phase 1 target)",
        f"**Outliers flagged:** {n_outliers}",
        f"**Duplicate readings flagged:** {n_dupes}",
        f"**Flood threshold crossings detected:** {n_flood_events} "
        f"(water level at least {FLOOD_WATER_THRESHOLD_M}m, or rainfall at least {FLOOD_RAINFALL_THRESHOLD_MM}mm per 24h)",
    ]
    if n_new_alerts:
        status_lines.append(
            f"**New alerts raised from this upload:** {n_new_alerts} "
            f"(bulk outbound alerts dispatched for {n_notified} of these, where contacts exist)."
        )
    if missing_required:
        status_lines.append(
            "Could not confidently find a column for: "
            + ", ".join(missing_required)
            + ". Rename your CSV headers to include one of: "
            + "; ".join(f"{k} ({'/'.join(v)})" for k, v in COLUMN_ALIASES.items() if k in missing_required)
        )
    report_md = "\n\n".join(status_lines)

    # Persist cleaned rows into the shared database for the dashboard tab
    for _, row in df.iterrows():
        ts_str = str(row["timestamp"])
        community = row["community"] if pd.notna(row["community"]) else None
        rainfall_mm = float(row["rainfall_mm"]) if pd.notna(row["rainfall_mm"]) else None
        water_level_m = float(row["water_level_m"]) if pd.notna(row["water_level_m"]) else None
        db.add_reading(
            ts_str, community, rainfall_mm, water_level_m,
            row["data_quality_flag"], bool(row["flood_event_flag"]),
        )

    display_df = df.copy()
    display_df["timestamp"] = display_df["timestamp"].astype(str)

    return report_md, display_df, missing_df


def load_sample_data():
    """Illustrative multi source CSV a hackathon judge can click to try instantly."""
    sample_text = (
        "date,community,rainfall,water_level\n"
        "24/06/2026,Alajo,20mm,1.8\n"
        "25/06/2026,Alajo,35mm,2.1\n"
        "26/06/2026,Alajo,55mm,2.6\n"
        "27/06/2026,Alajo,78mm,3.0\n"
        "28/06/2026,Alajo,95mm,3.6\n"
        "29/06/2026,Alajo,110mm,N/A\n"
        "28/06/2026,Mepe,88mm,\n"
        "29/06/2026,Mepe,120mm,4.4\n"
        "25/06/2026,Anloga,35mm,2.1\n"
        "29/06/2026,Anloga,,3.9\n"
        "29/06/2026,Anloga,120mm,3.9\n"  # duplicate on purpose
        "30/06/2026,Sokpoe,999mm,2.0\n"  # outlier on purpose
    )
    path = "/tmp/floodguard_sample.csv"
    with open(path, "w") as f:
        f.write(sample_text)

    return process_csv(path)


def _dashboard_table():
    rows = db.get_readings()
    if not rows:
        return pd.DataFrame(columns=["timestamp", "community", "rainfall_mm", "water_level_m", "data_quality_flag", "flood_event_flag"])
    df = pd.DataFrame(rows)
    df = df.sort_values(by="flood_event_flag", ascending=False)
    df["timestamp"] = df["timestamp"].astype(str)
    return df[["timestamp", "community", "rainfall_mm", "water_level_m", "data_quality_flag", "flood_event_flag"]]


# ---------------------------------------------------------------------------
# Step 2: Risk classification, rule based and fully transparent. Used for a
# one-off field report (a resident or reporter describing a live scenario),
# separate from the automatic 24h check above.
# ---------------------------------------------------------------------------
def classify_risk(community, rainfall_mm, water_level_m, notes):
    if not community:
        return "Enter a community name to classify."

    level, reasoning = _risk_level(rainfall_mm, water_level_m, notes)

    time_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    _raise_alert(time_str, community, float(rainfall_mm or 0), float(water_level_m or 0), level, notes, "manual")

    alert_summary = _dispatch_outbound(community, level, reasoning, float(rainfall_mm or 0), float(water_level_m or 0))

    return f"### Risk level: {level}\n\n{reasoning}{alert_summary}"


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
with gr.Blocks(title="FloodGuard AI") as demo:
    gr.Markdown(
        "# FloodGuard AI\n"
        "Automatic early warning for flood-prone communities. Every 6 hours, "
        "the system checks the rainfall FORECAST for the next 24 hours for each "
        "monitored community, works out when the heaviest rain is expected, "
        "combines it with the last reported water level, classifies the risk, "
        "and sends a real bulk email/SMS alert to every contact registered "
        "for that community, no signup required from residents themselves. "
        "This is designed to warn people 12-24 hours before rain arrives, not "
        "just confirm flooding after it's already happening."
    )

    with gr.Tab("Community Risk Dashboard"):
        gr.Markdown("Live view of every alert raised: automatic forecast checks, CSV uploads, and manual field reports, newest first.")
        refresh_btn = gr.Button("Refresh dashboard")
        readings_dashboard = gr.Dataframe(label="Cleaned CSV readings", value=_dashboard_table)
        alerts_dashboard = gr.Dataframe(label="Alerts raised (all sources)", value=_alerts_table)
        refresh_btn.click(lambda: (_dashboard_table(), _alerts_table()), outputs=[readings_dashboard, alerts_dashboard])

    with gr.Tab("Field Report"):
        gr.Markdown(
            "For a one-off live report, the way a community reporter or sensor feed would send it. "
            "Classification is rule based and transparent. A HIGH or CRITICAL result here also "
            "triggers a real bulk outbound alert to that community's contact list."
        )
        with gr.Row():
            comm_in = gr.Textbox(label="Community", placeholder="e.g. Mepe")
            rain_in = gr.Number(label="Rainfall, last 24h (mm)", value=0)
            level_in = gr.Number(label="Water level (m)", value=0)
        notes_in = gr.Textbox(label="Field notes (optional)", placeholder="e.g. river rising fast near the market")
        classify_btn = gr.Button("Classify", variant="primary")
        risk_out = gr.Markdown()
        classify_btn.click(classify_risk, inputs=[comm_in, rain_in, level_in, notes_in], outputs=risk_out)

    with gr.Tab("Data Pipeline (bulk CSV analysis)"):
        gr.Markdown(
            "Upload a CSV with columns for date, community, rainfall, and water level "
            "(headers can vary, the pipeline matches common aliases). Rows that cross a "
            "threshold automatically raise an alert and bulk-notify that community's "
            "contact list, so avoid uploading old historical data unless you intend for "
            "real contacts to be notified about it."
        )
        with gr.Row():
            csv_input = gr.File(label="Rainfall / water level CSV", file_types=[".csv"], type="filepath")
            sample_btn = gr.Button("Load sample data instead")
        report = gr.Markdown()
        with gr.Row():
            cleaned_out = gr.Dataframe(label="Cleaned, schema aligned rows", wrap=True)
        missing_out = gr.Dataframe(label="Missing data by field (%)", visible=True)
        csv_input.change(process_csv, inputs=csv_input, outputs=[report, cleaned_out, missing_out])
        sample_btn.click(load_sample_data, outputs=[report, cleaned_out, missing_out])

    with gr.Tab("Admin"):
        gr.Markdown(
            "Everything here requires the admin password, set once as `ADMIN_PASSWORD` "
            "in Render's Environment tab. This is where whoever is in charge imports each "
            "community's contact list in bulk (so residents never need to sign up "
            "themselves) and keeps the current water level up to date for the automatic "
            "forecast-based check."
        )
        admin_password = gr.Textbox(label="Admin password", type="password")

        gr.Markdown("### Import a community's contact list (CSV with columns: name, phone, email)")
        with gr.Row():
            import_community = gr.Dropdown(label="Community", choices=COMMUNITIES, value="Mepe")
            import_file = gr.File(label="Contacts CSV", file_types=[".csv"], type="filepath")
        import_btn = gr.Button("Import contacts", variant="primary")
        import_out = gr.Markdown()
        contact_counts = gr.Dataframe(label="Contacts per community", value=_subscriber_counts_table)
        import_btn.click(
            bulk_import_contacts,
            inputs=[admin_password, import_community, import_file],
            outputs=[import_out, contact_counts],
        )

        gr.Markdown("### Update a community's latest water level")
        with gr.Row():
            wl_community = gr.Dropdown(label="Community", choices=list(COMMUNITY_COORDS.keys()), value="Mepe")
            wl_level = gr.Number(label="Water level (m)", value=0)
        wl_btn = gr.Button("Update water level")
        wl_out = gr.Markdown()
        wl_table = gr.Dataframe(label="Latest water level per community", value=_water_levels_table)
        wl_btn.click(update_water_level, inputs=[admin_password, wl_community, wl_level], outputs=[wl_out, wl_table])

        gr.Markdown(
            "### Run the automatic forecast check now\n"
            "Normally runs every 6 hours by itself, so real lead time stays "
            "within about 12-24 hours before rain arrives. It looks at the rainfall "
            "**forecast for the next 24 hours**, works out the specific hours "
            "heavy rain is expected, and warns people before flooding happens, "
            "not after. Use this button to run it immediately, for testing or a demo."
        )
        run_now_btn = gr.Button("Run check now", variant="primary")
        run_now_out = gr.Markdown()
        run_now_alerts = gr.Dataframe(label="Alerts raised (all sources)")
        run_now_btn.click(run_check_now, inputs=[admin_password], outputs=[run_now_out, run_now_alerts])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
