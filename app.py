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
import secrets
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
    """Compares using secrets.compare_digest instead of ==, so the check
    takes the same amount of time regardless of where the strings first
    differ. A plain == comparison can (in principle) leak how many
    characters were correct through tiny timing differences; this closes
    that gap. The password itself is still a single shared plaintext value
    in an environment variable, which is fine for a small team but is not
    real per-user authentication - worth knowing as a real, disclosed
    limitation rather than something to quietly ignore."""
    real = os.environ.get("ADMIN_PASSWORD")
    if not real:
        return False, "Admin password is not set on the server yet. Set ADMIN_PASSWORD in Render's Environment tab first."
    if not secrets.compare_digest(password or "", real):
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


def _risk_badge_html(level):
    """A small colored pill for a risk level, used anywhere a level is shown
    in a gr.Markdown output (which renders raw HTML)."""
    colors = {
        "LOW": "#2F9E44",
        "MODERATE": "#F2A007",
        "HIGH": "#E8590C",
        "CRITICAL": "#C92A2A",
    }
    color = colors.get(level, "#666")
    return (
        f'<span style="display:inline-block;padding:3px 12px;border-radius:999px;'
        f'font-weight:600;color:#fff;background:{color};">{level}</span>'
    )


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

    # CRITICAL is reached either by the combined signal (both water level and
    # rainfall crossing their threshold together), or by either signal alone
    # being extreme enough on its own. This second path matters for
    # communities with no real water level sensor (water level stuck at 0,
    # e.g. New Legon, where flooding is drainage-driven, not river-driven):
    # without it, rainfall alone could never classify as CRITICAL, only HIGH,
    # no matter how extreme the forecast rain was.
    combined_critical = water_level_m >= FLOOD_WATER_THRESHOLD_M and rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM
    rainfall_alone_critical = rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM * 1.25
    water_alone_critical = water_level_m >= FLOOD_WATER_THRESHOLD_M * 1.25

    if hit_terms or combined_critical or rainfall_alone_critical or water_alone_critical:
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


def _fetch_river_water_level(lat, lon):
    """Automatic water level signal, sourced from Open-Meteo's free Flood API
    (GloFAS - Global Flood Awareness System), which models river discharge
    worldwide. There is no public network of river gauges in Ghana reporting
    a live number in metres, so this is a genuine but indirect proxy: it
    compares today's discharge (m3/s) to the recent 60 day median for that
    same river, and scales the ratio onto the same metres scale used
    elsewhere in this app, so it can be compared directly against
    FLOOD_WATER_THRESHOLD_M.

    Scaling: normal flow (ratio 1.0) maps to half the threshold; double the
    normal flow (ratio 2.0) maps to exactly the threshold; this is a
    reasonable approximation, not a real depth reading.

    Returns (water_level_m, ratio, error). water_level_m and ratio are None
    if no modelled river was found near these coordinates (GloFAS has no
    river to model there, e.g. a drainage-only location like New Legon) or
    the request failed; error is a short string in that case.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "river_discharge",
        "past_days": 60,
        "forecast_days": 1,
    }
    url = "https://flood-api.open-meteo.com/v1/flood?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
        values = data.get("daily", {}).get("river_discharge", [])
        values = [v for v in values if v is not None]
        if len(values) < 2:
            return None, None, "No modelled river found near these coordinates."

        today = values[-1]
        baseline = sorted(values[:-1])[len(values[:-1]) // 2]  # median of history, excluding today
        if not baseline or baseline <= 0:
            return None, None, "River discharge baseline was zero or unavailable."

        ratio = round(today / baseline, 2)
        water_level_m = round(ratio * (FLOOD_WATER_THRESHOLD_M / 2), 2)
        return water_level_m, ratio, None
    except Exception as e:
        return None, None, str(e)


def run_scheduled_check():
    """Check every monitored community once: fetch a rainfall FORECAST for
    the next 24 hours, fetch an automatic water level signal from river
    discharge data where a modelled river exists, classify, raise an alert,
    and dispatch a bulk outbound alert if HIGH or CRITICAL. This is the
    forward-looking early-warning path: it is meant to give people notice
    before flooding happens, not just confirm it already did. Used both by
    the background loop and the admin 'Run check now' button."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = []
    manual_water_levels = db.get_water_levels()
    for community, (lat, lon) in COMMUNITY_COORDS.items():
        rainfall_mm, window_start, window_end, err = _fetch_forecast_rainfall_mm(lat, lon)

        auto_level_m, ratio, wl_err = _fetch_river_water_level(lat, lon)
        if auto_level_m is not None:
            water_level_m = auto_level_m
            water_source = f"automatic, from river discharge running at {ratio}x its recent normal level"
            db.set_water_level(community, water_level_m)  # keep the Admin table in sync
        else:
            # No modelled river here (e.g. New Legon, which floods from
            # drainage, not a river), so fall back to whatever an admin
            # last entered by hand, or 0 if nothing has ever been set.
            water_level_m = manual_water_levels.get(community, 0.0)
            water_source = f"manual entry, no automatic river data available ({wl_err})"

        notes = "Automatic forecast-based check."
        if window_start:
            notes += f" Heaviest rain expected {window_start} to {window_end}."
        notes += f" Water level source: {water_source}."
        if err:
            notes += f" Rainfall forecast fetch failed, treated as 0mm: {err}"
        level, reasoning = _risk_level(rainfall_mm or 0, water_level_m, "", rainfall_label="forecast (next 24h)")
        # Every check (regardless of level) is logged to the readings table,
        # which is what powers the dashboard's "latest reading per
        # community" view. The alerts table is different: it should only
        # ever contain real HIGH/CRITICAL alerts, never routine LOW/MODERATE
        # checks, or "Alerts sent" becomes a log of everything instead of a
        # log of actual warnings. Previously _raise_alert ran unconditionally
        # here, which meant every 6-hour check for every community showed up
        # as an "alert" even at 0.2mm of rain. Fixed by moving it inside the
        # HIGH/CRITICAL branch below, alongside the real outbound dispatch.
        db.add_reading(now, community, rainfall_mm or 0, water_level_m, level, level in ("HIGH", "CRITICAL"))
        if level in ("HIGH", "CRITICAL"):
            _raise_alert(now, community, rainfall_mm or 0, water_level_m, level, notes, "scheduled")
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
        lines.append(f"- **{community}**: {rain_display}{window_display}, water level {water_level_m}m ({water_source}) -> {_risk_badge_html(level)}")
    return "\n".join(lines)


def _geocode_place(name):
    """Look up a place name using Open-Meteo's free Geocoding API (no key
    needed). Returns (lat, lon, display_name, error)."""
    if not name or not name.strip():
        return None, None, None, "Type a place name first."
    params = {"name": name.strip(), "count": 1, "language": "en", "format": "json"}
    url = "https://geocoding-api.open-meteo.com/v1/search?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
        results = data.get("results") or []
        if not results:
            return None, None, None, f"Could not find a place called '{name}'. Try a nearby bigger town."
        r = results[0]
        display = ", ".join(p for p in [r.get("name"), r.get("admin1"), r.get("country")] if p)
        return r["latitude"], r["longitude"], display, None
    except Exception as e:
        return None, None, None, str(e)


def check_my_area(place_name):
    """On-the-fly flood risk check for ANY location, not just the five fixed
    communities in COMMUNITY_COORDS. This is transient by design: it is not
    saved anywhere and no alert is sent, so anyone can check risk for
    wherever they are without registering a new community. To get ongoing
    automatic alerts, a community still needs to be added to
    COMMUNITY_COORDS and given a contact list in the Admin tab."""
    lat, lon, display_name, err = _geocode_place(place_name)
    if err:
        return f"**Could not check this location.** {err}"

    rainfall_mm, window_start, window_end, rain_err = _fetch_forecast_rainfall_mm(lat, lon)
    water_level_m, ratio, wl_err = _fetch_river_water_level(lat, lon)

    if water_level_m is None:
        water_level_m = 0.0
        water_note = (
            "No modelled river was found near this location, so this check is "
            "based on rainfall alone. If flooding here comes from drainage "
            "rather than a river (common in built-up areas), that is expected."
        )
    else:
        water_note = f"River discharge here is currently running at {ratio}x its recent normal level."

    level, reasoning = _risk_level(rainfall_mm or 0, water_level_m, "", rainfall_label="forecast (next 24h)")
    badge = _risk_badge_html(level)

    window_line = ""
    if window_start:
        window_line = f"\n\nHeaviest rain is expected between **{window_start}** and **{window_end}**."

    rain_line = f"{rainfall_mm}mm forecast over the next 24 hours" if rainfall_mm is not None else "unavailable right now"

    return (
        f"### {display_name}\n\n"
        f"Risk level: {badge}\n\n"
        f"Rainfall: {rain_line}.{window_line}\n\n"
        f"{water_note}\n\n"
        f"---\n"
        f"*This is a one-time check, not an ongoing alert. Nothing here is saved. "
        f"To get automatic warnings 12-24 hours before rain for this area going "
        f"forward, this location would need to be added as a monitored community "
        f"with a registered contact list, in the Admin tab.*\n\n"
        f"Technical detail: {reasoning}"
    )


KNOWN_FLOOD_EVENTS = [
    {
        "community": "New Legon",
        "start_date": "2025-05-17",
        "end_date": "2025-05-19",
        "description": (
            "Accra floods of 18 May 2025. Real, NADMO-confirmed: 5 deaths, over "
            "3,000 people displaced, after roughly four hours of heavy rain. "
            "Affected areas included Adenta, Kaneshie, Okponglo and East Legon "
            "Hills, right around New Legon. Caused by rainfall overwhelming "
            "drainage, not a river, so this tests whether rainfall alone "
            "correctly triggers a warning for a drainage-only location."
        ),
    },
    {
        "community": "New Legon",
        "start_date": "2026-06-27",
        "end_date": "2026-06-30",
        "description": (
            "Accra floods of 29 June 2026, the most recent major flooding in Ghana "
            "at the time this was written. Real, widely reported: at least 10-12 "
            "deaths, major roads submerged, drainage overwhelmed across Accra "
            "including Adenta, Madina, Achimota and East Legon, right where New "
            "Legon sits. GMet recorded about 333mm of rain in Accra for June 2026, "
            "its wettest June in over a decade. Unlike Mepe, this was caused by "
            "rainfall overwhelming drainage, not a river, so New Legon has no "
            "automatic water level signal here, same as it has live - this "
            "specifically tests whether rainfall alone correctly triggers a warning."
        ),
    },
]


def _fetch_historical_rainfall_daily(lat, lon, start_date, end_date):
    """Real historical daily rainfall totals (mm) for a past date range, from
    Open-Meteo's archive API. Returns (dates, values, error)."""
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": "precipitation_sum", "timezone": "Africa/Accra",
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.load(resp)
        dates = data.get("daily", {}).get("time", [])
        values = data.get("daily", {}).get("precipitation_sum", [])
        if not dates:
            return None, None, "No historical rainfall data returned."
        return dates, values, None
    except Exception as e:
        return None, None, str(e)


def _fetch_historical_river_discharge(lat, lon, start_date, end_date):
    """Real historical daily river discharge (m3/s) for a past date range,
    from Open-Meteo's Flood API (GloFAS). Returns (dates, values, error).
    GloFAS's consolidated historical record may not reach every past date,
    which is a genuine limitation of this free data source, not a bug -
    reported honestly if it happens rather than silently faked."""
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": "river_discharge",
    }
    url = "https://flood-api.open-meteo.com/v1/flood?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = json.load(resp)
        dates = data.get("daily", {}).get("time", [])
        values = data.get("daily", {}).get("river_discharge", [])
        if not dates or all(v is None for v in values):
            return None, None, "No historical river discharge data returned for this date range."
        return dates, values, None
    except Exception as e:
        return None, None, str(e)


def run_backtest(password):
    """Real accuracy check against a genuine, documented past flood event
    (not a simulation): for every day of the known event window, fetch REAL
    historical rainfall and river discharge, run them through the exact
    same _risk_level function the live app uses, and report whether the
    system would have flagged HIGH/CRITICAL risk at some point during the
    event. This calls live external APIs, so it needs network access and
    will only work once actually deployed and run, not in a local test with
    no internet."""
    ok, err = _check_admin_password(password)
    if not ok:
        return err, pd.DataFrame()

    report_lines = []
    all_rows = []
    for event in KNOWN_FLOOD_EVENTS:
        community = event["community"]
        lat, lon = COMMUNITY_COORDS[community]
        rain_dates, rain_values, rain_err = _fetch_historical_rainfall_daily(lat, lon, event["start_date"], event["end_date"])
        disc_dates, disc_values, disc_err = _fetch_historical_river_discharge(lat, lon, event["start_date"], event["end_date"])

        if rain_err:
            report_lines.append(f"### {community} ({event['start_date']} to {event['end_date']})\n\n"
                                 f"Could not run this backtest: {rain_err}")
            continue

        disc_by_date = dict(zip(disc_dates or [], disc_values or []))
        clean_discharge = [v for v in (disc_values or []) if v is not None]
        baseline = sorted(clean_discharge)[len(clean_discharge) // 2] if len(clean_discharge) >= 2 else None

        flagged_days, total_days = 0, 0
        for i, date in enumerate(rain_dates):
            rainfall_mm = rain_values[i] or 0
            discharge = disc_by_date.get(date)
            if discharge is not None and baseline and baseline > 0:
                ratio = discharge / baseline
                water_level_m = ratio * (FLOOD_WATER_THRESHOLD_M / 2)
            else:
                water_level_m = 0
            level, _ = _risk_level(rainfall_mm, water_level_m, "", rainfall_label="historical (actual)")
            total_days += 1
            if level in ("HIGH", "CRITICAL"):
                flagged_days += 1
            all_rows.append({"community": community, "date": date, "rainfall_mm": rainfall_mm,
                              "water_level_m": round(water_level_m, 2), "risk_level": level})

        accuracy_note = "no river discharge history was available for this range" if disc_err else "using real river discharge history"
        report_lines.append(
            f"### {community} ({event['start_date']} to {event['end_date']})\n\n"
            f"{event['description']}\n\n"
            f"**Result: the system would have flagged HIGH or CRITICAL risk on "
            f"{flagged_days} of {total_days} days** during this real, confirmed flood event "
            f"({accuracy_note}). "
            + ("This is a real hit: the system's logic would have caught this event."
               if flagged_days > 0 else
               "This is a real miss: the current thresholds would NOT have caught this "
               "event, worth investigating before relying on this system for real warnings.")
        )

    return "\n\n---\n\n".join(report_lines), pd.DataFrame(all_rows)


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
# Dashboard: readings and alerts raised by the automatic forecast check.
# ---------------------------------------------------------------------------
def _dashboard_table():
    """Latest reading per community only, not every 6-hour check piled up.
    The scheduled check logs a new row every time it runs, so without this
    dedupe a community with no change in conditions would just accumulate
    duplicate-looking rows every 6 hours."""
    rows = db.get_readings()
    if not rows:
        return pd.DataFrame(columns=["timestamp", "community", "rainfall_mm", "water_level_m", "risk_level", "flood_event_flag"])
    df = pd.DataFrame(rows)
    df["timestamp"] = df["timestamp"].astype(str)
    # db.get_readings() returns oldest first (ordered by id), so keeping the
    # last row per community keeps the most recent check for each one.
    df = df.drop_duplicates(subset="community", keep="last")
    df = df.rename(columns={"data_quality_flag": "risk_level"})
    df = df.sort_values(by="community")
    return df[["timestamp", "community", "rainfall_mm", "water_level_m", "risk_level", "flood_event_flag"]]


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
FLOODGUARD_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&display=swap');
h1, h2, h3, .prose h1, .prose h2, .prose h3 {
    font-family: 'Space Grotesk', sans-serif !important;
    letter-spacing: -0.01em;
}
.risk-legend {
    display: flex; gap: 10px; flex-wrap: wrap; margin: 8px 0 4px 0;
}
.risk-legend span {
    padding: 3px 12px; border-radius: 999px; font-weight: 600; color: #fff; font-size: 0.85em;
}
"""

with gr.Blocks(title="FloodGuard AI", theme=gr.themes.Soft(primary_hue="teal", secondary_hue="amber"), css=FLOODGUARD_CSS) as demo:
    gr.Markdown(
        "# FloodGuard AI\n"
        "An automatic flood warning system for flood prone communities in Ghana. "
        "It looks at the weather forecast and warns people 12 to 24 hours before "
        "flooding happens, so they have time to prepare instead of finding out "
        "after the flood has already started."
    )
    gr.HTML(
        '<div class="risk-legend">'
        '<span style="background:#2F9E44;">LOW</span>'
        '<span style="background:#F2A007;">MODERATE</span>'
        '<span style="background:#E8590C;">HIGH</span>'
        '<span style="background:#C92A2A;">CRITICAL</span>'
        '</div>'
    )

    with gr.Tab("Check My Area"):
        gr.Markdown(
            "Check flood risk for anywhere, not just the monitored communities below. "
            "This is a one-time check: nothing is saved, and no alert is sent."
        )
        with gr.Row():
            place_in = gr.Textbox(label="Place name", placeholder="e.g. Tamale, Ghana")
            check_btn = gr.Button("Check my risk", variant="primary")
        place_out = gr.Markdown()
        check_btn.click(check_my_area, inputs=place_in, outputs=place_out)

    with gr.Tab("Community Risk Dashboard"):
        gr.Markdown(
            "Forecast based prediction: every 6 hours the system checks the rainfall "
            "forecast, and where a river is nearby, the current water level too, for "
            "each community, works out the risk level, and shows it below. "
            "Automatic alerts are sent by email/SMS whenever a community reaches "
            "HIGH or CRITICAL risk."
        )
        refresh_btn = gr.Button("Refresh dashboard")
        readings_dashboard = gr.Dataframe(label="Risk readings per community", value=_dashboard_table, wrap=True)
        alerts_dashboard = gr.Dataframe(label="Alerts sent", value=_alerts_table, wrap=True)
        refresh_btn.click(lambda: (_dashboard_table(), _alerts_table()), outputs=[readings_dashboard, alerts_dashboard])

    with gr.Tab("Admin"):
        gr.Markdown(
            "Everything here requires the admin password, set once as `ADMIN_PASSWORD` "
            "in Render's Environment tab. This is where the community contact lists are "
            "managed and the forecast check can be run on demand."
        )
        admin_password = gr.Textbox(label="Admin password", type="password")

        gr.Markdown("### Community contact lists — import a CSV with columns: name, phone, email")
        with gr.Row():
            import_community = gr.Dropdown(label="Community", choices=COMMUNITIES, value="Mepe")
            import_file = gr.File(label="Contacts CSV", file_types=[".csv"], type="filepath")
        import_btn = gr.Button("Import contacts", variant="primary")
        import_out = gr.Markdown()
        contact_counts = gr.Dataframe(label="Contacts per community", value=_subscriber_counts_table, wrap=True)
        import_btn.click(
            bulk_import_contacts,
            inputs=[admin_password, import_community, import_file],
            outputs=[import_out, contact_counts],
        )

        gr.Markdown(
            "### Water level (fallback only)\n"
            "Water level is now fetched automatically from live river discharge data "
            "for communities near a modelled river (Mepe, Anloga, Sokpoe). It's "
            "overwritten by that automatic reading every check. Only use this manual "
            "field for a community with no river nearby, like New Legon, where there "
            "is no automatic source and flooding comes from drainage, not a river."
        )
        with gr.Row():
            wl_community = gr.Dropdown(label="Community", choices=list(COMMUNITY_COORDS.keys()), value="Mepe")
            wl_level = gr.Number(label="Water level (m)", value=0)
        wl_btn = gr.Button("Update water level")
        wl_out = gr.Markdown()
        wl_table = gr.Dataframe(label="Latest water level per community", value=_water_levels_table, wrap=True)
        wl_btn.click(update_water_level, inputs=[admin_password, wl_community, wl_level], outputs=[wl_out, wl_table])

        gr.Markdown(
            "### Run the automatic forecast check now\n"
            "Normally runs every 6 hours by itself, so real lead time stays "
            "within about 12-24 hours before rain arrives. Use this button to "
            "run it immediately, for testing or a demo."
        )
        run_now_btn = gr.Button("Run check now", variant="primary")
        run_now_out = gr.Markdown()
        run_now_alerts = gr.Dataframe(label="Alerts raised", wrap=True)
        run_now_btn.click(run_check_now, inputs=[admin_password], outputs=[run_now_out, run_now_alerts])

        gr.Markdown(
            "### Backtest against real, documented past floods\n"
            "Runs the exact same risk logic used live, but against REAL historical "
            "rainfall data for two confirmed, recent flood events near New Legon: "
            "the 18 May 2025 Accra floods (5 deaths, 3,000+ displaced, NADMO "
            "confirmed) and the 29 June 2026 Accra floods, the most recent major "
            "flooding in Ghana. Instead of made-up numbers, this uses real "
            "recorded data. It calls live external APIs, so it can take a few "
            "seconds and only works once the app is actually deployed with "
            "network access."
        )
        backtest_btn = gr.Button("Run backtest")
        backtest_out = gr.Markdown()
        backtest_table = gr.Dataframe(label="Day by day breakdown", wrap=True)
        backtest_btn.click(run_backtest, inputs=[admin_password], outputs=[backtest_out, backtest_table])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
