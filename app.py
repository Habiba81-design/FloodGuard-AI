"""
FloodGuard AI prototype pipeline.
Raw source upload, then real cleaning and missing data analysis, then
threshold and flood event detection, then risk classification, then a live
automatic forward-looking forecast checks and
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
    SMS (Ghana numbers, via Arkesel): ARKESEL_API_KEY, ARKESEL_SENDER_ID
        (the sender name you registered with Arkesel, max 11 characters).
    Optional: APP_URL (your app's link, shown in the 'stop alerts' text).
    Without these, sign-ups still work, alerts are
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
import re
import secrets
import smtplib
import ssl
import threading
import time as time_module
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from email.message import EmailMessage

import gradio as gr
import pandas as pd

import db

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
# missing SMTP or SMS setup never crashes the app.
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
    """Send one SMS through Arkesel's v2 API (Ghana). to_phone is +233XXXXXXXXX;
    Arkesel wants it without the +. Needs ARKESEL_API_KEY and ARKESEL_SENDER_ID."""
    api_key = os.environ.get("ARKESEL_API_KEY")
    sender = os.environ.get("ARKESEL_SENDER_ID")
    if not api_key or not sender:
        return False, "SMS not configured (missing ARKESEL_API_KEY / ARKESEL_SENDER_ID)."

    payload = json.dumps({
        "sender": sender,
        "message": body,
        "recipients": [to_phone.lstrip("+")],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://sms.arkesel.com/api/v2/sms/send",
        data=payload,
        headers={"api-key": api_key, "Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.load(resp)
        if str(data.get("status", "")).lower() == "success":
            return True, "sent"
        return False, f"Arkesel said: {str(data.get('message') or data)[:200]}"
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")[:200]
        except Exception:
            detail = ""
        return False, f"Arkesel error {e.code}: {detail}"
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
            "Follow any instructions given by local authorities or emergency services, even if they differ from this message.",
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


def _STOP_FOOTER():
    url = os.environ.get("APP_URL", "").strip()
    where = f"open {url}" if url else "open the FloodGuard app"
    return f"\n\nTo stop these alerts, {where} and use 'Stop alerts' with this phone number or email."


_RAIN_ONLY_STEPS = [
    "Expect heavy rain. Avoid low-lying roads, drains and river banks while it falls.",
    "Don't wait for the rain to start: move valuables and documents off the floor now.",
    "Keep your phone charged so you can receive updates.",
    "Check on elderly or disabled neighbours who may need help.",
]


def _rain_summary_text(rainfall_mm, window_start, window_end, info):
    """The rainfall part of an alert: how much, when, and how heavy."""
    info = info or {}
    lines = []
    if rainfall_mm is not None:
        lines.append(f"- Total rain expected in the next 24 hours: about {rainfall_mm:.0f} mm")
    if window_start:
        when = f"between {window_start} and {window_end}" if window_end and window_end != window_start else f"around {window_start}"
        hours = info.get("hours_until")
        lead = f" (starting in about {hours} hour{'s' if hours != 1 else ''})" if hours is not None and hours > 0 else " (starting very soon)"
        lines.append(f"- Heaviest rain expected: {when}{lead}")
    peak = info.get("peak_mm_h")
    if peak:
        lines.append(f"- Strongest rainfall in a single hour: about {peak:.0f} mm")
    return "\n".join(lines) if lines else "- Rainfall details are not available right now."


def _plain_language_message(community, level, rainfall_mm, water_level_m, reasoning,
                              forecast=False, window_start=None, window_end=None,
                              info=None, flood_alert=True, water_tracked=True):
    """flood_alert=False means this is a heavy-rain heads-up: the flood risk
    itself is only LOW/MODERATE, but a lot of rain is coming."""
    plain = _LEVEL_PLAIN.get(level, {"headline": "", "meaning": "", "steps": []})
    water_txt = f"{water_level_m:.1f} metres" if water_level_m is not None else "not available"
    water_line = (f"The river or water level being tracked for {community} is {water_txt}.\n\n"
                  if water_tracked else "")
    rain_block = _rain_summary_text(rainfall_mm, window_start, window_end, info)

    if flood_alert:
        title = f"FLOOD ALERT for {community}"
        headline = plain["headline"]
        meaning = plain["meaning"]
        steps = plain["steps"]
    else:
        title = f"HEAVY RAIN ALERT for {community}"
        headline = "Heavy rain is forecast for your area."
        meaning = ("The rain forecast is heavy enough to be worth preparing for, "
                   "even though the flood risk is not high right now.")
        steps = _RAIN_ONLY_STEPS

    steps_txt = "\n".join(f"{i}. {x}" for i, x in enumerate(steps, start=1))
    return (
        f"{title}\n"
        f"Flood risk: {level}\n\n"
        f"{headline}\n\n"
        f"This is an early warning based on the weather forecast.\n\n"
        f"Rain forecast:\n{rain_block}\n\n"
        f"{water_line}"
        f"What this means: {meaning}\n\n"
        f"What to do:\n{steps_txt}\n\n"
        f"Please also follow any guidance from local authorities and emergency services. "
        f"If you are unsure what to do, ask a neighbour, a community leader, "
        f"or call your local emergency number.\n\n"
        f"---\n"
        f"This message was sent automatically by FloodGuard AI. "
        f"For those who want the numbers behind this alert: {reasoning}"
        f"{_STOP_FOOTER()}"
    )


def _dispatch_outbound(community, level, reasoning, rainfall_mm=None, water_level_m=None,
                        forecast=False, window_start=None, window_end=None,
                        info=None, flood_alert=True, water_tracked=True):
    """Send a real email/SMS to everyone subscribed to this place. Called for
    both flood alerts (HIGH/CRITICAL risk) and heavy-rain heads-ups. The
    caller decides when to send; this just builds and delivers the message.
    Returns a short markdown summary."""
    targets = [
        s for s in db.get_subscribers()
        if s["community"].strip().lower() == community.strip().lower()
        or s["community"].strip().lower() == "all communities"
    ]

    if not targets:
        return f"\n\nNo contacts for {community} yet, so no outbound alert was sent."

    message = _plain_language_message(community, level, rainfall_mm, water_level_m, reasoning,
                                       forecast=forecast, window_start=window_start, window_end=window_end,
                                       info=info, flood_alert=flood_alert, water_tracked=water_tracked)
    hours = (info or {}).get("hours_until")
    when = f" in about {hours}h" if hours else ""
    if flood_alert:
        subject = f"⚠️ Flood alert: {level} risk in {community}{when}"
        sms_message = message
    else:
        subject = f"🌧️ Heavy rain expected in {community}{when}"
        sms_message = message

    sent_email, sent_sms, failed = 0, 0, 0
    reasons = set()
    for s in targets:
        if s.get("email"):
            ok, why = _send_email(s["email"], subject, message)
            sent_email += 1 if ok else 0
            failed += 0 if ok else 1
            if not ok:
                reasons.add(f"email: {why}")
        if s.get("phone"):
            ok, why = _send_sms(s["phone"], sms_message)
            sent_sms += 1 if ok else 0
            failed += 0 if ok else 1
            if not ok:
                reasons.add(f"SMS: {why}")

    summary = f"\n\n**Outbound alert:** {sent_email} email(s) and {sent_sms} SMS sent to contacts in {community}."
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
    Returns (total_mm, window_start, window_end, error, info). window_start
    and window_end are human readable strings like 'Mon 3:00 PM', or None if
    no meaningful rain is expected in the window. error is None on success, a
    short string on failure, never raises. info is a dict with
    'start_iso' (local date-time rain starts), 'hours_until' (about how many
    hours from now) and 'peak_mm_h' (heaviest single hour), or {} if unknown.

    Works anywhere: the forecast is requested in the place's own local
    timezone and the current hour is worked out from its UTC offset.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation",
        "forecast_days": 3,
        "timezone": "auto",
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
        times = data.get("hourly", {}).get("time", [])
        values = data.get("hourly", {}).get("precipitation", [])
        if not times or not values:
            return None, None, None, "No forecast data returned by the weather API.", {}

        # Forecast times are in the place's own local time ("timezone=auto"),
        # so shift the server's UTC clock by the place's UTC offset.
        offset_s = data.get("utc_offset_seconds", 0) or 0
        current_hour = (datetime.utcnow() + timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:00")
        try:
            start = times.index(current_hour)
        except ValueError:
            start = 0  # fall back to the start of the returned forecast

        window_times = times[start:start + 24]
        window_values = values[start:start + 24]
        if not window_values:
            return None, None, None, "Forecast window was empty.", {}
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

        info = {"peak_mm_h": max((v for v in window_values if v is not None), default=0)}
        if rain_hour_idxs:
            local_now = datetime.utcnow() + timedelta(seconds=offset_s)
            hours_until = max(0, round((first_dt - local_now).total_seconds() / 3600))
            info.update({"start_iso": first_dt.isoformat(), "hours_until": hours_until})
        return total_mm, window_start_str, window_end_str, None, info
    except Exception as e:
        return None, None, None, str(e), {}


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
    # Only check places somebody is actually subscribed to (people who signed
    # up for a place, plus any original community that has contacts).
    monitored = {}
    try:
        subs = db.get_subscribers()
        subscribed = {x["community"].strip().lower() for x in subs}
        for name, coords in COMMUNITY_COORDS.items():
            if name.lower() in subscribed or "all communities" in subscribed:
                monitored[name] = coords
        monitored.update(db.get_places())
    except Exception:
        pass
    if not monitored:
        return "No places have subscribers yet, so nothing was checked."
    for community, (lat, lon) in monitored.items():
        rainfall_mm, window_start, window_end, err, rain_info = _fetch_forecast_rainfall_mm(lat, lon)

        auto_level_m, ratio, wl_err = _fetch_river_water_level(lat, lon)
        if auto_level_m is not None:
            water_level_m = auto_level_m
            water_tracked = True
            water_source = f"automatic, from river discharge running at {ratio}x its recent normal level"
            db.set_water_level(community, water_level_m)  # keep the Admin table in sync
        else:
            # No modelled river here (e.g. a drainage-only area), so fall
            # back to whatever an admin last entered by hand, or 0.
            water_level_m = manual_water_levels.get(community, 0.0)
            water_tracked = community in manual_water_levels  # only if an admin entered one
            water_source = f"manual entry, no automatic river data available ({wl_err})"

        notes = "Automatic forecast-based check."
        if window_start:
            notes += f" Heaviest rain expected {window_start} to {window_end}."
        notes += f" Water level source: {water_source}."
        if err:
            notes += f" Rainfall forecast fetch failed, treated as 0mm: {err}"
        level, reasoning = _risk_level(rainfall_mm or 0, water_level_m, "", rainfall_label="forecast (next 24h)")

        flood_alert = level in ("HIGH", "CRITICAL")
        heavy_rain = ((rainfall_mm or 0) >= RAIN_ALERT_MM
                      or (rain_info.get("peak_mm_h") or 0) >= RAIN_ALERT_PEAK_MM_H)

        # Readings show current status only: drop this community's previous
        # reading before saving the new one. Alerts are NOT deleted: the
        # alert history is kept.
        if hasattr(db, "delete_reading_for"):
            db.delete_reading_for(community)
        db.add_reading(now, community, rainfall_mm or 0, water_level_m, level, flood_alert)

        # Send when flood risk is HIGH/CRITICAL, or when heavy rain is
        # forecast even if flood risk is lower. The forecast looks 24 hours
        # ahead and runs every 6 hours, so rain is first caught about 18-24
        # hours before it starts, and always at least 12 hours ahead unless
        # the forecast itself only firmed up later.
        if flood_alert or heavy_rain:
            alert_notes = notes if flood_alert else "Heavy rain alert. " + notes
            _raise_alert(now, community, rainfall_mm or 0, water_level_m, level, alert_notes, "scheduled")
            # One alert per place per rain day (and risk level), so people
            # aren't messaged every 6 hours about the same rain.
            day = (rain_info.get("start_iso") or datetime.now().isoformat())[:10]
            dedupe_key = f"forecast:{day}:{level}:{'flood' if flood_alert else 'rain'}"
            if not db.is_seen_key(community, dedupe_key):
                db.add_seen_key(community, dedupe_key)
                _dispatch_outbound(community, level, reasoning, rainfall_mm, water_level_m,
                                    forecast=True, window_start=window_start, window_end=window_end,
                                    info=rain_info, flood_alert=flood_alert, water_tracked=water_tracked)
        rain_display = f"{rainfall_mm}mm forecast" if rainfall_mm is not None else "unavailable"
        window_display = f", heaviest rain {window_start}-{window_end}" if window_start else ""
        lines.append(f"- **{community}**: {rain_display}{window_display}, water level {water_level_m}m ({water_source}) -> {_risk_badge_html(level)}")
    return "\n".join(lines)


def _http_json(url, headers=None, timeout=10):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def _nominatim_lookup(query):
    """OpenStreetMap search. Much better than a city database at villages,
    hamlets and small towns, and it understands 'Village, District, Country'.
    Returns (lat, lon, display_name) or None."""
    params = {"q": query, "format": "jsonv2", "limit": 1, "addressdetails": 1, "accept-language": "en"}
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    ua = os.environ.get("GEOCODER_USER_AGENT", "FloodGuardAI/1.0 (flood alert service)")
    data = _http_json(url, headers={"User-Agent": ua})
    if not data:
        return None
    r = data[0]
    addr = r.get("address", {}) or {}
    place = (r.get("name") or addr.get("hamlet") or addr.get("village") or addr.get("suburb")
             or addr.get("town") or addr.get("city") or addr.get("locality") or "")
    region = addr.get("state") or addr.get("region") or addr.get("county") or addr.get("state_district") or ""
    country = addr.get("country") or ""
    parts = []
    for p in (place, region, country):
        if p and p not in parts:
            parts.append(p)
    display = ", ".join(parts) or r.get("display_name", query)
    return float(r["lat"]), float(r["lon"]), display


def _open_meteo_lookup(name, country_hint=""):
    """Open-Meteo's geocoder (matches the place name only, so commas and
    extra words break it). Used as a backup, with the country as a filter."""
    params = {"name": name, "count": 10, "language": "en", "format": "json"}
    url = "https://geocoding-api.open-meteo.com/v1/search?" + urllib.parse.urlencode(params)
    results = _http_json(url).get("results") or []
    if not results:
        return None
    if country_hint:
        hint = country_hint.lower()
        matching = [x for x in results if hint in (x.get("country", "") or "").lower()
                    or hint == (x.get("country_code", "") or "").lower()]
        results = matching or results
    r = results[0]
    display = ", ".join(p for p in [r.get("name"), r.get("admin1"), r.get("country")] if p)
    return r["latitude"], r["longitude"], display


def _geocode_place(name):
    """Find any place, including villages and small towns. Accepts
    'Village, District, Country' style names or plain 'lat, lon' coordinates
    (for places that are on no map). Returns (lat, lon, display_name, error)."""
    if not name or not name.strip():
        return None, None, None, "Type a place name first."
    name = name.strip()

    # 1. Exact coordinates, e.g. "9.4075, -0.8533"
    m = re.fullmatch(r"\s*(-?\d{1,2}(?:\.\d+)?)\s*[,;\s]\s*(-?\d{1,3}(?:\.\d+)?)\s*", name)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon, f"Location {lat:.3f}, {lon:.3f}", None

    parts = [p.strip() for p in name.split(",") if p.strip()]
    first, last = parts[0], parts[-1]
    country_hint = last if len(parts) > 1 else ""

    # 2. Try several phrasings, most specific first, on OpenStreetMap
    queries = [name]
    if len(parts) > 2:
        queries.append(f"{first}, {last}")           # drop the middle (district)
    if len(parts) > 1:
        queries.append(first)                         # village name alone
    seen = set()
    for q in queries:
        if q in seen:
            continue
        seen.add(q)
        try:
            found = _nominatim_lookup(q)
        except Exception:
            found = None
        if found:
            return found[0], found[1], found[2], None
        time_module.sleep(1.1)  # Nominatim allows at most 1 request per second

    # 3. Backup: Open-Meteo's city database
    try:
        found = _open_meteo_lookup(first, country_hint)
        if found:
            return found[0], found[1], found[2], None
    except Exception:
        pass

    return None, None, None, (
        f"Could not find '{name}'. Try the village name plus its district and country "
        f"(for example 'Dungu, Tamale, Ghana'), a nearby bigger town, or type exact "
        f"coordinates like 9.40, -0.85."
    )


def check_my_area(place_name):
    """On-the-fly flood risk check for ANY location, not just the five fixed
    communities in COMMUNITY_COORDS. This is transient by design: it is not
    saved anywhere and no alert is sent. People who want ongoing alerts for
    the place sign up with the form below it (see send_signup_code)."""
    lat, lon, display_name, err = _geocode_place(place_name)
    if err:
        return f"**Could not check this location.** {err}"

    rainfall_mm, window_start, window_end, rain_err, rain_info = _fetch_forecast_rainfall_mm(lat, lon)
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
    if rain_info.get("hours_until"):
        window_line += f" That is about {rain_info['hours_until']} hours from now."
    if rain_info.get("peak_mm_h"):
        window_line += f" Strongest rainfall in a single hour: about {rain_info['peak_mm_h']:.0f}mm."

    return (
        f"### {display_name}\n\n"
        f"Risk level: {badge}\n\n"
        f"Rainfall: {rain_line}.{window_line}\n\n"
        f"{water_note}\n\n"
        f"---\n"
        f"*Want a warning before heavy rain reaches this area? Scroll down and "
        f"sign up with your phone number or email.*\n\n"
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


# ---------------------------------------------------------------------------
# Self-service sign-up: anyone picks a place, verifies their phone number or
# email with a 6-digit code, and is then alerted by the scheduled check
# whenever that place reaches HIGH or CRITICAL risk.
# ---------------------------------------------------------------------------
MAX_PLACES = int(os.environ.get("MAX_PLACES", "100"))
MAX_SUBSCRIBERS = int(os.environ.get("MAX_SUBSCRIBERS", "2000"))
# SMS is only offered for Ghana numbers (+233); everyone else uses email.
SMS_COUNTRY_CODE = "+233"

# A "heavy rain" alert is sent (even when flood risk is only LOW/MODERATE) if
# the next 24 hours bring at least this much rain in total, or at least this
# much in a single hour. Both can be changed with environment variables.
RAIN_ALERT_MM = float(os.environ.get("RAIN_ALERT_MM", "30"))
RAIN_ALERT_PEAK_MM_H = float(os.environ.get("RAIN_ALERT_PEAK_MM_H", "8"))
CODES_PER_HOUR = 3
MAX_CODE_ATTEMPTS = 5


def _normalize_contact(channel, raw):
    """Returns (value, error). SMS is Ghana only: numbers become +233XXXXXXXXX
    (a leading 0 is read as Ghana). Numbers from other countries are told to
    use email instead. Email works for everyone."""
    raw = (raw or "").strip()
    if not raw:
        return None, "Enter your phone number or email first."
    if channel == "SMS":
        num = re.sub(r"[\s\-().]", "", raw)
        if num.startswith("00"):
            num = "+" + num[2:]
        elif num.startswith("0"):
            num = SMS_COUNTRY_CODE + num[1:]
        elif num.startswith("233"):
            num = "+" + num
        if num.startswith("+") and not num.startswith(SMS_COUNTRY_CODE):
            return None, "SMS alerts are only available for Ghana (+233) phone numbers. Please choose Email instead."
        if not re.fullmatch(r"\+233\d{9}", num):
            return None, "That doesn't look like a valid Ghana phone number. Use a format like 0201234567 or +233201234567."
        return num, None
    email = raw.lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return None, "That doesn't look like a valid email address."
    return email, None


def send_signup_code(place_name, channel, contact, consent):
    if not consent:
        return "Please tick the box to agree to receive flood alerts."
    value, err = _normalize_contact(channel, contact)
    if err:
        return err
    lat, lon, display_name, geo_err = _geocode_place(place_name)
    if geo_err:
        return f"**Could not find that place.** {geo_err}"

    phone, email = (value, "") if channel == "SMS" else ("", value)
    try:
        if db.subscriber_signed_up(display_name, phone, email):
            return f"You're already signed up for **{display_name}** with this {('number' if phone else 'email')}."
        known_places = set(COMMUNITY_COORDS) | set(db.get_places())
        if display_name not in known_places and db.count_places() >= MAX_PLACES:
            return "Sign-ups for new places are paused right now because the system is at capacity. Please try again later."
        if db.count_subscribers() >= MAX_SUBSCRIBERS:
            return "Sign-ups are paused right now because the system is at capacity. Please try again later."
        if db.recent_codes_sent(value) >= CODES_PER_HOUR:
            return "Too many codes were requested for this contact. Please wait an hour and try again."
    except Exception as e:
        return f"Something went wrong on our side ({e}). Please try again."

    code = f"{secrets.randbelow(10**6):06d}"
    text = (f"Your FloodGuard AI verification code is {code}. It expires in 10 minutes. "
            f"If you didn't ask for this, ignore this message.")
    if channel == "SMS":
        ok, why = _send_sms(value, text)
    else:
        ok, why = _send_email(value, "Your FloodGuard AI verification code", text)
    if not ok:
        return f"We couldn't send the code: {why}"
    try:
        db.create_pending_signup(display_name, lat, lon, channel, value, code)
    except Exception as e:
        return f"Something went wrong on our side ({e}). Please try again."
    return (f"We sent a 6-digit code to **{value}** for **{display_name}**. "
            f"Enter it below within 10 minutes.")


def confirm_signup(channel, contact, code):
    value, err = _normalize_contact(channel, contact)
    if err:
        return err
    code = (code or "").strip()
    if not code:
        return "Enter the 6-digit code we sent you."
    try:
        pending = db.get_pending_signup(value)
        if not pending:
            return "No active code found for this contact (it may have expired). Tap 'Send me a code' again."
        if pending["attempts"] >= MAX_CODE_ATTEMPTS:
            db.delete_pending_signups(value)
            return "Too many wrong attempts. Please request a new code."
        if not secrets.compare_digest(code, pending["code"]):
            db.bump_pending_attempts(pending["id"])
            return "That code isn't right. Check it and try again."

        place = pending["place"]
        if place not in COMMUNITY_COORDS:
            db.add_place(place, pending["lat"], pending["lon"])
        phone, email = (value, "") if channel == "SMS" else ("", value)
        if not db.subscriber_signed_up(place, phone, email):
            db.add_subscriber("", place, phone, email)
        db.delete_pending_signups(value)
    except Exception as e:
        return f"Something went wrong on our side ({e}). Please try again."
    return (f"✅ You're signed up for **{place}**. We check the forecast every 6 hours and "
            f"will message you if flood risk there reaches HIGH or CRITICAL. "
            f"You can stop alerts any time using 'Stop alerts' below.")


def stop_alerts(channel, contact):
    value, err = _normalize_contact(channel, contact)
    if err:
        return err
    phone, email = (value, "") if channel == "SMS" else ("", value)
    try:
        removed = db.remove_subscriber_contact(phone or None, email or None)
    except Exception as e:
        return f"Something went wrong on our side ({e}). Please try again."
    if not removed:
        return "No alerts were set up for that contact."
    return f"Done. **{value}** has been removed from {removed} alert subscription(s)."


def clear_dashboard_and_alerts(password):
    """Admin only: wipes every stored reading and every raised alert, so the
    'Alerts raised' table starts empty.
    Contacts and water levels are not touched."""
    ok, err = _check_admin_password(password)
    if not ok:
        return err, _alerts_table()
    if not hasattr(db, "clear_readings_and_alerts"):
        return ("db.py is missing `clear_readings_and_alerts()`. Add it to db.py first.",
                _alerts_table())
    db.clear_readings_and_alerts()
    return "Cleared all risk readings and alerts.", _alerts_table()


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
# UI
# ---------------------------------------------------------------------------
FLOODGUARD_CSS = """
:root {
    --fg-deep: #0B3D3F;
    --fg-mid: #0F6466;
    --fg-accent: #14919B;
}

.gradio-container, h1, h2, h3, .prose h1, .prose h2, .prose h3, body, button, input, textarea {
    font-family: Arial, Helvetica, sans-serif !important;
    letter-spacing: -0.01em;
}

/* --- Hero banner: flood photo (from the Phase 1 proposal) as background, with a
   dark gradient overlay so the white text stays readable over it --- */
.fg-hero {
    position: relative;
    padding: 34px 26px 26px 26px !important;
    margin: -8px -8px 22px -8px !important;
    border-radius: 0 0 22px 22px;
    background:
        linear-gradient(135deg, rgba(11,61,63,0.82) 0%, rgba(15,100,102,0.78) 55%, rgba(20,145,155,0.65) 100%),
        url("{HERO_IMG}");
    background-blend-mode: normal;
    background-size: cover;
    background-position: center;
    box-shadow: 0 8px 22px rgba(11,61,63,0.28);
}
.fg-hero h1 {
    color: #ffffff !important;
    font-size: 2.15em !important;
    font-weight: 800 !important;
    margin: 0 0 8px 0 !important;
}
.fg-hero .prose p {
    color: rgba(255,255,255,0.93) !important;
    font-size: 1.02em !important;
    max-width: 680px;
    line-height: 1.55 !important;
}

.risk-legend {
    display: flex; gap: 10px; flex-wrap: wrap; margin: 18px 0 0 0;
}
.risk-legend span {
    padding: 4px 14px; border-radius: 999px; font-weight: 700; color: #fff; font-size: 0.8em;
    box-shadow: 0 2px 6px rgba(0,0,0,0.18);
    letter-spacing: 0.02em;
}


/* Bigger, bolder tab labels */
.tabs > .tab-nav button {
    font-weight: 700 !important;
    font-size: 1.03em !important;
}
"""

# Embed the hero photo (hero.jpg, next to this file) directly in the CSS so no
# static-file route is needed.
import base64
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "hero.jpg"), "rb") as _f:
    _hero_b64 = base64.b64encode(_f.read()).decode()
FLOODGUARD_CSS = FLOODGUARD_CSS.replace("{HERO_IMG}", "data:image/jpeg;base64," + _hero_b64)

with gr.Blocks(title="FloodGuard AI", theme=gr.themes.Soft(primary_hue="teal", secondary_hue="amber"), css=FLOODGUARD_CSS) as demo:
    with gr.Column(elem_classes="fg-hero"):
        gr.Markdown(
            "# 🌊 FloodGuard AI\n"
            "An automatic flood warning system. Sign up for your area and get a message "
            "when heavy rain puts it at risk. "
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

    with gr.Tab("Check & Get Alerts"):
        gr.Markdown(
            "### Check flood risk for anywhere, then get alerts\n"
            "Type any village, town or city (add the district and country for small places) "
            "to see its flood risk for the next 24 hours. "
            "Then sign up below and we'll message you automatically when heavy "
            "rain puts your area at HIGH or CRITICAL risk."
        )
        with gr.Row():
            place_in = gr.Textbox(label="Place name", placeholder="Village, district, country (e.g. Dungu, Tamale, Ghana)")
            check_btn = gr.Button("Check my risk", variant="primary")
        place_out = gr.Markdown()
        check_btn.click(check_my_area, inputs=place_in, outputs=place_out)

        gr.Markdown(
            "### Get alerts for this place\n"
            "Use the same place name as above. SMS is available for Ghana phone numbers; email works from anywhere. We'll send a code to confirm it's you."
        )
        with gr.Row():
            su_channel = gr.Radio(["SMS", "Email"], value="SMS", label="Send alerts by")
            su_contact = gr.Textbox(label="Phone number or email", placeholder="Ghana phone (0201234567) or any email")
        su_consent = gr.Checkbox(label="I agree to receive flood alerts for this place. I can stop any time.")
        su_send_btn = gr.Button("Send me a code", variant="primary")
        su_send_out = gr.Markdown()
        with gr.Row():
            su_code = gr.Textbox(label="6-digit code", max_lines=1)
            su_confirm_btn = gr.Button("Confirm")
        su_confirm_out = gr.Markdown()
        su_send_btn.click(send_signup_code, inputs=[place_in, su_channel, su_contact, su_consent], outputs=su_send_out)
        su_confirm_btn.click(confirm_signup, inputs=[su_channel, su_contact, su_code], outputs=su_confirm_out)

        gr.Markdown("📲 **Share this page** with your family, neighbours and community WhatsApp groups so they get warned too.")

        with gr.Accordion("Stop alerts", open=False):
            gr.Markdown("Enter the phone number or email you signed up with to stop all its alerts.")
            with gr.Row():
                stop_channel = gr.Radio(["SMS", "Email"], value="SMS", label="Signed up with")
                stop_contact = gr.Textbox(label="Phone number or email")
            stop_btn = gr.Button("Stop my alerts")
            stop_out = gr.Markdown()
            stop_btn.click(stop_alerts, inputs=[stop_channel, stop_contact], outputs=stop_out)

    # The Admin tab is hidden from subscribers. It only appears for whoever
    # opens the site with ?admin=1 at the end of the link, and every action
    # inside it still needs the admin password.
    with gr.Tab("Admin", visible=False) as admin_tab:
        with gr.Column():
            gr.Markdown(
                "### Admin access\n"
                "Everything on this page requires the admin password, set once "
                "as `ADMIN_PASSWORD` in Render's Environment tab. This is where "
                "community contact lists are managed and the forecast check can "
                "be run on demand."
            )
            admin_password = gr.Textbox(label="Admin password", type="password")

        with gr.Column():
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

        with gr.Column():
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

        with gr.Column():
            gr.Markdown(
                "### Run the automatic forecast check now\n"
                "Normally runs every 6 hours by itself, so real lead time stays "
                "within about 12-24 hours before rain arrives. Use this button to "
                "run it immediately, for testing or a demo."
            )
            run_now_btn = gr.Button("Run check now", variant="primary")
            run_now_out = gr.Markdown()
            run_now_alerts = gr.Dataframe(label="Alerts raised (history)", value=_alerts_table, wrap=True)
            run_now_btn.click(run_check_now, inputs=[admin_password], outputs=[run_now_out, run_now_alerts])

        with gr.Column():
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

        with gr.Column():
            gr.Markdown(
                "### Clear dashboard and alerts\n"
                "Deletes every stored risk reading and every raised alert. Contacts "
                "and water levels are kept."
            )
            clear_btn = gr.Button("Clear all readings and alerts", variant="stop")
            clear_out = gr.Markdown()
            clear_btn.click(
                clear_dashboard_and_alerts,
                inputs=[admin_password],
                outputs=[clear_out, run_now_alerts],
            )

    def _reveal_admin(request: gr.Request):
        show = False
        try:
            show = request.query_params.get("admin") == "1"
        except Exception:
            pass
        return gr.update(visible=show)

    demo.load(_reveal_admin, None, admin_tab)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
