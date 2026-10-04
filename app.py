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
            "Cancel or postpone travel and other plans for today.",
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
            "Review your schedule: postpone travel, market trips and farm work if you can, and move animals, farm inputs and stock to a safe place.",
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


def _rain_steps(level):
    """Advice for a heavy-rain alert where flood risk is only LOW or MODERATE.
    Covers planning the day around the rain and protecting belongings."""
    steps = [
        "Review your schedule: postpone travel, market trips and farm work during the rain if you can.",
        "Keep clear of streams, drains, river banks and low-lying roads while it falls.",
    ]
    if level == "MODERATE":
        steps.append("Flood chance is MODERATE: move documents, electronics, food stock and farm inputs off the floor or to higher ground now.")
    else:
        steps.append("Flood chance is LOW for now, but stay alert in case it changes, and keep valuables off the floor.")
    steps += [
        "Keep your phone charged so you can receive updates.",
        "Never walk or drive through flood water. Tell your family and neighbours.",
    ]
    return steps


def _rain_summary_text(rainfall_mm, window_start, window_end, info):
    """One-sentence rainfall summary: how much, how hard, and when."""
    info = info or {}
    if rainfall_mm is None:
        return "Rainfall details are not available right now."
    text = f"about {rainfall_mm:.0f} mm in the next 24 hours"
    peak = info.get("peak_mm_h")
    if peak and peak >= 1:
        text += f" (up to {peak:.0f} mm in one hour)"
    if window_start:
        when = f"between {window_start} and {window_end}" if window_end and window_end != window_start else f"around {window_start}"
        hours = info.get("hours_until")
        lead = f", starting in about {hours} hour{'s' if hours != 1 else ''}" if hours is not None and hours > 0 else ", starting very soon"
        text += f", heaviest {when}{lead}"
    return text + "."


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
        headline = f"Heavy rain is forecast for your area. Chance of flooding: {level}."
        meaning = ("The rain forecast is heavy enough to be worth preparing for, "
                   "even though the flood risk is not high right now. Plan your schedule "
                   "and protect your belongings before it starts.")
        steps = _rain_steps(level)

    return (
        f"{title}\n"
        f"Flood risk: {level}\n\n"
        f"{headline}\n\n"
        f"This is an early warning based on the weather forecast.\n\n"
        f"Rain forecast: {rain_block}\n\n"
        f"{water_line}"
        f"What this means: {meaning}\n\n"
        f"Please also follow any guidance from local authorities and emergency services. "
        f"If you are unsure what to do, ask a neighbour, a community leader, "
        f"or call your local emergency number.\n\n"
        f"---\n"
        f"This message was sent automatically by FloodGuard AI. "
        f"For those who want the numbers behind this alert: {reasoning}"
        f"{_STOP_FOOTER()}"
    )


def _short_sms(community, level, rainfall_mm, flood_alert, info, window_start):
    """A short text for SMS (long messages split into several paid parts)."""
    info = info or {}
    head = (f"FloodGuard FLOOD ALERT: {level} risk in {community}." if flood_alert
            else f"FloodGuard: heavy rain expected in {community}.")
    rain = f" About {rainfall_mm:.0f}mm in 24h." if rainfall_mm else ""
    hours = info.get("hours_until")
    when = (f" Starts in about {hours}h." if hours else (f" Rain around {window_start}." if window_start else ""))
    chance = f" Flood chance: {level}."
    if flood_alert:
        act = " Move people, animals and valuables to higher ground now. Avoid flood water."
    elif level == "MODERATE":
        act = " Plan your day around the rain and move valuables off the floor."
    else:
        act = " Plan your day around the rain and avoid low roads and drains."
    url = os.environ.get("APP_URL", "").strip()
    stop = f" Stop alerts: {url}" if url else " To stop, use 'Stop alerts' on the FloodGuard page."
    return head + rain + when + chance + act + stop


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
    else:
        subject = f"🌧️ Heavy rain expected in {community}{when} (flood chance: {level})"
    sms_message = _short_sms(community, level, rainfall_mm, flood_alert, info, window_start)

    alert_type = "Flood alert" if flood_alert else "Heavy rain"

    def _log(channel, recipient, ok, why):
        try:
            db.log_delivery(community, alert_type, level, channel, recipient, ok, why)
        except Exception:
            pass  # a logging problem must never stop an alert going out

    sent_email, sent_sms, failed = 0, 0, 0
    reasons = set()
    for s in targets:
        if s.get("email"):
            ok, why = _send_email(s["email"], subject, message)
            _log("Email", s["email"], ok, why)
            sent_email += 1 if ok else 0
            failed += 0 if ok else 1
            if not ok:
                reasons.add(f"email: {why}")
        if s.get("phone"):
            ok, why = _send_sms(s["phone"], sms_message)
            _log("SMS", s["phone"], ok, why)
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
_METEO_CACHE = {}


def _meteo_json(url, ttl=1800, timeout=10, retries=3, headers=None):
    """Fetch a weather-API URL as JSON, politely. Results are cached for
    `ttl` seconds so repeated checks of the same place do not hit the free
    API again, 'Too Many Requests' (429) and server errors are retried with a
    short wait, and if the API still refuses, an older cached copy is used
    rather than failing. Raises only if there is nothing to fall back on."""
    now = time_module.time()
    cached = _METEO_CACHE.get(url)
    if cached and now - cached[0] < ttl:
        return cached[1]
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.load(resp)
            if len(_METEO_CACHE) > 500:
                _METEO_CACHE.clear()
            _METEO_CACHE[url] = (now, data)
            return data
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code != 429 and e.code < 500:
                break
        except Exception as e:
            last_err = e
        if attempt < retries - 1:
            time_module.sleep(2 * (attempt + 1))
    if cached:
        return cached[1]  # stale is better than nothing
    raise last_err


def _metno_user_agent():
    """MET Norway requires a User-Agent naming the app plus a contact point.
    Set WEATHER_USER_AGENT on Render (e.g. 'FloodGuardAI/1.0 you@example.com')."""
    custom = os.environ.get("WEATHER_USER_AGENT", "").strip()
    if custom:
        return custom
    contact = os.environ.get("ALERT_FROM_EMAIL", "").strip() or "no-contact-set"
    return f"FloodGuardAI/1.0 {contact}"


def _hourly_precip_openmeteo(lat, lon, days):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation",
        "forecast_days": days,
        "timezone": "auto",
    }
    url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
    data = _meteo_json(url, retries=1)
    times = data.get("hourly", {}).get("time", [])
    values = data.get("hourly", {}).get("precipitation", [])
    if not times or not values:
        raise RuntimeError("No forecast data returned by the weather API.")
    return times, values, data.get("utc_offset_seconds", 0) or 0


def _hourly_precip_metno(lat, lon, days):
    """Backup forecast from MET Norway (free, no key, worldwide). Hourly
    values for roughly the first 2.5 days, then 6-hour totals that are spread
    evenly across their hours. Times are converted to the place's approximate
    local time (from its longitude)."""
    url = ("https://api.met.no/weatherapi/locationforecast/2.0/compact?"
           + urllib.parse.urlencode({"lat": round(lat, 4), "lon": round(lon, 4)}))
    data = _meteo_json(url, retries=2, headers={"User-Agent": _metno_user_agent()})
    series = data.get("properties", {}).get("timeseries", [])
    hourly = {}
    for entry in series:
        t = datetime.strptime(entry["time"], "%Y-%m-%dT%H:%M:%SZ")
        d = entry.get("data", {})
        if "next_1_hours" in d:
            hourly[t] = d["next_1_hours"].get("details", {}).get("precipitation_amount") or 0
        elif "next_6_hours" in d:
            amount = d["next_6_hours"].get("details", {}).get("precipitation_amount") or 0
            for k in range(6):
                hourly.setdefault(t + timedelta(hours=k), amount / 6)
    if not hourly:
        raise RuntimeError("No forecast data returned by the backup weather service.")
    offset_s = int(round(lon / 15.0)) * 3600
    keys = sorted(hourly)[: days * 24]
    times = [(k + timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:00") for k in keys]
    return times, [hourly[k] for k in keys], offset_s


def _hourly_precip(lat, lon, days=5):
    """Hourly rain forecast as (times, values, utc_offset_seconds). Uses
    Open-Meteo first; if it refuses (for example 'Too Many Requests'), falls
    back to MET Norway so the app keeps working."""
    try:
        return _hourly_precip_openmeteo(lat, lon, days)
    except Exception as e1:
        try:
            return _hourly_precip_metno(lat, lon, days)
        except Exception as e2:
            raise RuntimeError(f"{e1} (backup service also failed: {e2})")


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
    try:
        times, values, offset_s = _hourly_precip(lat, lon)

        # Forecast times are in the place's own local time, so shift the
        # server's UTC clock by the place's UTC offset.
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

        info = {
            "peak_mm_h": max((v for v in window_values if v is not None), default=0),
            "next6_mm": round(sum(v or 0 for v in window_values[:6]), 1),
        }
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
        data = _meteo_json(url, retries=1)
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
        # forecast even if flood risk is lower.
        # Only warn when the rain is expected within the next ALERT_LEAD_HOURS
        # (12 by default), so people are not left waiting for rain that is
        # still a day away and may come late. Flood risk driven by a river
        # (no rain start time) is still sent straight away.
        hours_until = rain_info.get("hours_until")
        starts_soon = hours_until is None or hours_until <= ALERT_LEAD_HOURS
        if (flood_alert or heavy_rain) and starts_soon:
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


def _find_next_heavy_rain(lat, lon, days=5):
    """Scan the hourly forecast for the NEXT heavy rain, up to `days` ahead.
    Heavy rain = a spell where the 24 hours from its start bring at least
    RAIN_ALERT_MM in total, or at least RAIN_ALERT_PEAK_MM_H in one hour.
    Returns (result_dict, error). result_dict has found=True with the start,
    end, peak hour, 24h total and hours until it starts, or found=False with
    the next 24 hours' total. Never raises."""
    try:
        times, values, offset_s = _hourly_precip(lat, lon, days)
        local_now = datetime.utcnow() + timedelta(seconds=offset_s)
        current_hour = local_now.strftime("%Y-%m-%dT%H:00")
        try:
            start = times.index(current_hour)
        except ValueError:
            start = 0
        vals = [v or 0 for v in values]

        for i in range(start, len(vals)):
            if vals[i] < 1.0:
                continue
            win = vals[i:i + 24]
            total, peak = sum(win), max(win)
            if total >= RAIN_ALERT_MM or peak >= RAIN_ALERT_PEAK_MM_H:
                rain_idx = [j for j, v in enumerate(win) if v >= 1.0]
                first_dt = datetime.fromisoformat(times[i])
                last_dt = datetime.fromisoformat(times[i + rain_idx[-1]])
                peak_dt = datetime.fromisoformat(times[i + win.index(peak)])
                return {
                    "found": True,
                    "total_mm": round(total, 1),
                    "peak_mm_h": round(peak, 1),
                    "start": first_dt.strftime("%a %-I:%M %p"),
                    "end": last_dt.strftime("%a %-I:%M %p"),
                    "peak_time": peak_dt.strftime("%a %-I:%M %p"),
                    "hours_until": max(0, round((first_dt - local_now).total_seconds() / 3600)),
                }, None
        return {"found": False, "next24_mm": round(sum(vals[start:start + 24]), 1)}, None
    except Exception as e:
        if "429" in str(e):
            return None, "The weather service is busy right now. Please wait a minute and try again."
        return None, str(e)


def _hours_phrase(hours):
    if hours <= 1:
        return "very soon"
    if hours < 48:
        return f"in about {hours} hours"
    return f"in about {round(hours / 24)} days"


def check_my_area(place_name):
    """On-the-fly check for ANY location: finds the next heavy rain in the
    forecast (up to 5 days ahead) and the flood risk that rain would bring.
    Transient by design: nothing is saved and no alert is sent. People who
    want ongoing alerts sign up with the form below (see send_signup_code)."""
    lat, lon, display_name, err = _geocode_place(place_name)
    if err:
        return f"**Could not check this location.** {err}"

    nxt, rain_err = _find_next_heavy_rain(lat, lon)
    if rain_err:
        return f"**Could not check the rain forecast for {display_name}.** {rain_err}"

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

    signup_note = (
        "*Want a warning before heavy rain reaches this area? Scroll down and "
        "sign up with your phone number or email.*"
    )

    if not nxt["found"]:
        level, reasoning = _risk_level(nxt["next24_mm"], water_level_m, "", rainfall_label="forecast (next 24h)")
        return (
            f"### {display_name}\n\n"
            f"**No heavy rain is forecast in the next 5 days.**\n\n"
            f"Flood risk: {_risk_badge_html(level)}\n\n"
            f"Rain in the next 24 hours: about {nxt['next24_mm']:.0f} mm.\n\n"
            f"{water_note}\n\n"
            f"---\n{signup_note}\n\n"
            f"Technical detail: {reasoning}"
        )

    level, reasoning = _risk_level(nxt["total_mm"], water_level_m, "", rainfall_label="forecast (24h from rain start)")
    when = _hours_phrase(nxt["hours_until"])
    window = (f"between **{nxt['start']}** and **{nxt['end']}**" if nxt["end"] != nxt["start"]
              else f"around **{nxt['start']}**")
    far_note = ("\n\n*This rain is more than a day away, so the forecast may still change.*"
                if nxt["hours_until"] > 24 else "")
    return (
        f"### {display_name}\n\n"
        f"🌧️ **Next heavy rain: {when}**\n\n"
        f"Flood risk from this rain: {_risk_badge_html(level)}\n\n"
        f"About {nxt['total_mm']:.0f} mm expected over 24 hours, {window}. "
        f"Strongest around {nxt['peak_time']} (about {nxt['peak_mm_h']:.0f} mm in one hour)."
        f"{far_note}\n\n"
        f"{water_note}\n\n"
        f"---\n{signup_note}\n\n"
        f"Technical detail: {reasoning}"
    )


def _subscribers_table():
    rows = db.get_subscribers_full()
    cols = ["subscribed_at", "place", "channel", "contact"]
    if not rows:
        return pd.DataFrame(columns=cols)
    out = []
    for r in rows:
        channel = "SMS" if r.get("phone") else "Email"
        out.append({"subscribed_at": r["subscribed_at"], "place": r["place"],
                    "channel": channel, "contact": r.get("phone") or r.get("email")})
    return pd.DataFrame(out, columns=cols)


def admin_show_subscribers():
    try:
        table = _subscribers_table()
    except Exception as e:
        return f"Could not load subscribers: {e}", _subscribers_table_empty()
    if table.empty:
        return "No subscribers yet.", table
    sms = int((table["channel"] == "SMS").sum())
    email = int((table["channel"] == "Email").sum())
    places = table["place"].nunique()
    return (f"**{len(table)} subscription(s)** across **{places} place(s)**: "
            f"{sms} by SMS, {email} by email."), table


def _subscribers_table_empty():
    return pd.DataFrame(columns=["subscribed_at", "place", "channel", "contact"])


def _deliveries_table_empty():
    return pd.DataFrame(columns=["sent_at", "place", "alert_type", "risk_level",
                                 "channel", "recipient", "status", "detail"])


def admin_show_alerts_sent():
    try:
        rows = db.get_deliveries()
    except Exception as e:
        return f"Could not load sent alerts: {e}", _deliveries_table_empty()
    if not rows:
        return "No alerts have been sent yet.", _deliveries_table_empty()
    table = pd.DataFrame(rows, columns=list(_deliveries_table_empty().columns))
    sent = int((table["status"] == "sent").sum())
    failed = int((table["status"] == "failed").sum())
    return (f"**{len(table)} message(s) recorded**: {sent} sent, {failed} failed "
            f"(showing the most recent {len(table)})."), table


def run_check_now(password):
    ok, err = _check_admin_password(password)
    if not ok:
        return err
    summary = run_scheduled_check()
    return f"Ran the check manually just now.\n\n{summary}"


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
# Send an alert only when the rain is due to start within this many hours.
ALERT_LEAD_HOURS = float(os.environ.get("ALERT_LEAD_HOURS", "12"))
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
        return "Please tick the box to agree to receive heavy rain and flood alerts."
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


def _welcome_info(place, lat, lon):
    """What a new subscriber is told right after signing up: the next heavy
    rain and the flood risk it brings, for their place. Returns a dict with
    'summary_md' (shown on the page), 'email_text', 'sms_text' and 'level',
    or None if the forecast could not be fetched (sign-up still succeeds)."""
    nxt, err = _find_next_heavy_rain(lat, lon)
    if err or not nxt:
        return None
    water_level_m, _ratio, _wl_err = _fetch_river_water_level(lat, lon)
    water_level_m = water_level_m or 0.0
    lead = f"{ALERT_LEAD_HOURS:g} hours"
    stop = _STOP_FOOTER()
    app_url = os.environ.get("APP_URL", "").strip()
    sms_stop = f" Stop: {app_url}" if app_url else ""

    if not nxt["found"]:
        level, _ = _risk_level(nxt["next24_mm"], water_level_m, "", rainfall_label="forecast (next 24h)")
        line = "No heavy rain is forecast in the next 5 days."
        detail = f"Flood risk right now: {level}."
        md = f"🌧️ **{line}**\n\n{detail}"
        sms = f"FloodGuard: you're signed up for {place}. {line} Flood risk: {level}."
    else:
        level, _ = _risk_level(nxt["total_mm"], water_level_m, "", rainfall_label="forecast (24h from rain start)")
        when = _hours_phrase(nxt["hours_until"])
        window = (f"between {nxt['start']} and {nxt['end']}" if nxt["end"] != nxt["start"]
                  else f"around {nxt['start']}")
        line = f"Next heavy rain: {when}."
        detail = (f"Flood risk from this rain: {level}. About {nxt['total_mm']:.0f} mm expected "
                  f"over 24 hours, {window}.")
        md = f"🌧️ **{line}**\n\n{detail}"
        sms = (f"FloodGuard: you're signed up for {place}. {line} Flood risk: {level}. "
               f"About {nxt['total_mm']:.0f} mm in 24h.")
    sms += f" We'll alert you when heavy rain is due within {lead}." + sms_stop
    email = (
        f"Welcome to FloodGuard AI. You're signed up for alerts for {place}.\n\n"
        f"{line}\n{detail}\n\n"
        f"We check the forecast every 6 hours and will message you if heavy rain is due "
        f"within {lead} or flood risk reaches HIGH or CRITICAL."
        f"{stop}"
    )
    return {"summary_md": md, "email_text": email, "sms_text": sms, "level": level}


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

    # Welcome message with the next heavy rain for this place. The sign-up is
    # already saved, so nothing here can make it fail.
    welcome_md, welcome_sent = "", False
    try:
        info = _welcome_info(place, pending["lat"], pending["lon"])
        if info:
            welcome_md = "\n\n" + info["summary_md"]
            if channel == "SMS":
                welcome_sent, why = _send_sms(value, info["sms_text"])
            else:
                welcome_sent, why = _send_email(value, f"Welcome to FloodGuard AI: {place}", info["email_text"])
            if welcome_sent:
                welcome_md += f"\n\n*We also sent this to your {'phone' if channel == 'SMS' else 'email'}.*"
    except Exception:
        pass
    return (f"✅ You're signed up for **{place}**. We check the forecast every 6 hours and "
            f"will message you if heavy rain is expected or flood risk there reaches HIGH or CRITICAL. "
            f"You can stop alerts any time using 'Stop alerts' below."
            f"{welcome_md}")


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
            "An automatic heavy rain and flood warning system. Sign up for your area and get a message "
            "when heavy rain is coming or flooding is likely. "
            "It looks at the weather forecast and warns people up to 12 hours before "
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
            "### See the next heavy rain and its flood risk, for anywhere\n"
            "Type any village, town or city (add the district and country for small places) "
            "to see when the next heavy rain is expected and how likely it is to cause flooding. "
            "Then sign up below and we'll message you automatically when heavy "
            "rain is forecast for your area or flood risk reaches HIGH or CRITICAL."
        )
        with gr.Row():
            place_in = gr.Textbox(label="Place name", placeholder="Village, district, country (e.g. Dungu, Tamale, Ghana)")
            check_btn = gr.Button("Check next heavy rain", variant="primary")
        place_out = gr.Markdown()
        check_btn.click(check_my_area, inputs=place_in, outputs=place_out)

        gr.Markdown(
            "### Get alerts for this place\n"
            "Use the same place name as above. SMS is available for Ghana phone numbers; email works from anywhere. We'll send a code to confirm it's you."
        )
        with gr.Row():
            su_channel = gr.Radio(["SMS", "Email"], value="SMS", label="Send alerts by")
            su_contact = gr.Textbox(label="Phone number or email", placeholder="Ghana phone (0201234567) or any email")
        su_consent = gr.Checkbox(label="I agree to receive heavy rain and flood alerts for this place. I can stop any time.")
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

    # The Records tab is hidden from subscribers. It only appears for whoever
    # opens the site with ?records=<RECORDS_KEY> at the end of the link, where
    # RECORDS_KEY is a long secret set on Render. If RECORDS_KEY is not set,
    # the tab never appears. No password is needed to view subscribers and
    # alerts sent, but "Run check now" sends real messages, so it still
    # needs the admin password.
    with gr.Tab("Records", visible=False) as admin_tab:
        with gr.Column():
            gr.Markdown(
                "### Records\n"
                "Who has subscribed and which alerts were sent."
            )

        with gr.Column():
            gr.Markdown("### Subscribers\nEveryone who signed up for alerts, with when they joined.")
            subs_btn = gr.Button("Show subscribers", variant="primary")
            subs_out = gr.Markdown()
            subs_table = gr.Dataframe(label="Subscribers", value=_subscribers_table_empty, wrap=True)
            subs_btn.click(admin_show_subscribers, inputs=None, outputs=[subs_out, subs_table])

        with gr.Column():
            gr.Markdown("### Alerts sent\nEvery heavy rain or flood alert sent to a subscriber, and whether it was delivered.")
            sent_btn = gr.Button("Show alerts sent", variant="primary")
            sent_out = gr.Markdown()
            sent_table = gr.Dataframe(label="Alerts sent", value=_deliveries_table_empty, wrap=True)
            sent_btn.click(admin_show_alerts_sent, inputs=None, outputs=[sent_out, sent_table])

        with gr.Column():
            gr.Markdown(
                "### Run the automatic forecast check now (owner only)\n"
                "Normally runs every 6 hours by itself. This sends real alerts to "
                "subscribers when the rules are met, so it needs the admin password."
            )
            admin_password = gr.Textbox(label="Admin password", type="password")
            run_now_btn = gr.Button("Run check now")
            run_now_out = gr.Markdown()
            run_now_btn.click(run_check_now, inputs=[admin_password], outputs=[run_now_out])

    def _reveal_admin(request: gr.Request):
        show = False
        try:
            key = os.environ.get("RECORDS_KEY", "").strip()
            given = (request.query_params.get("records") or "").strip()
            show = bool(key) and secrets.compare_digest(given, key)
        except Exception:
            pass
        return gr.update(visible=show)

    demo.load(_reveal_admin, None, admin_tab)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
