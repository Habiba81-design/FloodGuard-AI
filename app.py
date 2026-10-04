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
    APP_URL (your app's public link, e.g. https://your-app.onrender.com). It is used to
        build the private unsubscribe link in every email and SMS. On Render it is
        picked up automatically from RENDER_EXTERNAL_URL if APP_URL is not set.
    Rain alerts (all optional): RAIN_ALERT_MIN_INTENSITY ("light", "moderate" or
        "heavy"; default "heavy"; "light" means alert for any rain), RAIN_MODERATE_MM_H (2.5),
        RAIN_ALERT_PEAK_MM_H (8), RAIN_ALERT_MM (30), RAIN_MODERATE_TOTAL_MM (10),
        RAIN_MIN_TOTAL_MM (1), RAIN_HOUR_MM (0.5), ALERT_LEAD_HOURS (12).
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
def _send_email_brevo(to_email, subject, body, unsubscribe_url=None):
    """Send via Brevo's HTTPS API. Works on Render's free tier, which blocks
    SMTP ports. Needs BREVO_API_KEY and ALERT_FROM_EMAIL (a sender you have
    verified in Brevo)."""
    api_key = os.environ.get("BREVO_API_KEY")
    from_email = os.environ.get("ALERT_FROM_EMAIL")
    if not api_key or not from_email:
        return False, "Brevo not configured (missing BREVO_API_KEY / ALERT_FROM_EMAIL)."
    def _post(with_headers):
        data = {
            "sender": {"name": "FloodGuard AI", "email": from_email},
            "to": [{"email": to_email}],
            "subject": subject,
            "textContent": body,
        }
        if with_headers and unsubscribe_url:
            # Lets Gmail/Outlook show their own "Unsubscribe" button.
            data["headers"] = {"List-Unsubscribe": f"<{unsubscribe_url}>",
                               "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"}
        req = urllib.request.Request(
            "https://api.brevo.com/v3/smtp/email",
            data=json.dumps(data).encode("utf-8"),
            headers={"api-key": api_key, "content-type": "application/json", "accept": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return (True, "sent") if resp.status in (200, 201, 202) else (False, f"Brevo returned {resp.status}")

    try:
        try:
            return _post(True)
        except urllib.error.HTTPError as e:
            if e.code == 400 and unsubscribe_url:
                return _post(False)  # header rejected: send without it rather than lose the alert
            raise
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")[:200]
        except Exception:
            detail = ""
        return False, f"Brevo error {e.code}: {detail}"
    except Exception as e:
        return False, str(e)


def _send_email(to_email, subject, body, unsubscribe_url=None):
    # Prefer the HTTPS API (works on Render free tier); fall back to SMTP.
    if os.environ.get("BREVO_API_KEY"):
        return _send_email_brevo(to_email, subject, body, unsubscribe_url)

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
    if unsubscribe_url:
        msg["List-Unsubscribe"] = f"<{unsubscribe_url}>"
        msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
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


def _app_base_url():
    """Public address of this app: APP_URL, or Render's own URL if that is set."""
    return (os.environ.get("APP_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")


def _unsub_url(token):
    """The private unsubscribe link for one subscriber ('' if it can't be built)."""
    base = _app_base_url()
    return f"{base}/unsubscribe?token={token}" if base and token else ""


def _STOP_FOOTER(unsub_url=""):
    if not unsub_url:
        return ""
    return f"\n\nTo stop getting these alerts, open this private link: {unsub_url}"




# ---------------------------------------------------------------------------
# Rain strength: light / moderate / heavy
# ---------------------------------------------------------------------------
_INTENSITY_RANK = {"light": 1, "moderate": 2, "heavy": 3}
_RAIN_FEEL = {"light": "drizzle or gentle rain", "moderate": "steady rain", "heavy": "intense rain"}
_RAIN_MEANING = {
    "light": (
        "Only light rain is expected. It is unlikely to cause problems by itself, but "
        "roads can get wet and slippery and anything left outside may get damp."
    ),
    "moderate": (
        "Steady rain is expected. It can make travel and outdoor work difficult, and "
        "water may gather on low roads and around drains."
    ),
    "heavy": (
        "The rain forecast is heavy enough to be worth preparing for, even though the "
        "flood risk is not high right now. Plan your schedule and protect your "
        "belongings before it starts."
    ),
}


def _rain_intensity(peak_mm_h, total_mm):
    """Classify a spell of rain as 'light', 'moderate' or 'heavy' from its
    strongest single hour and its 24 hour total. Returns None when there is
    too little rain to be worth mentioning.

    heavy    : at least RAIN_ALERT_PEAK_MM_H in one hour, or RAIN_ALERT_MM in 24h
    moderate : at least RAIN_MODERATE_MM_H in one hour, or RAIN_MODERATE_TOTAL_MM in 24h
    light    : anything else that still adds up to RAIN_MIN_TOTAL_MM
    """
    peak = float(peak_mm_h or 0)
    total = float(total_mm or 0)
    if total < RAIN_MIN_TOTAL_MM:
        return None
    if peak >= RAIN_ALERT_PEAK_MM_H or total >= RAIN_ALERT_MM:
        return "heavy"
    if peak >= RAIN_MODERATE_MM_H or total >= RAIN_MODERATE_TOTAL_MM:
        return "moderate"
    return "light"


def _intensity_badge_html(intensity):
    """A small blue pill for light / moderate / heavy rain (blues, so it is
    not confused with the green-to-red flood risk pills)."""
    colors = {"light": "#4DABF7", "moderate": "#1C7ED6", "heavy": "#5F3DC4"}
    color = colors.get(intensity, "#666")
    return (
        f'<span style="display:inline-block;padding:3px 12px;border-radius:999px;'
        f'font-weight:600;color:#fff;background:{color};">{(intensity or "").upper()}</span>'
    )


def _mm(x):
    """Rainfall for people: whole numbers when large, one decimal when small."""
    x = float(x or 0)
    return f"{x:.0f}" if x >= 10 else f"{x:.1f}"


def _rain_steps(level, intensity="heavy"):
    """Advice for a rain alert where flood risk is only LOW or MODERATE,
    matched to how strong the rain is."""
    if intensity == "light":
        steps = [
            "Carry an umbrella or raincoat if you go out, and take care on wet, slippery roads.",
            "Normal activities can go on, but bring in washing and cover anything that must stay dry.",
        ]
    elif intensity == "moderate":
        steps = [
            "Plan your day around the rain: travel, market trips and farm work may be wet and slow.",
            "Cover or bring in washing, grain, stock and anything that must stay dry.",
            "Keep clear of streams, drains and river banks, and watch for water gathering on low roads.",
        ]
    else:
        steps = [
            "Review your schedule: postpone travel, market trips and farm work during the rain if you can.",
            "Keep clear of streams, drains, river banks and low-lying roads while it falls.",
        ]
    if level == "MODERATE":
        steps.append("Flood chance is MODERATE: move documents, electronics, food stock and farm inputs off the floor or to higher ground now.")
    elif intensity != "light":
        steps.append("Flood chance is LOW for now, but stay alert in case it changes, and keep valuables off the floor.")
    if intensity != "light":
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
        if info.get("raining_now"):
            if window_end and window_end != window_start:
                text += f", rain is falling now and is expected to last until about {window_end}"
            else:
                text += ", rain is falling now"
        else:
            when = f"between {window_start} and {window_end}" if window_end and window_end != window_start else f"around {window_start}"
            hours = info.get("hours_until")
            lead = f", starting in about {hours} hour{'s' if hours != 1 else ''}" if hours is not None and hours > 0 else ", starting very soon"
            text += f", expected {when}{lead}"
    return text + "."


def _plain_language_message(community, level, rainfall_mm, water_level_m, reasoning,
                              forecast=False, window_start=None, window_end=None,
                              info=None, flood_alert=True, water_tracked=True, unsub_url=""):
    """flood_alert=False means this is a rain heads-up: the flood risk itself
    is only LOW/MODERATE, but rain (light, moderate or heavy) is coming."""
    info = info or {}
    plain = _LEVEL_PLAIN.get(level, {"headline": "", "meaning": "", "steps": []})
    water_txt = f"{water_level_m:.1f} metres" if water_level_m is not None else "not available"
    water_line = (f"The river or water level being tracked for {community} is {water_txt}.\n\n"
                  if water_tracked else "")
    rain_block = _rain_summary_text(rainfall_mm, window_start, window_end, info)
    intensity = info.get("intensity")
    strength_line = (f"Rain strength: {intensity.upper()} ({_RAIN_FEEL[intensity]})\n\n"
                     if intensity in _RAIN_FEEL else "")

    if flood_alert:
        title = f"FLOOD ALERT for {community}"
        headline = plain["headline"]
        meaning = plain["meaning"]
        steps = plain["steps"]
    else:
        label = intensity if intensity in _RAIN_MEANING else "moderate"
        title = f"{label.upper()} RAIN ALERT for {community}"
        headline = f"{label.capitalize()} rain is forecast for your area. Chance of flooding: {level}."
        meaning = _RAIN_MEANING[label]
        steps = _rain_steps(level, label)
    steps_block = "What to do:\n" + "\n".join(f"- {x}" for x in steps) + "\n\n" if steps else ""

    return (
        f"{title}\n"
        f"Flood risk: {level}\n\n"
        f"{headline}\n\n"
        f"This is an early warning based on the weather forecast.\n\n"
        f"{strength_line}"
        f"Rain forecast: {rain_block}\n\n"
        f"{water_line}"
        f"What this means: {meaning}\n\n"
        f"{steps_block}"
        f"Please also follow any guidance from local authorities and emergency services. "
        f"If you are unsure what to do, ask a neighbour, a community leader, "
        f"or call your local emergency number.\n\n"
        f"---\n"
        f"This message was sent automatically by FloodGuard AI. "
        f"For those who want the numbers behind this alert: {reasoning}"
        f"{_STOP_FOOTER(unsub_url)}"
    )


def _short_sms(community, level, rainfall_mm, flood_alert, info, window_start, unsub_url=""):
    """A short text for SMS (long messages split into several paid parts)."""
    info = info or {}
    intensity = info.get("intensity") if info.get("intensity") in _INTENSITY_RANK else "moderate"
    head = (f"FloodGuard FLOOD ALERT: {level} risk in {community}." if flood_alert
            else f"FloodGuard: {intensity} rain expected in {community}.")
    rain = f" About {rainfall_mm:.0f}mm in 24h." if rainfall_mm else ""
    strength = f" Rain: {intensity}." if flood_alert and info.get("intensity") else ""
    hours = info.get("hours_until")
    if info.get("raining_now"):
        when = " Rain is falling now."
    elif hours:
        when = f" Starts in about {hours}h."
    elif window_start:
        when = f" Rain around {window_start}."
    else:
        when = ""
    chance = f" Flood chance: {level}."
    if flood_alert:
        act = " Move people, animals and valuables to higher ground now. Avoid flood water."
    elif intensity == "light":
        act = " Carry an umbrella and take care on wet roads."
    elif intensity == "moderate":
        act = " Plan your day around the rain and cover your belongings."
    elif level == "MODERATE":
        act = " Plan your day around the rain and move valuables off the floor."
    else:
        act = " Plan your day around the rain and avoid low roads and drains."
    stop = f" Stop: {unsub_url}" if unsub_url else ""
    return head + strength + rain + when + chance + act + stop


def _dispatch_outbound(community, level, reasoning, rainfall_mm=None, water_level_m=None,
                        forecast=False, window_start=None, window_end=None,
                        info=None, flood_alert=True, water_tracked=True):
    """Send a real email/SMS to everyone subscribed to this place. Called for
    both flood alerts (HIGH/CRITICAL risk) and rain heads-ups (light, moderate or heavy). The
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
    raining_now = bool((info or {}).get("raining_now"))
    when = " now" if raining_now else (f" in about {hours}h" if hours else "")
    intensity = (info or {}).get("intensity")
    if intensity not in _INTENSITY_RANK:
        intensity = "moderate"
    if flood_alert:
        subject = f"⚠️ Flood alert: {level} risk in {community}{when}"
    else:
        subject = f"🌧️ {intensity.capitalize()} rain expected in {community}{when} (flood chance: {level})"
    alert_type = "Flood alert" if flood_alert else f"{intensity.capitalize()} rain"

    def _log(channel, recipient, ok, why):
        try:
            db.log_delivery(community, alert_type, level, channel, recipient, ok, why)
        except Exception as e:
            # a logging problem must never stop an alert going out, but it is
            # printed so it shows up in Render's logs instead of vanishing
            print(f"Could not record alert delivery for {recipient}: {e}")

    sent_email, sent_sms, failed = 0, 0, 0
    reasons = set()
    for s in targets:
        link = _unsub_url(s.get("unsub_token"))
        if s.get("email"):
            ok, why = _send_email(s["email"], subject, message + _STOP_FOOTER(link), unsubscribe_url=link or None)
            _log("Email", s["email"], ok, why)
            sent_email += 1 if ok else 0
            failed += 0 if ok else 1
            if not ok:
                reasons.add(f"email: {why}")
        if s.get("phone"):
            sms_message = _short_sms(community, level, rainfall_mm, flood_alert, info, window_start, unsub_url=link)
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
    out the specific block of hours when rain is expected, and says how
    strong it is (light, moderate or heavy), so people know *when* and *how
    hard*, not just that a wet day is coming.
    Returns (total_mm, window_start, window_end, error, info). window_start
    and window_end are human readable strings like 'Mon 3:00 PM', or None if
    no meaningful rain is expected in the window. error is None on success, a
    short string on failure, never raises. info is a dict with
    'intensity' ('light', 'moderate', 'heavy' or None if no meaningful rain),
    'raining_now' (True if the forecast has rain falling this hour),
    'start_iso' (local date-time rain starts), 'hours_until' (about how many
    hours from now) and 'peak_mm_h' (heaviest single hour), or {} if unknown.

    Works anywhere: the forecast is requested in the place's own local
    timezone and the current hour is worked out from its UTC offset.
    """
    try:
        times, values, offset_s = _hourly_precip(lat, lon)

        # Forecast times are in the place's own local time, so shift the
        # server's UTC clock by the place's UTC offset.
        local_now = datetime.utcnow() + timedelta(seconds=offset_s)
        current_hour = local_now.strftime("%Y-%m-%dT%H:00")
        try:
            start = times.index(current_hour)
        except ValueError:
            start = 0  # fall back to the start of the returned forecast

        window_times = times[start:start + 24]
        window_values = [v or 0 for v in values[start:start + 24]]
        if not window_values:
            return None, None, None, "Forecast window was empty.", {}
        total_mm = round(sum(window_values), 1)
        peak = max(window_values)

        # Hours with real rain (RAIN_HOUR_MM or more), so the alert can say
        # *when*, and the whole spell is classed light / moderate / heavy.
        rain_hour_idxs = [i for i, v in enumerate(window_values) if v >= RAIN_HOUR_MM]
        intensity = _rain_intensity(peak, total_mm) if rain_hour_idxs else None

        info = {
            "peak_mm_h": peak,
            "next6_mm": round(sum(window_values[:6]), 1),
            "intensity": intensity,
            "raining_now": False,
        }
        window_start_str, window_end_str = None, None
        if intensity:
            first_dt = datetime.fromisoformat(window_times[rain_hour_idxs[0]])
            last_dt = datetime.fromisoformat(window_times[rain_hour_idxs[-1]])
            window_start_str = first_dt.strftime("%a %-I:%M %p")
            window_end_str = last_dt.strftime("%a %-I:%M %p")
            hours_until = max(0, round((first_dt - local_now).total_seconds() / 3600))
            info.update({
                "start_iso": first_dt.isoformat(),
                "hours_until": hours_until,
                "raining_now": rain_hour_idxs[0] == 0,
            })
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
            notes += f" {(rain_info.get('intensity') or 'rain').capitalize()} rain expected {window_start} to {window_end}."
        notes += f" Water level source: {water_source}."
        if err:
            notes += f" Rainfall forecast fetch failed, treated as 0mm: {err}"
        level, reasoning = _risk_level(rainfall_mm or 0, water_level_m, "", rainfall_label="forecast (next 24h)")

        flood_alert = level in ("HIGH", "CRITICAL")
        intensity = rain_info.get("intensity")  # 'light' / 'moderate' / 'heavy' / None
        rain_alert = (intensity in _INTENSITY_RANK
                      and _INTENSITY_RANK[intensity] >= _INTENSITY_RANK[RAIN_ALERT_MIN_INTENSITY])

        # Readings show current status only: drop this community's previous
        # reading before saving the new one. Alerts are NOT deleted: the
        # alert history is kept.
        if hasattr(db, "delete_reading_for"):
            db.delete_reading_for(community)
        db.add_reading(now, community, rainfall_mm or 0, water_level_m, level, flood_alert)

        # Send when flood risk is HIGH/CRITICAL, or when rain of at least
        # RAIN_ALERT_MIN_INTENSITY (default: heavy) is
        # forecast, even if flood risk is low.
        # Only warn when the rain is expected within the next ALERT_LEAD_HOURS
        # (12 by default), so people are not left waiting for rain that is
        # still a day away and may come late. Flood risk driven by a river
        # (no rain start time) is still sent straight away.
        hours_until = rain_info.get("hours_until")
        starts_soon = hours_until is None or hours_until <= ALERT_LEAD_HOURS
        if (flood_alert or rain_alert) and starts_soon:
            alert_notes = notes if flood_alert else f"{intensity.capitalize()} rain alert. " + notes
            _raise_alert(now, community, rainfall_mm or 0, water_level_m, level, alert_notes, "scheduled")
            # One alert per place per rain day, risk level and rain strength,
            # so people aren't messaged every 6 hours about the same rain, but
            # ARE told again if the rain is upgraded (e.g. light to heavy).
            day = (rain_info.get("start_iso") or datetime.now().isoformat())[:10]
            dedupe_key = f"forecast:{day}:{level}:{intensity or 'none'}:{'flood' if flood_alert else 'rain'}"
            if not db.is_seen_key(community, dedupe_key):
                db.add_seen_key(community, dedupe_key)
                _dispatch_outbound(community, level, reasoning, rainfall_mm, water_level_m,
                                    forecast=True, window_start=window_start, window_end=window_end,
                                    info=rain_info, flood_alert=flood_alert, water_tracked=water_tracked)
        rain_display = f"{rainfall_mm}mm forecast" if rainfall_mm is not None else "unavailable"
        window_display = (f", {intensity} rain {window_start}-{window_end}" if window_start and intensity
                          else (f", rain {window_start}-{window_end}" if window_start else ""))
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


def _spell_from(times, vals, i, start, local_now):
    """Describe the spell of rain whose first rainy hour is index i: the 24
    hours from there, classed light / moderate / heavy. None if too little."""
    win = vals[i:i + 24]
    total, peak = sum(win), max(win)
    intensity = _rain_intensity(peak, total)
    if intensity is None:
        return None
    rain_idx = [j for j, v in enumerate(win) if v >= RAIN_HOUR_MM]
    first_dt = datetime.fromisoformat(times[i + rain_idx[0]])
    last_dt = datetime.fromisoformat(times[i + rain_idx[-1]])
    peak_dt = datetime.fromisoformat(times[i + win.index(peak)])
    return {
        "intensity": intensity,
        "total_mm": round(total, 1),
        "peak_mm_h": round(peak, 1),
        "start": first_dt.strftime("%a %-I:%M %p"),
        "end": last_dt.strftime("%a %-I:%M %p"),
        "peak_time": peak_dt.strftime("%a %-I:%M %p"),
        "hours_until": max(0, round((first_dt - local_now).total_seconds() / 3600)),
        "raining_now": i == start,
    }


def _find_next_rain(lat, lon, days=5):
    """Scan the hourly forecast for the NEXT rain of any strength, up to
    `days` ahead, and class it as light, moderate or heavy. If the first rain
    is not heavy but heavy rain is forecast later, that is returned too, as
    'heavy_later'. Returns (result_dict, error). result_dict has found=True
    with the spell's details, or found=False with the next 24 hours' total.
    Never raises."""
    try:
        times, values, offset_s = _hourly_precip(lat, lon, days)
        local_now = datetime.utcnow() + timedelta(seconds=offset_s)
        current_hour = local_now.strftime("%Y-%m-%dT%H:00")
        try:
            start = times.index(current_hour)
        except ValueError:
            start = 0
        vals = [v or 0 for v in values]

        first, heavy = None, None
        for i in range(start, len(vals)):
            if vals[i] < RAIN_HOUR_MM:
                continue
            spell = _spell_from(times, vals, i, start, local_now)
            if spell is None:
                continue
            if first is None:
                first = spell
            if spell["intensity"] == "heavy":
                heavy = spell
                break
        if first is None:
            return {"found": False, "next24_mm": round(sum(vals[start:start + 24]), 1)}, None
        result = dict(first)
        result["found"] = True
        result["heavy_later"] = heavy if (heavy and first["intensity"] != "heavy") else None
        return result, None
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


def _rain_when_phrase(spell):
    """'is forecast to be falling now' / 'is expected very soon' / 'is expected in about 5 hours'."""
    if spell.get("raining_now"):
        return "is forecast to be falling now"
    return "is expected " + _hours_phrase(spell["hours_until"])


def _rain_window_text(spell):
    start, end = spell["start"], spell["end"]
    if spell.get("raining_now"):
        return f"lasting until about {end}" if end != start else "falling now"
    return f"between {start} and {end}" if end != start else f"around {start}"


def check_my_area(place_name):
    """On-the-fly check for ANY location: finds the next rain in the forecast
    (up to 5 days ahead), says whether it is light, moderate or heavy, and
    the flood risk that rain would bring. Transient by design: nothing is
    saved and no alert is sent. People who want ongoing alerts sign up with
    the form below (see send_signup_code)."""
    lat, lon, display_name, err = _geocode_place(place_name)
    if err:
        return f"**Could not check this location.** {err}"

    nxt, rain_err = _find_next_rain(lat, lon)
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
        f"*Want a warning before {_ALERT_WORD} reaches this area? Scroll down and "
        "sign up with your phone number or email.*"
    )
    forecast_note = (
        "*Forecasts cover a wide area and can miss local showers or get the timing "
        "wrong. If it is raining where you are, trust what you see outside.*"
    )

    if not nxt["found"]:
        level, reasoning = _risk_level(nxt["next24_mm"], water_level_m, "", rainfall_label="forecast (next 24h)")
        return (
            f"### {display_name}\n\n"
            f"**No significant rain is forecast in the next 5 days.**\n\n"
            f"Flood risk: {_risk_badge_html(level)}\n\n"
            f"Rain in the next 24 hours: about {_mm(nxt['next24_mm'])} mm.\n\n"
            f"{water_note}\n\n"
            f"{forecast_note}\n\n"
            f"---\n{signup_note}\n\n"
            f"Technical detail: {reasoning}"
        )

    level, reasoning = _risk_level(nxt["total_mm"], water_level_m, "", rainfall_label="forecast (24h from rain start)")
    inten = nxt["intensity"]
    far_note = (
        "\n\n*This rain is more than a day away, so the forecast may still change.*"
        if not nxt["raining_now"] and nxt["hours_until"] > 24 else ""
    )
    later = nxt.get("heavy_later")
    later_note = ""
    if later:
        later_note = (
            f"\n\n⚠️ **Heavier rain later:** {_intensity_badge_html('heavy')} rain "
            f"{_hours_phrase(later['hours_until'])}, about {_mm(later['total_mm'])} mm over 24 hours, "
            f"around {later['start']}."
        )
    return (
        f"### {display_name}\n\n"
        f"🌧️ **{inten.capitalize()} rain {_rain_when_phrase(nxt)}**\n\n"
        f"Rain strength: {_intensity_badge_html(inten)} ({_RAIN_FEEL[inten]})\n\n"
        f"Flood risk from this rain: {_risk_badge_html(level)}\n\n"
        f"About {_mm(nxt['total_mm'])} mm expected over 24 hours, {_rain_window_text(nxt)}. "
        f"Strongest around {nxt['peak_time']} (about {_mm(nxt['peak_mm_h'])} mm in one hour)."
        f"{far_note}{later_note}\n\n"
        f"{water_note}\n\n"
        f"{forecast_note}\n\n"
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


def admin_show_subscribers(password):
    ok, err = _check_admin_password(password)
    if not ok:
        return err, _subscribers_table_empty()
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


def admin_show_alerts_sent(password):
    ok, err = _check_admin_password(password)
    if not ok:
        return err, _deliveries_table_empty()
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

# Rain is classed LIGHT, MODERATE or HEAVY, and a rain alert is sent (even when
# flood risk is only LOW/MODERATE) for rain at or above RAIN_ALERT_MIN_INTENSITY
# (default "heavy"; the Check page still shows rain of every strength).
# All of these can be changed with environment variables.
#   HEAVY    : at least RAIN_ALERT_PEAK_MM_H in one hour, or RAIN_ALERT_MM in 24h
#   MODERATE : at least RAIN_MODERATE_MM_H in one hour, or RAIN_MODERATE_TOTAL_MM in 24h
#   LIGHT    : any other rain that adds up to RAIN_MIN_TOTAL_MM
# An hour only counts as "raining" if it has RAIN_HOUR_MM or more.
# Set RAIN_ALERT_MIN_INTENSITY to "moderate" or "light" to send more rain
# alerts (each SMS costs credit, and light rain is common in the wet season).
RAIN_ALERT_MM = float(os.environ.get("RAIN_ALERT_MM", "30"))
RAIN_ALERT_PEAK_MM_H = float(os.environ.get("RAIN_ALERT_PEAK_MM_H", "8"))
RAIN_MODERATE_MM_H = float(os.environ.get("RAIN_MODERATE_MM_H", "2.5"))
RAIN_MODERATE_TOTAL_MM = float(os.environ.get("RAIN_MODERATE_TOTAL_MM", "10"))
RAIN_MIN_TOTAL_MM = float(os.environ.get("RAIN_MIN_TOTAL_MM", "1"))
RAIN_HOUR_MM = float(os.environ.get("RAIN_HOUR_MM", "0.5"))
RAIN_ALERT_MIN_INTENSITY = os.environ.get("RAIN_ALERT_MIN_INTENSITY", "heavy").strip().lower()
if RAIN_ALERT_MIN_INTENSITY not in _INTENSITY_RANK:
    RAIN_ALERT_MIN_INTENSITY = "heavy"
# How the alert scope is described to people, so sign-up text never promises
# more (or less) than the app will actually send.
_ALERT_WORD = {"light": "rain", "moderate": "moderate or heavy rain", "heavy": "heavy rain"}[RAIN_ALERT_MIN_INTENSITY]
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
        return f"Please tick the box to agree to receive {_ALERT_WORD} and flood alerts."
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


def _signup_forecast_md(lat, lon):
    """Shown on the page right after sign-up: the next rain (light, moderate
    or heavy) and its flood risk. Display only - no message is sent to the
    subscriber. Returns markdown, or None if the forecast could not be fetched
    (sign-up still succeeds)."""
    nxt, err = _find_next_rain(lat, lon)
    if err or not nxt:
        return None
    water_level_m, _ratio, _wl_err = _fetch_river_water_level(lat, lon)
    water_level_m = water_level_m or 0.0
    if not nxt["found"]:
        level, _ = _risk_level(nxt["next24_mm"], water_level_m, "", rainfall_label="forecast (next 24h)")
        line = "No significant rain is forecast in the next 5 days."
        detail = f"Flood risk right now: {level}."
    else:
        level, _ = _risk_level(nxt["total_mm"], water_level_m, "", rainfall_label="forecast (24h from rain start)")
        line = f"{nxt['intensity'].capitalize()} rain {_rain_when_phrase(nxt)}."
        detail = (f"Flood risk from this rain: {level}. About {_mm(nxt['total_mm'])} mm expected "
                  f"over 24 hours, {_rain_window_text(nxt)}.")
    return f"🌧️ **{line}**\n\n{detail}"


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

    # Show the next rain for this place on the page. No message is sent, and
    # the sign-up is already saved, so nothing here can make it fail.
    welcome_md = ""
    try:
        summary = _signup_forecast_md(pending["lat"], pending["lon"])
        if summary:
            welcome_md = "\n\n" + summary
    except Exception:
        pass
    return (f"✅ You're signed up for **{place}**. We check the forecast every 6 hours and "
            f"will message you if {_ALERT_WORD} is expected or flood risk there reaches HIGH or CRITICAL. "
            f"Every alert includes a private link to unsubscribe."
            f"{welcome_md}")


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
            "An automatic rain and flood warning system. Sign up for your area and get a message "
            f"when {_ALERT_WORD} is coming or flooding is likely. "
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
            "### See the next rain and its flood risk, for anywhere\n"
            "Type any village, town or city (add the district and country for small places) "
            "to see when the next rain is expected, how strong it will be, and how likely it is to cause flooding. "
            "Then sign up below and we'll message you automatically when "
            f"{_ALERT_WORD} is forecast for your area or flood risk reaches HIGH or CRITICAL.\n\n"
            "**Rain strength:** Light = drizzle or gentle rain. Moderate = steady rain (about 2.5 to 8 mm an hour). "
            "Heavy = 8 mm an hour or more, or 30 mm or more in a day."
        )
        with gr.Row():
            place_in = gr.Textbox(label="Place name", placeholder="Village, district, country (e.g. Dungu, Tamale, Ghana)")
            check_btn = gr.Button("Check next rain", variant="primary")
        place_out = gr.Markdown()
        check_btn.click(check_my_area, inputs=place_in, outputs=place_out)

        gr.Markdown(
            "### Get alerts for this place\n"
            "Use the same place name as above. SMS is available for Ghana phone numbers; email works from anywhere. We'll send a code to confirm it's you."
        )
        with gr.Row():
            su_channel = gr.Radio(["SMS", "Email"], value="SMS", label="Send alerts by")
            su_contact = gr.Textbox(label="Phone number or email", placeholder="Ghana phone (0201234567) or any email")
        su_consent = gr.Checkbox(label=f"I agree to receive {_ALERT_WORD} and flood alerts for this place. I can unsubscribe from any message.")
        su_send_btn = gr.Button("Send me a code", variant="primary")
        su_send_out = gr.Markdown()
        with gr.Row():
            su_code = gr.Textbox(label="6-digit code", max_lines=1)
            su_confirm_btn = gr.Button("Confirm")
        su_confirm_out = gr.Markdown()
        su_send_btn.click(send_signup_code, inputs=[place_in, su_channel, su_contact, su_consent], outputs=su_send_out)
        su_confirm_btn.click(confirm_signup, inputs=[su_channel, su_contact, su_code], outputs=su_confirm_out)

        gr.Markdown("📲 **Share this page** with your family, neighbours and community WhatsApp groups so they get warned too.")

    # The Admin tab is visible to everyone, but every action in it (viewing
    # subscribers, viewing alerts sent, running a check) needs ADMIN_PASSWORD.
    with gr.Tab("Admin"):
        with gr.Column():
            gr.Markdown(
                "### Admin\n"
                "Who has subscribed and which alerts were sent. Enter the admin password to use anything on this tab."
            )
            admin_password = gr.Textbox(label="Admin password", type="password")

        with gr.Column():
            gr.Markdown("### Subscribers\nEveryone who signed up for alerts, with when they joined.")
            subs_btn = gr.Button("Show subscribers", variant="primary")
            subs_out = gr.Markdown()
            subs_table = gr.Dataframe(label="Subscribers", value=_subscribers_table_empty, wrap=True)
            subs_btn.click(admin_show_subscribers, inputs=[admin_password], outputs=[subs_out, subs_table])

        with gr.Column():
            gr.Markdown("### Alerts sent\nEvery rain or flood alert sent to a subscriber, and whether it was delivered.")
            sent_btn = gr.Button("Show alerts sent", variant="primary")
            sent_out = gr.Markdown()
            sent_table = gr.Dataframe(label="Alerts sent", value=_deliveries_table_empty, wrap=True)
            sent_btn.click(admin_show_alerts_sent, inputs=[admin_password], outputs=[sent_out, sent_table])

        with gr.Column():
            gr.Markdown(
                "### Run the automatic forecast check now (owner only)\n"
                "Normally runs every 6 hours by itself. This sends real alerts to "
                "subscribers when the rules are met."
            )
            run_now_btn = gr.Button("Run check now")
            run_now_out = gr.Markdown()
            run_now_btn.click(run_check_now, inputs=[admin_password], outputs=[run_now_out])

# ---------------------------------------------------------------------------
# Private unsubscribe page. The link in each person's own email/SMS carries a
# long random token, so nobody can unsubscribe someone else by guessing or
# knowing their phone number or email. Opening the link only shows a confirm
# button (mail/SMS scanners often "open" links, and must not unsubscribe
# people by accident); pressing the button, or a mail app's one-click
# "Unsubscribe", sends a POST that does the removal.
# ---------------------------------------------------------------------------
def _unsub_page(title, message, button_token=None):
    import html as _html
    button = ""
    if button_token:
        button = (f'<form method="post" action="/unsubscribe?token={_html.escape(button_token)}">'
                  f'<input type="hidden" name="token" value="{_html.escape(button_token)}">'
                  f'<button type="submit">Yes, unsubscribe me</button></form>')
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_html.escape(title)}</title>
<style>
body{{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0b1018;color:#f1f5f9;
display:flex;min-height:100vh;align-items:center;justify-content:center;padding:20px}}
.card{{max-width:440px;background:#1e293b;border-radius:16px;padding:28px;text-align:center}}
h1{{font-size:1.25rem;margin:0 0 12px}} p{{line-height:1.5;margin:0 0 18px}}
button{{background:#0d9488;color:#fff;border:0;border-radius:10px;padding:12px 22px;font-size:1rem;font-weight:700;cursor:pointer}}
</style></head><body><div class="card"><h1>{_html.escape(title)}</h1><p>{_html.escape(message)}</p>{button}</div></body></html>"""


def _mask_contact(contact):
    contact = contact or ""
    if "@" in contact:
        name, _, domain = contact.partition("@")
        return (name[:2] + "***@" + domain) if name else contact
    return (contact[:4] + "****" + contact[-3:]) if len(contact) > 7 else "your number"


def _register_unsubscribe_routes(api):
    from fastapi import Form, Request
    from fastapi.responses import HTMLResponse

    @api.get("/unsubscribe", response_class=HTMLResponse)
    def unsubscribe_confirm(token: str = ""):
        try:
            sub = db.get_subscriber_by_token(token)
        except Exception:
            return HTMLResponse(_unsub_page("Something went wrong", "Please try the link again in a moment."), status_code=500)
        if not sub:
            return HTMLResponse(_unsub_page(
                "Link not valid",
                "This unsubscribe link isn't valid, or you have already been unsubscribed."), status_code=404)
        who = _mask_contact(sub.get("phone") or sub.get("email"))
        return HTMLResponse(_unsub_page(
            "Stop flood alerts?",
            f"This will stop all FloodGuard AI alerts sent to {who}.", button_token=token))

    @api.post("/unsubscribe", response_class=HTMLResponse)
    async def unsubscribe_do(request: Request, token: str = ""):
        # token comes from the confirm form (body) or from the link itself
        # (mail apps' one-click unsubscribe POSTs to the URL with the token in it)
        try:
            form = await request.form()
            token = form.get("token") or token
        except Exception:
            pass
        try:
            result = db.unsubscribe_by_token(token)
        except Exception:
            return HTMLResponse(_unsub_page("Something went wrong", "Please try again in a moment."), status_code=500)
        if not result:
            return HTMLResponse(_unsub_page(
                "Already unsubscribed",
                "This link isn't valid, or you have already been unsubscribed."), status_code=404)
        return HTMLResponse(_unsub_page(
            "You're unsubscribed",
            "You will no longer receive FloodGuard AI alerts. You can sign up again any time."))


if __name__ == "__main__":
    import uvicorn
    from fastapi import FastAPI

    port = int(os.environ.get("PORT", 7860))
    api = FastAPI()
    _register_unsubscribe_routes(api)
    # Gradio serves the main page; the unsubscribe routes above are registered
    # first so they take priority.
    app = gr.mount_gradio_app(api, demo, path="/")
    uvicorn.run(app, host="0.0.0.0", port=port)
