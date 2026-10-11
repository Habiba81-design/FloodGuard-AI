# FloodGuard AI

A rain and flood early-warning service for Ghana. Anyone can type in a village, town or city, see when the next rain is expected and how likely it is to cause flooding, and sign up to be messaged automatically before it arrives. Alerts go out by **SMS** (Ghana numbers) or **email** (anywhere).

Built with Python and Gradio, backed by Postgres, and deployed as a single web service.

## How it works

The service looks **forward**, not backward. It reads a rainfall forecast for the next 24 hours, so a warning can reach people before the rain starts instead of confirming a flood after it has begun.

```
Place name ──▶ Geocoding (OpenStreetMap Nominatim, Open-Meteo)
                      │
                      ▼
Hourly rain forecast ──┐   Open-Meteo (primary), MET Norway (backup)
River discharge ───────┤   Open-Meteo Flood API (GloFAS)
                      ▼
          Risk classification (rule-based)
                      │
                      ▼
      Alert + dedupe ──▶ Brevo (email) / Arkesel (SMS)
                      │
                      ▼
        Postgres (Supabase): subscribers, readings, alerts, delivery log
```

A background thread checks every subscribed place **every 6 hours**. Checking more often than once a day keeps the warning inside its 12 to 24 hour window.

## Rain strength

Each rain spell is classed from its heaviest single hour and its 24-hour total:

| Class | Rule (defaults) |
|---|---|
| Heavy | 8 mm in one hour, or 30 mm in 24 h |
| Moderate | 2.5 mm in one hour, or 10 mm in 24 h |
| Light | any other rain adding up to at least 1 mm |

An hour counts as "raining" at 0.5 mm or more. All of these values can be changed with environment variables (see Setup).

## Flood risk

Risk is a simple, explainable rule, deliberately not a black-box model, because people receiving a safety alert should be able to understand why they got it.

It compares the 24-hour rainfall forecast and a water-level signal against two fixed thresholds (`FLOOD_RAINFALL_THRESHOLD_MM = 80`, `FLOOD_WATER_THRESHOLD_M = 3.2`):

| Level | Condition |
|---|---|
| CRITICAL | both thresholds crossed together, **or** either one alone at 1.25x its threshold |
| HIGH | water level at 0.8x its threshold, or rainfall at 0.65x its threshold |
| MODERATE | water level at 0.6x its threshold, or rainfall at 0.3x its threshold |
| LOW | everything else |

Either signal can reach CRITICAL alone, because flooding has two different causes: a river rising, or heavy rain overwhelming drainage with no river involved.

### Water level is a proxy

Ghana has no public network of river gauges reporting live depth. Instead, the service takes modelled river discharge from the GloFAS system (via Open-Meteo's Flood API) and compares today's flow with that river's own median over the past 60 days. That ratio is scaled onto the metres scale used for the threshold: normal flow maps to half the threshold, and double normal flow maps to the threshold exactly.

This is an approximation, not a depth reading. Where no modelled river exists nearby (for example a drainage-only area like New Legon), the service uses a manually entered level if an admin has set one, and otherwise **0**. It does not invent a number.

## When an alert is sent

An alert goes out when **either**:

- flood risk is HIGH or CRITICAL, **or**
- forecast rain meets `RAIN_ALERT_MIN_INTENSITY` (default: heavy), even if flood risk is low,

and the rain is due to start within `ALERT_LEAD_HOURS` (default 12). Flood risk driven by a river, which has no rain start time, is sent straight away.

To avoid messaging people every 6 hours about the same storm, the service sends **one alert per place, per rain day, risk level and rain strength**. If the forecast is upgraded (say, from light to heavy), people are told again.

Each message is written in plain language with steps to take for that risk level. Every send attempt is logged with its channel, recipient, and whether it succeeded.

## Features

1. **Check any place.** Type a village, town or city and see when the next rain is expected, how strong it will be, and the flood risk. Works anywhere, not only the original communities.
2. **Self-service sign-up.** Choose SMS or email and confirm with a 6-digit code (expires in 10 minutes, 5 attempts, 3 codes per hour per contact), so nobody can sign up someone else.
3. **Forecast-based alerts.** Sent automatically, as described above.
4. **Live Stats tab.** Public totals only (subscribers, places covered, alerts delivered, delivery rate). Never any phone number or email.
5. **Admin tab.** Hidden unless the page is opened with `?admin=1`, and every action needs the admin password. Lists subscribers, shows every alert sent and its delivery status, and can run a check immediately.

Five communities are built in with coordinates: Alajo, Mepe, Anloga, Sokpoe and New Legon.

## Setup

Run locally:

```bash
pip install -r requirements.txt
python app.py
```

Deploy on Render as a Web Service with build command `pip install -r requirements.txt` and start command `python app.py`.

### Environment variables

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | **Required.** Postgres connection string (e.g. from Supabase) |
| `ADMIN_PASSWORD` | **Required for admin actions.** Gates every admin action |
| `BREVO_API_KEY`, `ALERT_FROM_EMAIL` | Send email over HTTPS (works on Render's free tier, which blocks SMTP) |
| `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_HOST`, `SMTP_PORT` | Optional SMTP fallback if Brevo is not set |
| `ARKESEL_API_KEY`, `ARKESEL_SENDER_ID` | Send SMS to Ghana numbers (sender name max 11 characters) |
| `WEATHER_USER_AGENT` | Contact string required by MET Norway, e.g. `FloodGuardAI/1.0 you@example.com` |
| `APP_URL` | Public site URL used in link previews |
| `MAX_PLACES` (100), `MAX_SUBSCRIBERS` (2000) | Sign-up limits |
| `RAIN_ALERT_MIN_INTENSITY` (heavy), `ALERT_LEAD_HOURS` (12) | When rain alerts are sent |
| `RAIN_ALERT_PEAK_MM_H` (8), `RAIN_ALERT_MM` (30), `RAIN_MODERATE_MM_H` (2.5), `RAIN_MODERATE_TOTAL_MM` (10), `RAIN_MIN_TOTAL_MM` (1), `RAIN_HOUR_MM` (0.5) | Rain-strength cutoffs |

If email or SMS credentials are missing, sign-up and checks still work, but messages cannot be sent and the failure is recorded in the delivery log.

Tables are created automatically on first start. If `DATABASE_URL` is missing the app raises a clear error instead of silently falling back to memory, which would lose data.

## Known limitations

- **Free-tier hosting sleeps.** Render's free tier spins down after about 15 minutes without visits, which pauses the 6-hour background check until the next visit. Use an always-on instance, or a free uptime pinger (such as UptimeRobot), for guaranteed checks.
- **Water level is modelled, not measured.** See above. It is not a real gauge reading.
- **Thresholds are fixed, not calibrated.** The 80 mm and 3.2 m values apply everywhere and have not been fitted or validated per community against recorded flood events.
- **No automated tests or accuracy evaluation are included yet.**
- **SMS is Ghana-only (+233).** Everyone else uses email.
- **Forecasts can be wrong.** An alert means flooding is possible, not certain.
- **One shared admin password.** Compared in constant time, but it is not per-user authentication.
- **Delivery depends on providers.** SMS costs credit, and failed sends are logged but not retried.

## What's next

- Validate and calibrate thresholds against documented flood events, per community.
- Add real gauge data (for example through the Volta River Authority or the Water Resources Commission) to replace the GloFAS proxy where possible.
- Add local languages to the alert messages.
- Add automated tests.
