# FloodGuard AI

An automatic flood warning system for flood prone communities in Ghana. It checks the weather forecast ahead of time and warns people 12 to 24 hours before flooding happens, instead of only confirming it after it has started.

## Why forecast-based, not reactive

Most flood reporting tools (including an earlier version of this one) work backward: someone reports water is rising, or a sensor detects it, and the system reacts. That gives people little or no time to prepare. This system instead pulls a rainfall **forecast** for the next 24 hours, so a warning can go out before the rain has even started, which is the whole point of an early-warning system.

## Architecture

```
Open-Meteo Forecast API  ──┐
Open-Meteo Flood API    ──┼──▶  run_scheduled_check()  ──▶  Postgres (Supabase)
Open-Meteo Geocoding API──┘            │                          │
                                        ▼                          ▼
                                  risk classification      Dashboard / Admin UI
                                        │
                                        ▼
                                Brevo (email) / Twilio (SMS)
```

- **Gradio** — the web UI and app server, deployed on Render's free tier.
- **Postgres (Supabase)** — persists readings, alerts, contacts, and water levels across restarts. Render's own filesystem is wiped on every redeploy, so nothing can be stored locally.
- **Open-Meteo Forecast API** — free, no API key, gives rainfall forecast for any coordinates.
- **Open-Meteo Flood API (GloFAS)** — free, no API key, gives modelled river discharge for rivers worldwide. Used as the automatic water-level signal.
- **Open-Meteo Geocoding API** — free, no API key, turns a typed place name into coordinates for the "Check My Area" feature.
- **Brevo** — sends real outbound email over HTTPS. Chosen specifically because Render's free tier blocks outbound SMTP (ports 25/465/587) as of September 2025, which silently breaks Gmail SMTP sending.
- **Twilio** — optional SMS sending, not required for the system to work.

## Why river discharge instead of a real water-level sensor

There is no public network of river gauges in Ghana reporting a live depth in metres. What does exist, freely, is GloFAS — a global hydrological model that estimates river discharge (in cubic metres per second) for modelled rivers worldwide, including the Volta system.

This system compares today's discharge to that river's own recent 60-day median, and scales the ratio onto the same metres scale already used for the flood threshold:

- normal flow (ratio 1.0) → scaled to half the threshold
- double normal flow (ratio 2.0) → scaled to exactly the threshold

This is an approximation, not a real depth reading, and the code says so directly in its own comments. For communities with no modelled river nearby (e.g. New Legon, which floods from drainage, not a river), the system honestly falls back to 0 rather than inventing a number.

## Risk classification

Risk is a simple, transparent rule-based check, not a black-box model — deliberately, since a safety-critical alert needs to be explainable to the people receiving it:

- `FLOOD_RAINFALL_THRESHOLD_MM` and `FLOOD_WATER_THRESHOLD_M` define the baseline HIGH/CRITICAL cutoffs.
- Either signal alone, if extreme enough (roughly 1.25x the threshold), can push a community straight to CRITICAL, even if the other signal is low — this matters because the two known real-world flood mechanisms in these communities (extreme rainfall vs. a dam-driven discharge spike) don't always show up in both signals at once.

## Known limitations, stated honestly

- **Render's free tier spins down after ~15 minutes idle**, pausing the background check until the next visit. This is disclosed in the app itself rather than hidden.
- **A single shared admin password**, compared with `secrets.compare_digest` to avoid timing attacks, but still not per-user authentication. Fine for a small team, not enterprise-grade.
- **GloFAS discharge is a proxy, not a real sensor.** It is disclosed as such everywhere it's used, including in the alert email itself.
- **Thresholds are not yet calibrated per community.** They were set from general Volta-region reporting, not a rigorous statistical fit. The backtest tool (see below) is the first real step toward validating them against actual outcomes rather than assuming they are correct.

## Validating accuracy: the backtest tool

Rather than claim an accuracy number with no evidence behind it, the Admin tab includes a real backtest against two confirmed, recent flood events near New Legon, run through the exact same classification logic the live app uses:

1. **18 May 2025, Accra** — NADMO-confirmed: 5 deaths, over 3,000 people displaced, after around four hours of heavy rain. Affected Adenta, Kaneshie, Okponglo and East Legon Hills, right around New Legon.
2. **29 June 2026, Accra** — the most recent major flooding in Ghana at the time this was written, killing at least 10–12 people and overwhelming drainage across Adenta, Madina, Achimota and East Legon.

Both events are rainfall-driven with no river involved, so together they specifically test whether rainfall alone correctly triggers a warning for a drainage-only location, across two separate real storms a year apart. For each event, it pulls actual historical rainfall for the real dates, and reports how many days during that known flood the system would have correctly flagged HIGH/CRITICAL risk.

This only works once deployed (it calls live external APIs), and is designed to be extended with more known events over time as a genuine, growing evidence base for the system's accuracy.

## Features

1. **Location-based risk check** ("Check My Area" tab) — anyone can check flood risk for any place name, not just the five pre-registered communities, using live geocoding plus the same forecast and discharge logic.
2. **Forecast-based prediction** — uses the rainfall forecast, not past data, so warnings come before the rain, not after.
3. **Automatic alerts** — real email (and SMS, if Twilio is configured) sent 12–24 hours before heavy rain, in plain language with concrete steps.
4. **Risk levels** — LOW, MODERATE, HIGH, CRITICAL, from forecast rainfall combined with automatic (or manual fallback) water level.
5. **Community contact lists** — bulk CSV import per community, so residents never need to sign up themselves.

## Setup

Required environment variables (Render → Environment tab):

| Variable | Purpose |
|---|---|
| `ADMIN_PASSWORD` | Gates every admin action |
| `DATABASE_URL` | Postgres connection string (Supabase) |
| `BREVO_API_KEY` | Sends real alert emails over HTTPS |
| `ALERT_FROM_EMAIL` | Sender address, verified in Brevo |
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` / `TWILIO_FROM_NUMBER` | Optional, for SMS |

## What's next

- Calibrate thresholds per community using more real historical events, once the backtest tool has been run against several.
- Redesign the interface further beyond the current Gradio theme (color-coded risk badges, distinct typography) toward a fully custom layout.
- Explore real partnerships with VRA or the Water Resources Commission for genuine gauge data, to replace the GloFAS proxy where possible.
