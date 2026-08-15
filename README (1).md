# FloodGuard AI — Phase 1 Working Pipeline

A working pipeline for the hackathon build: real CSV data cleaning, threshold-based
flood-event detection, and field-scenario risk classification, all in one Gradio app.

## How it maps to the proposal

| Proposal section | Code |
|---|---|
| Data acquisition & integration (Sec. 6) | `process_csv()`, flexible column matching across source formats |
| Data cleaning (Sec. 6) | `process_csv()`, type coercion, outlier/duplicate/missing-value flags |
| Exploratory analysis / thresholds (Sec. 6, 10) | `FLOOD_WATER_THRESHOLD_M`, `FLOOD_RAINFALL_THRESHOLD_MM`, flood-event flagging |
| Success metric: <10% missing data (Sec. 10) | Missing-data-by-field report in the "Data Pipeline" tab |
| Advance warning / risk signal (Sec. 5) | `classify_risk()`, rule-based by default, Claude-enhanced if a key is set |
| Community risk view | "Community Risk Dashboard" tab, live table of readings + alerts |

## Run it locally

```bash
pip install -r requirements.txt
python app.py
```

This opens a local web UI (usually at `http://127.0.0.1:7860`).

## Deploy on Render (Web Service — this needs a running Python process, not a Static Site)

1. Push this folder to a GitHub repo.
2. On Render: **New → Web Service**, connect the repo.
3. Settings:
   - **Runtime:** Python 3
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `python app.py`
4. (Optional) Add an environment variable `ANTHROPIC_API_KEY` if you want
   Claude-enhanced classification context instead of the rule-based-only output.
5. Deploy — Render gives you a public URL, this is what you'd show judges.

Render sets a `PORT` environment variable automatically; `app.py` reads it, so no
extra config is needed there.

## Deploy to a Hugging Face Space (alternative)

1. Go to huggingface.co, create a new **Space**, choose the **Gradio** SDK.
2. Upload `app.py` and `requirements.txt`.
3. (Optional) Add `ANTHROPIC_API_KEY` as a repository secret.
4. The Space builds automatically and gives you a public URL.

## Using it without an API key

The app works fully without any API key — `classify_risk()` falls back to a
transparent, threshold-derived rule-based classifier (checks rainfall and water
level against Phase 1's flood thresholds, and scans field notes for critical terms
like "trapped" or "rising fast"). This is a legitimate design choice to mention in
your pitch: traceable logic instead of a black box, in a context where lives are
at stake.

If you do want Claude-enhanced output (a plain-language sentence a responder could
act on, layered on top of the rule-based level), set the environment variable
before running:

```bash
export ANTHROPIC_API_KEY=your_key_here
python app.py
```

## Testing without a real CSV

Click **"Load sample data instead"** in the Data Pipeline tab — it loads a small
illustrative dataset (deliberately includes a missing value, a duplicate row, and
an outlier) so you can see the cleaning logic actually catch each one live.

## Known limitations to mention in the demo

- The in-memory store (`READINGS`, `ALERTS`) resets every time the app restarts —
  fine for a demo, swap in Firebase/Supabase/Airtable for anything persistent.
- Column matching handles common header variants but isn't exhaustive; if your CSV
  isn't recognized, the report tells you which canonical fields it couldn't map.
- Thresholds (3.2m water level, 80mm/24h rainfall) are illustrative starting points
  from the proposal's exploratory-analysis goals, not yet validated per community —
  that validation is Phase 1's actual Section 6/10 deliverable.
