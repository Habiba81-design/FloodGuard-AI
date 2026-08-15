"""
FloodGuard AI prototype pipeline
Raw source upload -> real cleaning & missing-data analysis -> threshold/flood-event
detection -> risk classification -> live community risk dashboard, all in one
Gradio app.

Run locally:
    pip install -r requirements.txt
    python app.py

Or deploy directly to a Hugging Face Space:
    1. Create a new Space (SDK: Gradio)
    2. Upload app.py and requirements.txt
    3. Add ANTHROPIC_API_KEY as a secret in the Space settings (optional, see note below)

Or deploy on Render as a Web Service:
    Build command: pip install -r requirements.txt
    Start command: python app.py
"""

import os
import io
import json
from datetime import datetime

import gradio as gr
import pandas as pd

# ---------------------------------------------------------------------------
# In-memory "database" for the live dashboard. Swap this for
# Firebase/Supabase/Airtable later, the rest of the pipeline does not need
# to change.
# ---------------------------------------------------------------------------
READINGS = []   # every cleaned row ever ingested, across all CSV uploads
ALERTS = []     # every scenario classified in the "Risk Classification" tab, newest first

FLOOD_WATER_THRESHOLD_M = 3.2      # matches Section 6 exploratory-analysis output
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


# ---------------------------------------------------------------------------
# Step 1: Data acquisition + integration (CSV upload, flexible column matching)
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
            "Upload a CSV to run the pipeline, or click \"Load sample data\" below.",
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

    # --- Real cleaning ---
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

    # Duplicate detection (same community + timestamp)
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

    # --- Missing-data report (the Section 10 success metric) ---
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

    status_lines = [
        f"**Rows processed:** {n}",
        f"**Average missing data across key fields:** {avg_missing}% "
        f"({'under' if avg_missing < 10 else 'over'} the 10% Phase 1 target)",
        f"**Outliers flagged:** {n_outliers}",
        f"**Duplicate readings flagged:** {n_dupes}",
        f"**Flood-threshold crossings detected:** {n_flood_events} "
        f"(water level ≥ {FLOOD_WATER_THRESHOLD_M}m or rainfall ≥ {FLOOD_RAINFALL_THRESHOLD_MM}mm/24h)",
    ]
    if missing_required:
        status_lines.append(
            f"⚠️ Couldn't confidently find a column for: {', '.join(missing_required)}. "
            f"Rename your CSV headers to include one of: "
            + "; ".join(f"{k} ({'/'.join(v)})" for k, v in COLUMN_ALIASES.items() if k in missing_required)
        )
    report_md = "\n\n".join(status_lines)

    # Persist cleaned rows into the shared "database" for the dashboard tab
    for _, row in df.iterrows():
        READINGS.append(row.to_dict())

    display_df = df.copy()
    display_df["timestamp"] = display_df["timestamp"].astype(str)

    return report_md, display_df, missing_df


def load_sample_data():
    """Illustrative multi-source CSV a hackathon judge can click to try instantly."""
    sample = io.StringIO(
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
        f.write(sample.getvalue())

    return process_csv(path)


def _dashboard_table():
    if not READINGS:
        return pd.DataFrame(columns=["timestamp", "community", "rainfall_mm", "water_level_m", "data_quality_flag", "flood_event_flag"])
    df = pd.DataFrame(READINGS)
    df = df.sort_values(by="flood_event_flag", ascending=False)
    df["timestamp"] = df["timestamp"].astype(str)
    return df[["timestamp", "community", "rainfall_mm", "water_level_m", "data_quality_flag", "flood_event_flag"]]


# ---------------------------------------------------------------------------
# Step 2: Risk classification (rule-based, Claude-enhanced if a key is set)
# ---------------------------------------------------------------------------
def classify_risk(community, rainfall_mm, water_level_m, notes):
    if not community:
        return "Enter a community name to classify."

    rainfall_mm = float(rainfall_mm or 0)
    water_level_m = float(water_level_m or 0)
    notes_lower = (notes or "").lower()

    hit_terms = [t for t in CRITICAL_TERMS if t in notes_lower]

    # --- Transparent rule-based classifier ---
    if hit_terms or (water_level_m >= FLOOD_WATER_THRESHOLD_M and rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM):
        level = "CRITICAL"
    elif water_level_m >= FLOOD_WATER_THRESHOLD_M * 0.8 or rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM * 0.65:
        level = "HIGH"
    elif water_level_m >= FLOOD_WATER_THRESHOLD_M * 0.6 or rainfall_mm >= FLOOD_RAINFALL_THRESHOLD_MM * 0.3:
        level = "MODERATE"
    else:
        level = "LOW"

    reasoning = (
        f"Rule-based: water level {water_level_m}m vs {FLOOD_WATER_THRESHOLD_M}m threshold, "
        f"rainfall {rainfall_mm}mm/24h vs {FLOOD_RAINFALL_THRESHOLD_MM}mm threshold"
        + (f", critical terms detected: {', '.join(hit_terms)}" if hit_terms else "")
        + "."
    )

    # --- Optional Claude-enhanced reasoning, same opt-in pattern as SafeBirth ---
    claude_note = None
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            prompt = (
                f"You are assisting a flood early-warning dashboard for Ghana. "
                f"Given community='{community}', rainfall_24h_mm={rainfall_mm}, "
                f"water_level_m={water_level_m}, field notes='{notes}', and a rule-based "
                f"classification of {level}, write one sentence a NADMO responder could act on. "
                f"Do not contradict the rule-based level; add context only."
            )
            resp = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            claude_note = "".join(
                block.text for block in resp.content if getattr(block, "type", "") == "text"
            ).strip()
        except Exception as e:
            claude_note = f"(Claude enhancement unavailable: {e})"

    alert = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "community": community,
        "rainfall_mm": rainfall_mm,
        "water_level_m": water_level_m,
        "risk_level": level,
        "notes": notes or "",
    }
    ALERTS.insert(0, alert)

    out = f"### Risk level: {level}\n\n{reasoning}"
    if claude_note:
        out += f"\n\n**Claude context:** {claude_note}"
    else:
        out += "\n\n*(Set ANTHROPIC_API_KEY to add Claude-generated response guidance to this classification.)*"

    return out


def _alerts_table():
    if not ALERTS:
        return pd.DataFrame(columns=["time", "community", "rainfall_mm", "water_level_m", "risk_level", "notes"])
    return pd.DataFrame(ALERTS)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
with gr.Blocks(title="FloodGuard AI — Phase 1 Pipeline") as demo:
    gr.Markdown(
        "# 🌊 FloodGuard AI — Phase 1 Working Pipeline\n"
        "Early warning, not late reaction. Upload real (or sample) rainfall/water-level "
        "data to run the actual cleaning and threshold-detection logic, then classify "
        "field scenarios the same way a responder would."
    )

    with gr.Tab("Data Pipeline"):
        gr.Markdown(
            "Upload a CSV with columns for date, community, rainfall, and water level "
            "(headers can vary — the pipeline matches common aliases). This runs real "
            "type-coercion, outlier/duplicate detection, and flood-threshold flagging, "
            "not a canned demo."
        )
        with gr.Row():
            csv_input = gr.File(label="Rainfall / water-level CSV", file_types=[".csv"], type="filepath")
            sample_btn = gr.Button("Load sample data instead")
        report = gr.Markdown()
        with gr.Row():
            cleaned_out = gr.Dataframe(label="Cleaned, schema-aligned rows", wrap=True)
        missing_out = gr.Dataframe(label="Missing data by field (%)", visible=True)

        csv_input.change(process_csv, inputs=csv_input, outputs=[report, cleaned_out, missing_out])
        sample_btn.click(load_sample_data, outputs=[report, cleaned_out, missing_out])

    with gr.Tab("Risk Classification"):
        gr.Markdown(
            "Type a field scenario the way a community reporter or sensor feed would send it. "
            "Classification is rule-based and transparent by default — traceable logic instead "
            "of a black box, in a context where lives are at stake — with optional Claude-generated "
            "response context if `ANTHROPIC_API_KEY` is set."
        )
        with gr.Row():
            comm_in = gr.Textbox(label="Community", placeholder="e.g. Mepe")
            rain_in = gr.Number(label="Rainfall, last 24h (mm)", value=0)
            level_in = gr.Number(label="Water level (m)", value=0)
        notes_in = gr.Textbox(label="Field notes (optional)", placeholder="e.g. river rising fast near the market")
        classify_btn = gr.Button("Classify", variant="primary")
        risk_out = gr.Markdown()

        classify_btn.click(classify_risk, inputs=[comm_in, rain_in, level_in, notes_in], outputs=risk_out)

    with gr.Tab("Community Risk Dashboard"):
        gr.Markdown("Live view of everything ingested this session — cleaned readings and classified alerts, highest risk first.")
        refresh_btn = gr.Button("Refresh dashboard")
        readings_dashboard = gr.Dataframe(label="Cleaned readings", value=_dashboard_table)
        alerts_dashboard = gr.Dataframe(label="Classified alerts", value=_alerts_table)
        refresh_btn.click(lambda: (_dashboard_table(), _alerts_table()), outputs=[readings_dashboard, alerts_dashboard])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    demo.launch(server_name="0.0.0.0", server_port=port)
