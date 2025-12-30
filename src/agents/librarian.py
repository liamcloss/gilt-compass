"""
Librarian Agent (v1 — realigned)

Role:
- Build a durable, factual reference entry per instrument
- Run ONLY when an instrument first enters attention
- No judgement, no scoring, no prioritisation

Inputs:
- outputs/thesis_queue.csv
- data/prices/prices_1y.parquet
- outputs/scores_current.parquet

Outputs:
- outputs/library/{instrument_id}.md

Deterministic. Event-driven. Memory-focused.
"""

from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import textwrap


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

QUEUE = BASE_DIR / "outputs" / "thesis_queue.csv"
PRICES = BASE_DIR / "data" / "prices" / "prices_1y.parquet"
SCORES = BASE_DIR / "outputs" / "scores_current.parquet"

OUT_DIR = BASE_DIR / "outputs" / "library"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Build factual entry (NO OPINION)
# ---------------------------------------------------------------------

def build_entry(
    instrument_id: str,
    prices: pd.DataFrame,
    score_row: pd.Series,
) -> str:
    px = (
        prices[prices["instrument_id"] == instrument_id]
        .sort_values("date")
        .copy()
    )

    if px.empty:
        raise RuntimeError("No price history available")

    start_date = px["date"].iloc[0]
    end_date = px["date"].iloc[-1]

    hi = px["close"].max()
    lo = px["close"].min()
    last = px["close"].iloc[-1]

    md = f"""
    # Library entry: {instrument_id}

    ## Identification
    - **Instrument ID:** {instrument_id}
    - **Data window:** {start_date.date()} → {end_date.date()}
    - **Observations:** {len(px):,}

    ## Price facts (1Y window)
    - **Last close:** {last:.2f}
    - **52w high:** {hi:.2f}
    - **52w low:** {lo:.2f}
    - **Range:** {(hi / lo - 1):.1%}

    ## Derived metrics (from scorer)
    - **1Y return:** {score_row["ret_1y"]:.1%}
    - **20d volatility:** {score_row["vol_20d"]:.2%}
    - **Avg £ turnover (20d):** £{score_row["avg_turnover_20d"]:,.0f}
    - **Score (snapshot):** {score_row["score"]:.2f}

    ## Librarian note
    This entry records **descriptive, historical facts only** as observed
    at the time the instrument first entered attention.

    It is not updated automatically and contains **no judgement,
    forecasts, or recommendations**.
    """

    return textwrap.dedent(md).strip()


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run(max_per_run: int = 10) -> None:
    if not QUEUE.exists():
        print("✓ No thesis queue found — nothing to catalogue")
        return

    queue = pd.read_csv(QUEUE)

    if "status" not in queue.columns:
        print("✓ Queue has no lifecycle state — skipping librarian")
        return

    pending = queue[queue["status"] == "queued"]

    if pending.empty:
        print("✓ No queued instruments to catalogue")
        return

    prices = pd.read_parquet(PRICES)
    prices["date"] = pd.to_datetime(prices["date"], utc=True)

    scores = pd.read_parquet(SCORES)

    created = 0

    for _, job in pending.head(max_per_run).iterrows():
        instrument_id = str(job["instrument_id"])
        out_file = OUT_DIR / f"{instrument_id}.md"

        if out_file.exists():
            continue  # Librarian rule A: build once, never overwrite

        print(f"▶ Cataloguing {instrument_id}")

        score_row = scores.loc[
            scores["instrument_id"] == instrument_id
        ]

        if score_row.empty:
            print(f"⚠ No score data for {instrument_id}, skipping")
            continue

        try:
            entry = build_entry(
                instrument_id=instrument_id,
                prices=prices,
                score_row=score_row.iloc[0],
            )

            out_file.write_text(entry, encoding="utf-8")
            created += 1

        except Exception as e:
            print(f"✗ Failed to catalogue {instrument_id}: {e}")

    print(f"✓ Librarian created {created} new entries → {OUT_DIR}")


if __name__ == "__main__":
    run()
