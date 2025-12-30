"""
Trigger thesis generation based on score changes.

Rules:
- Only consider instruments in the active universe
- Only trigger on material, directional score changes
- Never trigger on stale price data
- Apply cooldown, direction filters, and priority banding
"""

from pathlib import Path
import pandas as pd
from datetime import datetime, UTC, timedelta
import numpy as np


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

SCORES_CURR = BASE_DIR / "outputs" / "scores_current.parquet"
SCORES_PREV = BASE_DIR / "outputs" / "scores_previous.parquet"
QUEUE_OUT = BASE_DIR / "outputs" / "thesis_queue.csv"

PRICES = BASE_DIR / "data" / "prices" / "prices_1y.parquet"
ACTIVE_UNIVERSE = BASE_DIR / "data" / "watchlists" / "active_universe.csv"


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

SCORE_DELTA_THRESHOLD = 1.25      # raised for signal
TRIGGER_DIRECTION = "up"          # "up", "down", "both"
MIN_ABS_SCORE = 0.25

COOLDOWN_DAYS = 7
MAX_PRICE_AGE_DAYS = 2            # freshness gate (critical)

PRIORITY_BANDS = {
    "high": 1.50,
    "medium": 1.00,
    "low": SCORE_DELTA_THRESHOLD,
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def priority_from_delta(delta: float) -> str:
    if delta >= PRIORITY_BANDS["high"]:
        return "high"
    if delta >= PRIORITY_BANDS["medium"]:
        return "medium"
    return "low"


def direction_ok(delta: float) -> bool:
    if TRIGGER_DIRECTION == "both":
        return True
    if TRIGGER_DIRECTION == "up":
        return delta > 0
    if TRIGGER_DIRECTION == "down":
        return delta < 0
    return False


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not SCORES_CURR.exists():
        print("✓ No scores found — nothing to trigger")
        return

    if not ACTIVE_UNIVERSE.exists():
        print("✗ Missing active_universe.csv — nothing will be enqueued")
        return

    curr = pd.read_parquet(SCORES_CURR)

    if SCORES_PREV.exists():
        prev = pd.read_parquet(SCORES_PREV)
    else:
        prev = pd.DataFrame(columns=["instrument_id", "score"])

    # -----------------------------------------------------------------
    # Merge scores (THIS IS WHERE YOU ASKED)
    # -----------------------------------------------------------------

    merged = curr.merge(
        prev[["instrument_id", "score"]],
        on="instrument_id",
        how="left",
        suffixes=("", "_prev"),
    )

    # -----------------------------------------------------------------
    # Numeric safety
    # -----------------------------------------------------------------

    merged["score"] = pd.to_numeric(merged["score"], errors="coerce")
    merged["score_prev"] = (
        pd.to_numeric(merged["score_prev"], errors="coerce")
        .fillna(0.0)
    )

    merged["score_delta"] = merged["score"] - merged["score_prev"]
    merged["abs_delta"] = merged["score_delta"].abs()

    # -----------------------------------------------------------------
    # ACTIVE UNIVERSE GATE
    # -----------------------------------------------------------------

    active_ids = set(
        pd.read_csv(ACTIVE_UNIVERSE)["instrument_id"].astype(str)
    )

    merged["instrument_id"] = merged["instrument_id"].astype(str)
    is_active = merged["instrument_id"].isin(active_ids)

    # -----------------------------------------------------------------
    # FRESHNESS GATE (BASELINE HARDENING)
    # -----------------------------------------------------------------

    prices = pd.read_parquet(PRICES, columns=["instrument_id", "date"])
    prices["date"] = pd.to_datetime(prices["date"], utc=True)

    last_price = (
        prices
        .groupby("instrument_id")["date"]
        .max()
        .reset_index()
        .rename(columns={"date": "last_price_date"})
    )

    merged = merged.merge(
        last_price,
        on="instrument_id",
        how="left",
    )

    fresh_cutoff = datetime.now(UTC) - timedelta(days=MAX_PRICE_AGE_DAYS)
    is_fresh = merged["last_price_date"] >= fresh_cutoff

    # -----------------------------------------------------------------
    # Trigger logic (signal, not noise)
    # -----------------------------------------------------------------

    is_material = merged["abs_delta"] >= SCORE_DELTA_THRESHOLD
    passes_direction = merged["score_delta"].apply(direction_ok)
    passes_floor = merged["score"].abs() >= MIN_ABS_SCORE

    triggers = merged[
        is_active &
        is_fresh &
        is_material &
        passes_direction &
        passes_floor
    ].copy()

    if triggers.empty:
        print("✓ No thesis triggers detected")
        curr.to_parquet(SCORES_PREV, index=False)
        return

    # -----------------------------------------------------------------
    # Build queue
    # -----------------------------------------------------------------

    now = datetime.now(UTC)

    queue = triggers[
        ["instrument_id", "score", "score_prev", "score_delta", "abs_delta"]
    ].copy()

    queue["enqueued_at"] = now.isoformat()
    queue["reason"] = "score_change"
    queue["priority"] = queue["abs_delta"].apply(priority_from_delta)

    # -----------------------------------------------------------------
    # De-duplication + cooldown
    # -----------------------------------------------------------------

    if QUEUE_OUT.exists():
        existing = pd.read_csv(QUEUE_OUT)
        existing["enqueued_at"] = pd.to_datetime(existing["enqueued_at"], utc=True)

        cutoff = now - timedelta(days=COOLDOWN_DAYS)
        recently_enqueued = set(
            existing.loc[
                existing["enqueued_at"] >= cutoff,
                "instrument_id",
            ]
        )

        queue = queue[
            ~queue["instrument_id"].isin(recently_enqueued)
        ]

        if not queue.empty:
            queue = pd.concat([existing, queue], ignore_index=True)
        else:
            queue = existing

    if queue.empty:
        print("✓ All triggers suppressed by cooldown / dedupe")
        curr.to_parquet(SCORES_PREV, index=False)
        return

    # -----------------------------------------------------------------
    # Persist
    # -----------------------------------------------------------------

    queue.to_csv(QUEUE_OUT, index=False)
    curr.to_parquet(SCORES_PREV, index=False)

    # -----------------------------------------------------------------
    # Output
    # -----------------------------------------------------------------

    print(f"✓ Enqueued {len(queue):,} instruments")
    print("By priority:")
    print(queue["priority"].value_counts().to_string())
    print(f"✓ Queue written → {QUEUE_OUT}")


if __name__ == "__main__":
    run()
