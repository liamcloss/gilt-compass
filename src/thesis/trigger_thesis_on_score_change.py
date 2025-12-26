"""
Trigger thesis generation based on score changes.

Rules:
- Enqueue instruments that are new OR whose score changed materially
- Do not regenerate theses unnecessarily
"""

from pathlib import Path
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

SCORES_CURR = BASE_DIR / "outputs" / "scores_current.parquet"
SCORES_PREV = BASE_DIR / "outputs" / "scores_previous.parquet"
QUEUE_OUT = BASE_DIR / "outputs" / "thesis_queue.csv"


# ---------------------------------------------------------------------
# Config (explicit + adjustable)
# ---------------------------------------------------------------------

SCORE_DELTA_THRESHOLD = 0.75  # material change


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not SCORES_CURR.exists():
        print("✓ No scores found — nothing to trigger")
        return

    curr = pd.read_parquet(SCORES_CURR)

    if SCORES_PREV.exists():
        prev = pd.read_parquet(SCORES_PREV)
    else:
        prev = pd.DataFrame(columns=["instrument_id", "score"])

    merged = curr.merge(
        prev[["instrument_id", "score"]],
        on="instrument_id",
        how="left",
        suffixes=("", "_prev"),
    )

    # -----------------------------------------------------------------
    # Identify triggers
    # -----------------------------------------------------------------

    merged["score_prev"] = merged["score_prev"].fillna(0)
    merged["score_delta"] = (merged["score"] - merged["score_prev"]).abs()

    triggers = merged[
        (merged["score_prev"] == 0) |
        (merged["score_delta"] >= SCORE_DELTA_THRESHOLD)
    ]

    if triggers.empty:
        print("✓ No thesis triggers detected")
        curr.to_parquet(SCORES_PREV, index=False)
        return

    queue = triggers[[
        "instrument_id",
        "score",
        "score_prev",
        "score_delta",
    ]].copy()

    queue["enqueued_at"] = datetime.utcnow().isoformat()
    queue["reason"] = queue.apply(
        lambda r: "new" if r["score_prev"] == 0 else "score_change",
        axis=1,
    )

    # -----------------------------------------------------------------
    # Persist queue
    # -----------------------------------------------------------------

    if QUEUE_OUT.exists():
        queue = pd.concat(
            [pd.read_csv(QUEUE_OUT), queue],
            ignore_index=True,
        )

    queue.to_csv(QUEUE_OUT, index=False)

    # Snapshot current scores for next run
    curr.to_parquet(SCORES_PREV, index=False)

    print(f"✓ Enqueued {len(queue):,} instruments for thesis generation")
    print(f"✓ Queue written → {QUEUE_OUT}")


if __name__ == "__main__":
    run()
