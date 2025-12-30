"""
Thesis Agent (Queue-driven, v1)

Consumes thesis_queue.csv and emits a short, sceptical markdown thesis
per instrument_id.

Decision-support only. No recommendations.
Deterministic. Incremental. Idempotent.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime, UTC
import textwrap


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

QUEUE = BASE_DIR / "outputs" / "thesis_queue.csv"
SCORES = BASE_DIR / "outputs" / "scores_current.parquet"

OUT_DIR = BASE_DIR / "outputs" / "theses"
OUT_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------
# Thesis builder
# ---------------------------------------------------------------------

def build_thesis(row: pd.Series) -> str:
    """
    Produce a compact markdown thesis for a single instrument.
    Uses score + delta context.
    """

    instrument_id = row["instrument_id"]
    score = row["score"]
    delta = row["score_delta"]
    priority = row["priority"]
    reason = row["reason"]

    direction = "improving" if delta > 0 else "deteriorating"

    sceptic = (
        "Is this improvement durable, or a short-term factor exposure?"
        if delta > 0
        else
        "Is this deterioration signal or noise?"
    )

    md = f"""
    # {instrument_id}

    **Trigger:** {reason}  
    **Priority:** {priority}

    ## Score context
    - **Current score:** {score:.2f}
    - **Score change:** {delta:+.2f} ({direction})

    ## Interpretation
    {instrument_id} entered the thesis queue due to a **{reason}** event.
    The score has **{direction} materially**, pushing it beyond the
    configured threshold for review.

    This is a *screening signal*, not a view. The score reflects relative
    momentum and risk-adjusted characteristics, not fundamentals.

    ## Sceptical question
    > {sceptic}

    ## What would invalidate this signal?
    - Reversion of score over the next scoring cycle
    - Breakdown in liquidity or coverage
    - Broader factor rotation overwhelming idiosyncratic effects
    """

    return textwrap.dedent(md).strip()


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run(max_per_run: int = 5) -> None:
    if not QUEUE.exists():
        print("✓ No thesis queue found")
        return

    queue = pd.read_csv(QUEUE)

    # Initialise lifecycle columns if missing
    if "status" not in queue.columns:
        queue["status"] = "queued"
        queue["processed_at"] = pd.NA
        queue["error"] = pd.NA

    pending = queue[queue["status"] == "queued"]

    if pending.empty:
        print("✓ No queued theses to process")
        return

    scores = pd.read_parquet(SCORES)

    processed = 0

    for idx, job in pending.head(max_per_run).iterrows():
        instrument_id = job["instrument_id"]
        print(f"▶ Generating thesis for {instrument_id}")

        try:
            row = scores.loc[
                scores["instrument_id"] == instrument_id
            ].iloc[0]

            thesis = build_thesis({
                **job.to_dict(),
                **row.to_dict(),
            })

            out_file = OUT_DIR / f"{instrument_id}.md"
            out_file.write_text(thesis, encoding="utf-8")

            queue.loc[idx, "status"] = "done"
            queue.loc[idx, "processed_at"] = datetime.now(UTC).isoformat()

            processed += 1
            print(f"✓ Written → {out_file.name}")

        except Exception as e:
            queue.loc[idx, "status"] = "failed"
            queue.loc[idx, "error"] = str(e)
            queue.loc[idx, "processed_at"] = datetime.now(UTC).isoformat()
            print(f"✗ Failed for {instrument_id}: {e}")

    queue.to_csv(QUEUE, index=False)

    print(f"✓ Processed {processed} thesis jobs")


if __name__ == "__main__":
    run()
