"""
Patrician Agent (v1.1 — fixed)

Role:
- Govern attention allocation
- Prioritise queued instruments
- Enforce hard cognitive limits

Inputs:
- outputs/thesis_queue.csv

Outputs:
- Mutates thesis_queue.csv with priority_rank and attention_bucket

No scoring. No judgement. No filtering.
Deterministic. Idempotent.
"""

from pathlib import Path
import pandas as pd


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent
QUEUE = BASE_DIR / "outputs" / "thesis_queue.csv"


# ---------------------------------------------------------------------
# Config — attention governance
# ---------------------------------------------------------------------

MAX_ATTENTION_PER_RUN = 5  # hard cognitive cap

PRIORITY_ORDER = {
    "high": 0,
    "medium": 1,
    "low": 2,
}


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not QUEUE.exists():
        print("✓ No thesis queue found — nothing to govern")
        return

    queue = pd.read_csv(QUEUE)

    if "status" not in queue.columns:
        print("✓ Queue has no lifecycle state — skipping patrician")
        return

    # Work only on queued items
    mask = queue["status"] == "queued"
    pending = queue.loc[mask].copy()

    if pending.empty:
        print("✓ No queued instruments to prioritise")
        return

    # -----------------------------------------------------------------
    # Normalise priority
    # -----------------------------------------------------------------

    if "priority" not in pending.columns:
        pending["priority"] = "medium"

    pending["priority_rank"] = (
        pending["priority"]
        .map(PRIORITY_ORDER)
        .fillna(PRIORITY_ORDER["medium"])
        .astype(int)
    )

    # -----------------------------------------------------------------
    # Sort by priority then absolute delta
    # -----------------------------------------------------------------

    if "abs_delta" not in pending.columns:
        raise RuntimeError("Patrician requires abs_delta in thesis_queue")

    pending = pending.sort_values(
        by=["priority_rank", "abs_delta"],
        ascending=[True, False],
    )

    # -----------------------------------------------------------------
    # Assign attention buckets
    # -----------------------------------------------------------------

    pending["attention_bucket"] = "deferred"
    pending.iloc[:MAX_ATTENTION_PER_RUN, pending.columns.get_loc("attention_bucket")] = "active"

    # -----------------------------------------------------------------
    # Write results back (Patrician OWNS these fields)
    # -----------------------------------------------------------------

    queue.loc[mask, "priority_rank"] = pending["priority_rank"].values
    queue.loc[mask, "attention_bucket"] = pending["attention_bucket"].values

    queue.to_csv(QUEUE, index=False)

    print("✓ Patrician prioritised thesis queue")
    print(queue["attention_bucket"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    run()
