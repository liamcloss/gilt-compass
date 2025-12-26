"""
Trading 212 Universe Diff (v0)

Purpose:
- Compare the two most recent ISA universe snapshots
- Detect new, removed, and changed instruments
- Write a diff artefact for review and automation

No pipeline integration yet.
Deterministic. Inspectable.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ISA_DIR = BASE_DIR / "data" / "universe" / "isa"
OUT_DIR = BASE_DIR / "data" / "universe" / "diffs"

OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def latest_two_snapshots() -> tuple[Path, Path]:
    files = sorted(ISA_DIR.glob("trading212_isa_*.parquet"))
    if len(files) < 2:
        raise RuntimeError("Need at least two ISA universe snapshots to diff")
    return files[-2], files[-1]


def load_snapshot(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


# ---------------------------------------------------------------------
# Diff logic
# ---------------------------------------------------------------------

def diff_universe(prev: pd.DataFrame, curr: pd.DataFrame) -> dict:
    key = "ticker"

    prev_idx = prev.set_index(key)
    curr_idx = curr.set_index(key)

    prev_tickers = set(prev_idx.index)
    curr_tickers = set(curr_idx.index)

    added = sorted(curr_tickers - prev_tickers)
    removed = sorted(prev_tickers - curr_tickers)
    common = sorted(prev_tickers & curr_tickers)

    # Detect changes in common instruments
    changed = []

    compare_cols = [
        "name",
        "shortName",
        "type",
        "currencyCode",
        "isin",
        "maxOpenQuantity",
        "extendedHours",
    ]

    for t in common:
        before = prev_idx.loc[t]
        after = curr_idx.loc[t]

        diffs = {}
        for col in compare_cols:
            if col in prev.columns and col in curr.columns:
                if before[col] != after[col]:
                    diffs[col] = {
                        "before": before[col],
                        "after": after[col],
                    }

        if diffs:
            changed.append({
                "ticker": t,
                "changes": diffs,
            })

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
    }


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    prev_path, curr_path = latest_two_snapshots()

    print(f"▶ Diffing snapshots:")
    print(f"  - Previous: {prev_path.name}")
    print(f"  - Current : {curr_path.name}")

    prev = load_snapshot(prev_path)
    curr = load_snapshot(curr_path)

    diff = diff_universe(prev, curr)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"trading212_isa_diff_{timestamp}.json"

    pd.Series(diff).to_json(out, indent=2)

    p
