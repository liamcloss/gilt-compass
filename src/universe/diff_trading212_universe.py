"""
Trading 212 Universe Diff

Purpose:
- Compare the two most recent Trading 212 universe snapshots
- Detect added, removed, and changed instruments
- Emit structured diff artefacts for downstream automation

Deterministic. Inspectable. Idempotent.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime, timezone


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

RAW_DIR = BASE_DIR / "data" / "universe" / "raw"
OUT_DIR = BASE_DIR / "data" / "universe" / "diffs"

OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def latest_two_snapshots() -> tuple[Path | None, Path]:
    files = sorted(RAW_DIR.glob("trading212_raw_*.parquet"))

    if not files:
        raise RuntimeError("No Trading 212 snapshots found")

    if len(files) == 1:
        return None, files[0]

    return files[-2], files[-1]


def load_snapshot(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def derive_instrument_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive a stable instrument_id from Trading 212 ticker.

    Assumes ticker format like:
    ZOM_US_EQ
    """
    if "instrument_id" in df.columns:
        return df

    if "ticker" not in df.columns:
        raise RuntimeError("Snapshot missing 'ticker' column")

    df = df.copy()
    df["instrument_id"] = df["ticker"]
    return df


# ---------------------------------------------------------------------
# Diff logic
# ---------------------------------------------------------------------

def diff_universe(prev: pd.DataFrame, curr: pd.DataFrame) -> dict:
    key = "instrument_id"

    prev_idx = prev.set_index(key)
    curr_idx = curr.set_index(key)

    prev_ids = set(prev_idx.index)
    curr_ids = set(curr_idx.index)

    added = sorted(curr_ids - prev_ids)
    removed = sorted(prev_ids - curr_ids)
    common = sorted(prev_ids & curr_ids)

    changed = []

    compare_cols = [
        "ticker",
        "name",
        "shortName",
        "type",
        "currencyCode",
        "isin",
        "maxOpenQuantity",
        "extendedHours",
    ]

    for iid in common:
        before = prev_idx.loc[iid]
        after = curr_idx.loc[iid]

        diffs = {}

        for col in compare_cols:
            if col not in prev.columns or col not in curr.columns:
                continue

            b = before[col]
            a = after[col]

            if pd.isna(b) and pd.isna(a):
                continue

            if b != a:
                diffs[col] = {
                    "before": b,
                    "after": a,
                }

        if diffs:
            changed.append({
                "instrument_id": iid,
                "symbol": after.get("ticker"),
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

    print("▶ Diffing Trading 212 snapshots")
    print(f"  - Previous: {prev_path.name}")
    print(f"  - Current : {curr_path.name}")

    prev = derive_instrument_id(load_snapshot(prev_path))
    curr = derive_instrument_id(load_snapshot(curr_path))

    diff = diff_universe(prev, curr)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # -----------------------------------------------------------------
    # Write automation-friendly artefacts
    # -----------------------------------------------------------------

    if diff["added"]:
        pd.DataFrame({"instrument_id": diff["added"]}) \
            .to_csv(OUT_DIR / f"trading212_new_{ts}.csv", index=False)

    if diff["removed"]:
        pd.DataFrame({"instrument_id": diff["removed"]}) \
            .to_csv(OUT_DIR / f"trading212_removed_{ts}.csv", index=False)

    if diff["changed"]:
        pd.json_normalize(diff["changed"]) \
            .to_csv(OUT_DIR / f"trading212_changed_{ts}.csv", index=False)

    print(f"✓ Added   : {len(diff['added'])}")
    print(f"✓ Removed : {len(diff['removed'])}")
    print(f"✓ Changed : {len(diff['changed'])}")


if __name__ == "__main__":
    run()
