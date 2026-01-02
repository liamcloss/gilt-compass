"""
Mark inactive Trading 212 instruments in Gilt universe.

Rules:
- If an instrument exists in gilt_universe
  but NOT in the latest Trading 212 snapshot,
  mark it inactive.
- Never delete rows.
- Idempotent and safe to re-run.
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

UNIVERSE_PATH = BASE_DIR / "data" / "universe" / "gilt_universe.csv"
RAW_DIR = BASE_DIR / "data" / "universe" / "raw"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def latest_snapshot() -> Path:
    files = sorted(RAW_DIR.glob("trading212_raw_*.parquet"))
    if not files:
        raise RuntimeError(f"No Trading 212 snapshots found in {RAW_DIR}")
    return files[-1]


def derive_instrument_id(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Derive a canonical instrument_id.

    Priority:
    1. instrument_id (if already present)
    2. ticker
    3. symbol
    """
    df = df.copy()

    if "instrument_id" in df.columns:
        return df

    for col in ("ticker", "symbol"):
        if col in df.columns:
            df["instrument_id"] = df[col]
            return df

    raise RuntimeError(
        f"{source} missing identifier column "
        "(expected one of: instrument_id, ticker, symbol)"
    )


def validate_universe(df: pd.DataFrame) -> pd.DataFrame:
    df = derive_instrument_id(df, source="Universe")

    if "active" not in df.columns:
        df["active"] = True

    if "inactive_on" not in df.columns:
        df["inactive_on"] = pd.NA

    df["active"] = df["active"].astype(bool)

    return df


def validate_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    return derive_instrument_id(df, source="Snapshot")


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not UNIVERSE_PATH.exists():
        raise RuntimeError(f"Universe file not found: {UNIVERSE_PATH}")

    snapshot_path = latest_snapshot()

    print("▶ Marking inactive Trading 212 instruments")
    print(f"  - Universe : {UNIVERSE_PATH.name}")
    print(f"  - Snapshot : {snapshot_path.name}")

    universe = validate_universe(pd.read_csv(UNIVERSE_PATH))
    snapshot = validate_snapshot(pd.read_parquet(snapshot_path))

    # -----------------------------------------------------------------
    # Identify removals
    # -----------------------------------------------------------------

    universe_ids = set(universe["instrument_id"])
    snapshot_ids = set(snapshot["instrument_id"])

    to_deactivate = universe_ids - snapshot_ids

    if not to_deactivate:
        print("✓ No instruments to mark inactive")
        return

    mask = universe["instrument_id"].isin(to_deactivate) & universe["active"]
    count = int(mask.sum())

    if count == 0:
        print("✓ All removed instruments already inactive")
        return

    # -----------------------------------------------------------------
    # Mark inactive
    # -----------------------------------------------------------------

    today = datetime.now(timezone.utc).date().isoformat()

    universe.loc[mask, "active"] = False
    universe.loc[mask, "inactive_on"] = today

    universe.to_csv(UNIVERSE_PATH, index=False)

    print(f"✓ Marked {count:,} instruments inactive")
    print(f"✓ Universe size unchanged → {len(universe):,}")


if __name__ == "__main__":
    run()
