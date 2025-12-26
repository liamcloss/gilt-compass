"""
Mark inactive Trading 212 instruments in Gilt universe.

Rules:
- If an instrument_id exists in gilt_universe
  but NOT in the latest Trading 212 snapshot,
  mark it inactive.
- Never delete rows.
- Idempotent and safe to re-run.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

UNIVERSE = BASE_DIR / "data" / "universe" / "gilt_universe.csv"

# Adjust if your snapshot naming differs
LATEST_SNAPSHOT = (
    BASE_DIR / "data" / "universe" / "raw" / "trading212_raw_latest.parquet"
)


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not UNIVERSE.exists():
        raise RuntimeError("gilt_universe.csv not found")

    if not LATEST_SNAPSHOT.exists():
        raise RuntimeError("Latest Trading 212 snapshot not found")

    universe = pd.read_csv(UNIVERSE)
    snapshot = pd.read_parquet(LATEST_SNAPSHOT)

    if "instrument_id" not in universe.columns:
        raise RuntimeError("Universe missing instrument_id")

    if "instrument_id" not in snapshot.columns:
        raise RuntimeError("Snapshot missing instrument_id")

    # -----------------------------------------------------------------
    # Identify removals
    # -----------------------------------------------------------------

    universe_ids = set(universe["instrument_id"])
    snapshot_ids = set(snapshot["instrument_id"])

    to_deactivate = universe_ids - snapshot_ids

    if not to_deactivate:
        print("✓ No instruments to mark inactive")
        return

    mask = universe["instrument_id"].isin(to_deactivate) & (universe["active"] == True)
    count = mask.sum()

    if count == 0:
        print("✓ All removed instruments already inactive")
        return

    # -----------------------------------------------------------------
    # Mark inactive
    # -----------------------------------------------------------------

    today = datetime.utcnow().date().isoformat()

    universe.loc[mask, "active"] = False
    universe.loc[mask, "inactive_on"] = today

    universe.to_csv(UNIVERSE, index=False)

    print(f"✓ Marked {count:,} instruments inactive")
    print(f"✓ Universe size unchanged → {len(universe):,}")


if __name__ == "__main__":
    run()
