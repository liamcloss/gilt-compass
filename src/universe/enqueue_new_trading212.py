"""
Enqueue new Trading 212 instruments into Gilt universe
AND auto-tag price eligibility.

Idempotent and safe to re-run.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

UNIVERSE = BASE_DIR / "data" / "universe" / "gilt_universe.csv"
DIFF_NEW = BASE_DIR / "data" / "universe" / "diffs" / "trading212_new.csv"


# ---------------------------------------------------------------------
# Yahoo eligibility (same logic as tag_price_source)
# ---------------------------------------------------------------------

def is_yahoo_eligible(symbol) -> bool:
    if not isinstance(symbol, str):
        return False

    symbol = symbol.strip()
    if not symbol:
        return False

    if symbol[0].isdigit():
        return False

    if not any(c.isalpha() for c in symbol):
        return False

    if len(symbol) >= 5 and symbol.endswith(("D", "R", "U", "W")):
        return False

    if not any(v in symbol.upper() for v in "AEIOU"):
        return False

    return True


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not DIFF_NEW.exists():
        print("✓ No new Trading 212 instruments to enqueue")
        return

    new = pd.read_csv(DIFF_NEW)

    if new.empty:
        print("✓ Diff file empty — nothing to enqueue")
        return

    print(f"▶ Enqueuing {len(new):,} new instruments")

    universe = pd.read_csv(UNIVERSE) if UNIVERSE.exists() else pd.DataFrame()

    # -----------------------------------------------------------------
    # Deduplicate
    # -----------------------------------------------------------------

    if not universe.empty and "instrument_id" in universe.columns:
        new = new[~new["instrument_id"].isin(universe["instrument_id"])]

    if new.empty:
        print("✓ All diff instruments already present")
        return

    # -----------------------------------------------------------------
    # Defaults
    # -----------------------------------------------------------------

    now = datetime.utcnow().date().isoformat()

    new["added_on"] = new.get("added_on", now)
    new["active"] = new.get("active", True)

    # -----------------------------------------------------------------
    # Auto-tag price eligibility
    # -----------------------------------------------------------------

    new["price_eligible"] = new["symbol"].apply(is_yahoo_eligible)
    new["price_source"] = new["price_eligible"].map(
        lambda x: "yahoo" if x else "none"
    )

    # -----------------------------------------------------------------
    # Append + persist
    # -----------------------------------------------------------------

    combined = pd.concat([universe, new], ignore_index=True)
    combined.to_csv(UNIVERSE, index=False)

    print(f"✓ Appended {len(new):,} instruments")
    print("✓ Price eligibility auto-tagged")
    print(f"✓ Universe size → {len(combined):,}")


if __name__ == "__main__":
    run()
