from __future__ import annotations

from pathlib import Path
from typing import Optional
import pandas as pd


# =====================================================================
# Paths
# =====================================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
UNIVERSE = BASE_DIR / "data" / "universe" / "gilt_universe.csv"
BACKUP = BASE_DIR / "data" / "universe" / "gilt_universe.backup.csv"


# =====================================================================
# Market helpers (same logic as ingest)
# =====================================================================

def normalise_market(market: Optional[str]) -> Optional[str]:
    if not isinstance(market, str):
        return None
    m = market.strip().upper()
    if m in {"GB", "GBX", "LSE", "UK"}:
        return "GB"
    if m in {"US", "NYSE", "NASDAQ"}:
        return "US"
    return None


def infer_market_from_instrument(
    instrument_id: str,
    symbol: str,
) -> Optional[str]:
    if instrument_id.endswith("l_EQ"):
        return "GB"
    if instrument_id.endswith("_US_EQ"):
        return "US"
    if instrument_id.endswith("_CA_EQ"):
        return "CA"

    # very conservative symbol-only fallback
    if symbol.isupper() and symbol.isalpha() and len(symbol) <= 5:
        return "US"

    return None


# =====================================================================
# Main
# =====================================================================

def main() -> None:
    df = pd.read_csv(UNIVERSE)

    if "market" not in df.columns:
        df["market"] = None

    # backup once
    if not BACKUP.exists():
        df.to_csv(BACKUP, index=False)
        print(f"[backup] written → {BACKUP.name}")

    filled = 0
    unresolved = 0

    for idx, r in df.iterrows():
        current = normalise_market(r.get("market"))
        if current:
            df.at[idx, "market"] = current
            continue

        inferred = infer_market_from_instrument(
            instrument_id=r["instrument_id"],
            symbol=r["symbol"],
        )

        if inferred:
            df.at[idx, "market"] = inferred
            filled += 1
        else:
            unresolved += 1

    df.to_csv(UNIVERSE, index=False)

    print("[done] market backfill complete")
    print(f"  filled     : {filled}")
    print(f"  unresolved : {unresolved}")
    print(f"  total      : {len(df)}")


if __name__ == "__main__":
    main()
