"""
Build Active Universe (Hybrid)

Purpose:
- Auto-generate a tradable watchlist from inbound data
- Eligibility filter ONLY (not a signal)
- Designed for short-term (15–20%) opportunity hunting

Runs weekly or manually.
Deterministic. Inspectable.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime, UTC, timedelta


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

PRICES = BASE_DIR / "data" / "prices" / "prices_1y.parquet"
SCORES = BASE_DIR / "outputs" / "scores_current.parquet"

OUT_DIR = BASE_DIR / "data" / "watchlists"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "active_universe.csv"


# ---------------------------------------------------------------------
# Config — tune later, start conservative
# ---------------------------------------------------------------------

MAX_PRICE_AGE_DAYS = 2

MIN_TURNOVER_20D = 500_000        # £500k
MIN_VOL_20D = 0.015               # avoid dead names
MAX_VOL_20D = 0.06                # avoid already-exploded names
#TODO - REINTRODUCE ABS SCORE
#MIN_ABS_SCORE = 0.25              # optional quality floor

MAX_UNIVERSE_SIZE = 250           # hard cap for cognitive safety


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not PRICES.exists() or not SCORES.exists():
        print("✗ Missing prices or scores — cannot build watchlist")
        return

    prices = pd.read_parquet(PRICES)
    scores = pd.read_parquet(SCORES)

    prices["date"] = pd.to_datetime(prices["date"], utc=True)

    # -----------------------------------------------------------------
    # Latest price per instrument (freshness + liquidity + vol)
    # -----------------------------------------------------------------

    latest = (
        prices
        .sort_values("date")
        .groupby("instrument_id")
        .tail(1)
        .rename(columns={"date": "last_price_date"})
    )

    now = datetime.now(UTC)
    fresh_cutoff = now - timedelta(days=MAX_PRICE_AGE_DAYS)

    eligible = latest[
        latest["last_price_date"] >= fresh_cutoff
    ].copy()

    # -----------------------------------------------------------------
    # Merge in scores
    # -----------------------------------------------------------------

    eligible = eligible.merge(
        scores,
        on="instrument_id",
        how="inner",
    )

    # -----------------------------------------------------------------
    # Apply HYBRID eligibility filters
    # -----------------------------------------------------------------

    eligible = eligible[
        (eligible["avg_turnover_20d"] >= MIN_TURNOVER_20D) &
        (eligible["vol_20d"] >= MIN_VOL_20D) &
        (eligible["vol_20d"] <= MAX_VOL_20D)
        #& eligible["score"] >= MIN_ABS_SCORE)
    ].copy()

    if eligible.empty:
        print("✓ No instruments met active universe criteria")
        return

    # -----------------------------------------------------------------
    # Rank & cap universe size (stability > coverage)
    # -----------------------------------------------------------------

    eligible["rank_score"] = (
        eligible["score"].rank(ascending=False)
    )

    eligible = (
        eligible
        .sort_values("rank_score")
        .head(MAX_UNIVERSE_SIZE)
    )

    # -----------------------------------------------------------------
    # Persist
    # -----------------------------------------------------------------

    out = eligible[[
        "instrument_id",
        "score",
        "avg_turnover_20d",
        "vol_20d",
        "last_price_date",
    ]].copy()

    out["generated_at"] = now.isoformat()

    out.to_csv(OUT_FILE, index=False)

    print(f"✓ Active universe written → {OUT_FILE}")
    print(f"✓ Instruments eligible: {len(out):,}")


if __name__ == "__main__":
    run()
