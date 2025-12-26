"""
Incremental Scoring – v1

Scores only instruments whose price data has changed
since the last scoring run.

Inputs:
- data/prices/prices_1y.parquet

Outputs:
- outputs/scores_current.parquet
- outputs/score_runs.csv
"""

from pathlib import Path
import pandas as pd
from datetime import datetime
import numpy as np


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

PRICES = BASE_DIR / "data" / "prices" / "prices_1y.parquet"

OUT_DIR = BASE_DIR / "outputs"
OUT_DIR.mkdir(exist_ok=True)

SCORES = OUT_DIR / "scores_current.parquet"
RUN_LOG = OUT_DIR / "score_runs.csv"


# ---------------------------------------------------------------------
# Feature engineering (same logic as before, scoped per instrument)
# ---------------------------------------------------------------------

def score_instrument(df: pd.DataFrame) -> dict:
    """
    Compute momentum-style features for a single instrument.
    """

    df = df.sort_values("date")

    if len(df) < 60:
        return None  # insufficient history

    first = df.iloc[0]
    last = df.iloc[-1]

    ret_1y = (last["close"] / first["close"]) - 1

    df["daily_ret"] = df["close"].pct_change()
    vol_20d = df["daily_ret"].rolling(20).std().iloc[-1]

    avg_turnover_20d = (
        df["close"].rolling(20).mean().iloc[-1]
        * df["volume"].rolling(20).mean().iloc[-1]
    )

    return {
        "ret_1y": ret_1y,
        "vol_20d": vol_20d,
        "avg_turnover_20d": avg_turnover_20d,
    }


def zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    return (series - series.mean()) / (std if std else 1.0)


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    prices = pd.read_parquet(PRICES)

    # ---------------------------------------------------------------
    # Determine what needs scoring
    # ---------------------------------------------------------------

    prices["date"] = pd.to_datetime(prices["date"])
    last_price_date = prices.groupby("instrument_id")["date"].max()

    if SCORES.exists():
        existing = pd.read_parquet(SCORES)
        scored_ids = set(existing["instrument_id"])
    else:
        existing = pd.DataFrame()
        scored_ids = set()

    to_score = set(last_price_date.index) - scored_ids

    print(f"▶ Scoring {len(to_score):,} instruments")

    if not to_score:
        print("✓ No new instruments to score")
        return

    # ---------------------------------------------------------------
    # Score only new instruments
    # ---------------------------------------------------------------

    rows = []

    for instrument_id in to_score:
        df = prices[prices["instrument_id"] == instrument_id]
        features = score_instrument(df)

        if features is None:
            continue

        rows.append({
            "instrument_id": instrument_id,
            **features,
            "scored_at": datetime.utcnow().isoformat(),
        })

    scored = pd.DataFrame(rows)

    if scored.empty:
        print("✓ No instruments met scoring criteria")
        return

    # ---------------------------------------------------------------
    # Normalise & score (relative to new batch)
    # ---------------------------------------------------------------

    scored["z_mom"] = zscore(scored["ret_1y"])
    scored["z_vol"] = zscore(scored["vol_20d"])

    scored["score"] = (
        scored["z_mom"]
        - 0.5 * scored["z_vol"]
    )

    # ---------------------------------------------------------------
    # Persist
    # ---------------------------------------------------------------

    if not existing.empty:
        scored = pd.concat([existing, scored], ignore_index=True)

    scored.to_parquet(SCORES, index=False)

    run_log = pd.DataFrame([{
        "timestamp": datetime.utcnow().isoformat(),
        "new_scored": len(rows),
    }])

    if RUN_LOG.exists():
        run_log = pd.concat(
            [pd.read_csv(RUN_LOG), run_log],
            ignore_index=True,
        )

    run_log.to_csv(RUN_LOG, index=False)

    print(f"✓ Scores updated → {SCORES}")
    print(f"✓ Run logged → {RUN_LOG}")


if __name__ == "__main__":
    run()
