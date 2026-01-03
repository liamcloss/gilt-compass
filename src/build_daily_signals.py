from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import numpy as np


# ============================================================
# Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
PRICES = BASE_DIR / "data" / "prices" / "prices_1y.parquet"

OUT_DIR = BASE_DIR / "outputs"
OUT_DIR.mkdir(exist_ok=True)
OUT_FILE = OUT_DIR / "daily_attention.txt"

# ============================================================
# Parameters
# ============================================================

VOL_SIGMA_THRESHOLD = 2.0
VOLUME_RATIO_THRESHOLD = 1.8
VOL_LOOKBACK = 20
RANGE_LOOKBACK = 30
MAX_ITEMS = 5

# ============================================================
# Load prices
# ============================================================

df = pd.read_parquet(PRICES)
df["date"] = pd.to_datetime(df["date"], utc=True)

# ============================================================
# Process instrument by instrument (simple & safe)
# ============================================================

events = []

for instrument_id, g in df.groupby("instrument_id"):
    g = g.sort_values("date")

    if len(g) < max(VOL_LOOKBACK, RANGE_LOOKBACK) + 1:
        continue

    last = g.iloc[-1]
    prev = g.iloc[-2]

    if prev["close"] == 0:
        continue

    # --- 1d return
    ret_1d = (last["close"] / prev["close"]) - 1

    # --- volatility
    returns = g["close"].pct_change(fill_method=None).dropna()
    vol_20d = returns.iloc[-VOL_LOOKBACK:].std()

    if pd.isna(vol_20d) or vol_20d == 0:
        continue

    shock_sigma = abs(ret_1d) / vol_20d

    # --- volume
    avg_vol_20d = g["volume"].iloc[-VOL_LOOKBACK:].mean()
    volume_ratio = (
        last["volume"] / avg_vol_20d
        if avg_vol_20d and avg_vol_20d > 0
        else np.nan
    )

    # --- range
    max_30d = g["close"].iloc[-RANGE_LOOKBACK:].max()
    min_30d = g["close"].iloc[-RANGE_LOOKBACK:].min()

    range_break = None
    if last["close"] > max_30d:
        range_break = "UP"
    elif last["close"] < min_30d:
        range_break = "DOWN"

    # --- signals (FORCE BOOLEANS)
    signal_price = bool(shock_sigma >= VOL_SIGMA_THRESHOLD)
    signal_volume = (
        bool(volume_ratio >= VOLUME_RATIO_THRESHOLD)
        if not pd.isna(volume_ratio)
        else False
    )
    signal_range = range_break is not None

    signal_count = int(signal_price) + int(signal_volume) + int(signal_range)

    if signal_count == 0:
        continue

    events.append({
        "instrument_id": instrument_id,
        "symbol": last.get("symbol", "UNKNOWN"),
        "market": last.get("market", "?"),
        "ret_1d": ret_1d,
        "shock_sigma": shock_sigma,
        "volume_ratio": volume_ratio,
        "range_break": range_break,
        "signal_price": signal_price,
        "signal_volume": signal_volume,
        "signal_range": signal_range,
        "signal_count": signal_count,
    })

# ============================================================
# Rank + cap
# ============================================================

events_df = pd.DataFrame(events)

if not events_df.empty:
    events_df = events_df.sort_values(
        ["signal_count", "shock_sigma"],
        ascending=[False, False]
    ).head(MAX_ITEMS)

# ============================================================
# Human output
# ============================================================

lines = []
today = datetime.now(UTC).date()

lines.append(f"DAILY MARKET ATTENTION — {today}")
lines.append("=" * 40)
lines.append("")

if events_df.empty:
    lines.append("No material price events detected.")
else:
    for i, r in enumerate(events_df.itertuples(), 1):
        sigs = []
        if r.signal_price:
            sigs.append("PRICE SHOCK")
        if r.signal_volume:
            sigs.append("VOLUME")
        if r.signal_range:
            sigs.append(f"RANGE {r.range_break}")

        lines.append(f"{i}) {r.symbol} ({r.market})")
        lines.append(f"   Signals: {' + '.join(sigs)}")
        lines.append(
            f"   • 1d move: {r.ret_1d:+.2%} ({r.shock_sigma:.1f}σ)"
        )

        if not pd.isna(r.volume_ratio):
            lines.append(
                f"   • Volume: {r.volume_ratio:.1f}× 20d avg"
            )

        if r.range_break:
            lines.append(
                f"   • Context: 30-day {r.range_break.lower()}"
            )

        lines.append("")

lines.append(f"({len(events_df)} items)")

OUT_FILE.write_text("\n".join(lines), encoding="utf-8")
print(f"✓ Daily attention file written → {OUT_FILE}")
