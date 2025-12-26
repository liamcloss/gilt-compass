from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent

RAW = BASE_DIR / "data" / "raw" / "prices_1y.parquet"
OUT = BASE_DIR / "outputs"
OUT.mkdir(exist_ok=True)
def momentum_features(prices: pd.DataFrame) -> pd.DataFrame:
    prices = prices.sort_values(["ticker", "date"]).copy()

    # 1Y return
    first = prices.groupby("ticker").first()
    last = prices.groupby("ticker").last()
    ret_1y = (last["close"] / first["close"] - 1).rename("ret_1y")

    # Daily returns
    prices["daily_ret"] = prices.groupby("ticker")["close"].pct_change(fill_method=None)

    # 20d volatility (per ticker, take last available window)
    vol_20d = (
        prices.groupby("ticker")["daily_ret"]
        .rolling(20)
        .std()
        .groupby("ticker")
        .last()
        .rename("vol_20d")
    )

    # 20d average turnover
    avg_turnover_20d = (
        prices.assign(turnover=prices["close"] * prices["volume"])
        .groupby("ticker")["turnover"]
        .rolling(20)
        .mean()
        .groupby("ticker")
        .last()
        .rename("avg_turnover_20d")
    )

    feats = pd.concat([ret_1y, vol_20d, avg_turnover_20d], axis=1).reset_index()

    return feats

def rank(feats: pd.DataFrame) -> pd.DataFrame:
    # Normalise and score: prefer higher momentum + adequate liquidity, penalise extreme vol
    f = feats.copy()
    f["liq_ok"] = f["avg_turnover_20d"] > f["avg_turnover_20d"].median()

    # z-scores (safe)
    def z(x):
        s = x.std(ddof=0)
        return (x - x.mean()) / (s if s else 1.0)

    f["z_mom"] = z(f["ret_1y"])
    f["z_vol"] = z(f["vol_20d"])  # higher = riskier

    f["score"] = (1.0 * f["z_mom"]) + (0.3 * f["liq_ok"].astype(int)) - (0.5 * f["z_vol"])
    return f.sort_values("score", ascending=False)

if __name__ == "__main__":
    prices = pd.read_parquet(RAW)
    feats = momentum_features(prices)

    ranked = rank(feats).reset_index()

    # Normalise ticker column name after reset_index()
    if "ticker" not in ranked.columns and "index" in ranked.columns:
        ranked = ranked.rename(columns={"index": "ticker"})

    out = OUT / "ranked_v0.csv"
    ranked.to_csv(out, index=False)
    print(f"Saved: {out}")

    cols = ["ticker", "score", "ret_1y", "vol_20d", "avg_turnover_20d"]
    print(ranked.head(10)[cols].to_string(index=False))
