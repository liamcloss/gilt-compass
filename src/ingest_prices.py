import pandas as pd
import yfinance as yf
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

RAW = BASE_DIR / "data" / "raw" / "prices_1y.parquet"
RAW.parent.mkdir(parents=True, exist_ok=True)

def load_universe():
    path = BASE_DIR / "data" / "universe.csv"
    return pd.read_csv(path)["ticker"].dropna().tolist()


def fetch_prices(tickers, period="1y"):
    data = yf.download(
        tickers,
        period=period,
        auto_adjust=True,
        group_by="ticker",
        progress=False,
    )

    rows = []
    for t in tickers:
        if t not in data.columns.get_level_values(0):
            continue

        df = data[t].reset_index()
        df["ticker"] = t
        df = df.rename(columns=str.lower)
        rows.append(df[["date", "open", "high", "low", "close", "volume", "ticker"]])

    return pd.concat(rows, ignore_index=True)

if __name__ == "__main__":
    tickers = load_universe()
    prices = fetch_prices(tickers)
    out = RAW
    prices.to_parquet(out, index=False)
    print(f"Saved {len(prices)} rows → {out}")
