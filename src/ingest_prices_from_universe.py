"""
Price Ingest from Universe (Incremental, Tolerant) – v1

Key properties:
- Reads Gilt Compass universe
- Only attempts Yahoo-priceable instruments
- Never retries known failures
- Never re-downloads existing prices
- O(Δ) per run, not O(N)

State files:
- prices_1y.parquet           → successful prices
- price_ingest_failures.csv   → permanent Yahoo failures
- price_ingest_attempted.csv  → all attempted instrument_ids
"""

from pathlib import Path
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

UNIVERSE = BASE_DIR / "data" / "universe" / "gilt_universe.csv"

OUT_DIR = BASE_DIR / "data" / "prices"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRICES_OUT = OUT_DIR / "prices_1y.parquet"
FAILED_OUT = OUT_DIR / "price_ingest_failures.csv"
ATTEMPTED_OUT = OUT_DIR / "price_ingest_attempted.csv"


# ---------------------------------------------------------------------
# Yahoo eligibility (authoritative)
# ---------------------------------------------------------------------

def is_yahoo_eligible(symbol: str) -> bool:
    if not symbol:
        return False

    # Numeric / synthetic
    if symbol[0].isdigit():
        return False

    # Must contain letters
    if not any(c.isalpha() for c in symbol):
        return False

    # SPAC units, rights, warrants, notes
    if len(symbol) >= 5 and symbol.endswith(("D", "R", "U", "W")):
        return False

    # ETN / synthetic pattern (no vowels)
    if not any(v in symbol.upper() for v in "AEIOU"):
        return False

    return True


# ---------------------------------------------------------------------
# Yahoo ticker mapping (tolerant)
# ---------------------------------------------------------------------

def map_to_yahoo(symbol: str, market: str | None) -> list[str]:
    candidates = []

    if market == "US":
        candidates.append(symbol)
    elif market == "GB":
        candidates.append(f"{symbol}.L")
    elif market == "DE":
        candidates.append(f"{symbol}.DE")
    elif market == "FR":
        candidates.append(f"{symbol}.PA")
    elif market == "NL":
        candidates.append(f"{symbol}.AS")

    # Fallback
    candidates.append(symbol)

    # Deduplicate while preserving order
    return list(dict.fromkeys(candidates))


# ---------------------------------------------------------------------
# Fetch prices
# ---------------------------------------------------------------------

def fetch_prices(yahoo_ticker: str, start: str, end: str) -> pd.DataFrame | None:
    try:
        df = yf.download(
            yahoo_ticker,
            start=start,
            end=end,
            auto_adjust=False,
            progress=False,
            threads=False,
        )

        if df.empty:
            return None

        df = df.reset_index()
        df["yahoo_ticker"] = yahoo_ticker
        return df

    except Exception:
        return None


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    universe = pd.read_csv(UNIVERSE)

    # -----------------------------------------------------------------
    # Load state
    # -----------------------------------------------------------------

    attempted = set()
    if ATTEMPTED_OUT.exists():
        attempted = set(pd.read_csv(ATTEMPTED_OUT)["instrument_id"])

    failures_df = pd.read_csv(FAILED_OUT) if FAILED_OUT.exists() else pd.DataFrame()
    failed = set(failures_df["instrument_id"]) if not failures_df.empty else set()

    existing_prices = set()
    if PRICES_OUT.exists():
        existing_prices = set(
            pd.read_parquet(PRICES_OUT)["instrument_id"].unique()
        )

    # -----------------------------------------------------------------
    # Candidate selection (THIS IS THE KEY FIX)
    # -----------------------------------------------------------------

    candidates = universe[
        (universe["active"] == True)
        & (universe["price_eligible"] == True)
        & (~universe["instrument_id"].isin(attempted))
        & (~universe["instrument_id"].isin(existing_prices))
        & (~universe["instrument_id"].isin(failed))
    ]

    print(f"▶ Attempting prices for {len(candidates):,} new instruments")

    if candidates.empty:
        print("✓ No new priceable instruments to ingest")
        return

    end = datetime.today()
    start = end - timedelta(days=365)

    all_prices = []
    new_failures = []

    # -----------------------------------------------------------------
    # Ingest loop
    # -----------------------------------------------------------------

    for _, row in candidates.iterrows():
        instrument_id = row["instrument_id"]
        symbol = row["symbol"]
        market = row["market"]

        tried = []
        success = False

        for yahoo_ticker in map_to_yahoo(symbol, market):
            tried.append(yahoo_ticker)

            df = fetch_prices(
                yahoo_ticker,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
            )

            if df is not None:
                df["instrument_id"] = instrument_id
                df["symbol"] = symbol
                df["market"] = market
                all_prices.append(df)
                success = True
                break

            time.sleep(0.5)

        attempted.add(instrument_id)

        if not success:
            new_failures.append({
                "instrument_id": instrument_id,
                "symbol": symbol,
                "market": market,
                "tried": ";".join(tried),
            })

    # -----------------------------------------------------------------
    # Persist results
    # -----------------------------------------------------------------

    if all_prices:
        prices = pd.concat(all_prices, ignore_index=True)
        prices = prices.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        })

        if PRICES_OUT.exists():
            prices = pd.concat(
                [pd.read_parquet(PRICES_OUT), prices],
                ignore_index=True,
            )

        prices.to_parquet(PRICES_OUT, index=False)
        print(f"✓ Prices written → {PRICES_OUT}")

    if new_failures:
        failures_df = pd.concat(
            [failures_df, pd.DataFrame(new_failures)],
            ignore_index=True,
        )
        failures_df.to_csv(FAILED_OUT, index=False)
        print(f"⚠ New failures written → {FAILED_OUT}")

    pd.DataFrame(
        sorted(attempted), columns=["instrument_id"]
    ).to_csv(ATTEMPTED_OUT, index=False)

    print("\n✓ Incremental price ingest complete")


if __name__ == "__main__":
    run()
