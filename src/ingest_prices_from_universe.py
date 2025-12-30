from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, UTC
from typing import Dict
import time
import gc
import csv
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import yfinance as yf
from filelock import FileLock, Timeout


# =====================================================================
# Paths
# =====================================================================

BASE_DIR = Path(__file__).resolve().parent.parent
UNIVERSE = BASE_DIR / "data" / "universe" / "gilt_universe.csv"

OUT_DIR = BASE_DIR / "data" / "prices"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRICES_OUT = OUT_DIR / "prices_1y.parquet"
FAILURES_OUT = OUT_DIR / "price_ingest_failures.csv"
LOCK_FILE = OUT_DIR / ".price_ingest.lock"


# =====================================================================
# Config
# =====================================================================

DEBUG_MODE = False
DEBUG_MAX_INSTRUMENTS = 100

DAILY_BACKFILL_DAYS = 10
MAX_LOOKBACK_DAYS = 365
CHECKPOINT_EVERY = 25

MAX_WORKERS = 4
REQUEST_SLEEP_SECONDS = 0.35
LOCK_TIMEOUT_SECONDS = 5

NO_DATA_EXCLUSION_THRESHOLD = 3
NO_DATA_PATTERNS = [
    "no data",
    "possibly delisted",
    "no timezone",
    "no objects to concatenate",
]


# =====================================================================
# Failure telemetry (learning exclusions)
# =====================================================================

FAILURE_COLUMNS = [
    "instrument_id",
    "symbol",
    "market",
    "last_attempt",
    "attempt_count",
    "last_error",
]


def load_failures() -> pd.DataFrame:
    if not FAILURES_OUT.exists():
        return pd.DataFrame(columns=FAILURE_COLUMNS)

    df = pd.read_csv(
        str(FAILURES_OUT),
        parse_dates=["last_attempt"],
        engine="python",
        on_bad_lines="skip",
    )

    for col in FAILURE_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    return df[FAILURE_COLUMNS]


def record_no_data_failure(
    instrument_id: str,
    symbol: str,
    market: str,
    error: str,
) -> None:
    df = load_failures()
    existing = df[df["instrument_id"] == instrument_id]

    attempt = int(existing["attempt_count"].iloc[0]) + 1 if not existing.empty else 1
    df = df[df["instrument_id"] != instrument_id]

    df = pd.concat(
        [
            df,
            pd.DataFrame([{
                "instrument_id": instrument_id,
                "symbol": symbol,
                "market": market,
                "last_attempt": datetime.now(UTC),
                "attempt_count": attempt,
                "last_error": error,
            }]),
        ],
        ignore_index=True,
    )

    df.to_csv(str(FAILURES_OUT), index=False, quoting=csv.QUOTE_ALL)


def load_excluded_instruments() -> set[str]:
    if not FAILURES_OUT.exists():
        return set()

    df = load_failures()

    mask = (
        df["last_error"]
        .astype(str)
        .str.lower()
        .apply(lambda x: any(p in x for p in NO_DATA_PATTERNS))
        & (df["attempt_count"] >= NO_DATA_EXCLUSION_THRESHOLD)
    )

    return set(df.loc[mask, "instrument_id"])


# =====================================================================
# Incremental state
# =====================================================================

def load_last_price_dates() -> Dict[str, pd.Timestamp]:
    if not PRICES_OUT.exists():
        return {}

    prices = pd.read_parquet(str(PRICES_OUT), columns=["instrument_id", "date"])
    prices["date"] = pd.to_datetime(prices["date"], utc=True, errors="coerce")

    raw = prices.groupby("instrument_id")["date"].max().to_dict()
    return {
        str(k): pd.Timestamp(v)
        for k, v in raw.items()
        if pd.notna(v)
    }


# =====================================================================
# Schema helpers
# =====================================================================

def ensure_series(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col]
    return s.iloc[:, 0] if isinstance(s, pd.DataFrame) else s


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()].copy() if not df.columns.is_unique else df


def normalise_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["Open", "High", "Low"]:
        df[col] = ensure_series(df, col) if col in df.columns else df["Close"]

    df["Adj Close"] = ensure_series(df, "Adj Close") if "Adj Close" in df.columns else df["Close"]
    df["Volume"] = ensure_series(df, "Volume") if "Volume" in df.columns else None
    return df


def build_price_frame(
    df: pd.DataFrame,
    instrument_id: str,
    symbol: str,
    market: str,
) -> pd.DataFrame:
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })

    df["instrument_id"] = instrument_id
    df["symbol"] = symbol
    df["market"] = market
    df["price_source"] = "yahoo"
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    return df[
        [
            "instrument_id",
            "symbol",
            "market",
            "price_source",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
        ]
    ]


# =====================================================================
# Yahoo fetch
# =====================================================================

def fetch_prices_yahoo(
    ticker: str,
    instrument_id: str,
    symbol: str,
    market: str,
    start: str,
    end: str,
) -> pd.DataFrame | None:
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            progress=False,
            threads=False,
        )
    except (ValueError, KeyError, TypeError):
        return None

    if df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = normalise_columns(df.reset_index())

    if "Date" not in df.columns or "Close" not in df.columns:
        return None

    df = normalise_ohlcv(df)
    return build_price_frame(df, instrument_id, symbol, market)


# =====================================================================
# Async wrapper
# =====================================================================

async def fetch_async(loop, executor, payload):
    await asyncio.sleep(REQUEST_SLEEP_SECONDS)
    result = await loop.run_in_executor(executor, fetch_prices_yahoo, *payload)
    return payload, result


# =====================================================================
# Checkpoint
# =====================================================================

def checkpoint_prices(frames: list[pd.DataFrame]) -> None:
    valid = [f for f in frames if isinstance(f, pd.DataFrame) and not f.empty]
    frames.clear()

    if not valid:
        return

    prices = pd.concat(valid, ignore_index=True)
    prices["date"] = pd.to_datetime(prices["date"], utc=True, errors="coerce")
    prices = prices.dropna(subset=["instrument_id", "date"])

    prices["volume"] = (
        pd.to_numeric(prices["volume"], errors="coerce")
        .round()
        .astype("Int64")
    )

    if PRICES_OUT.exists():
        existing = pd.read_parquet(str(PRICES_OUT))
        existing["date"] = pd.to_datetime(existing["date"], utc=True, errors="coerce")
        prices = pd.concat([existing, prices], ignore_index=True)

    prices = prices.drop_duplicates(["instrument_id", "date"], keep="last")
    prices.to_parquet(str(PRICES_OUT), index=False)

    print(f"✓ Prices written ({len(prices):,} rows total)")
    gc.collect()


# =====================================================================
# Main
# =====================================================================

async def run_async():
    universe = pd.read_csv(str(UNIVERSE))
    last_dates = load_last_price_dates()
    excluded = load_excluded_instruments()

    candidates = (
        universe[
            (universe["active"] == True)
            & (universe["price_eligible"] == True)
            & (~universe["instrument_id"].isin(excluded))
        ]
        .sort_values("instrument_id")
        .drop_duplicates("instrument_id")
    )

    if DEBUG_MODE:
        candidates = candidates.head(DEBUG_MAX_INSTRUMENTS)
        print(f"⚠ DEBUG MODE: limiting to {len(candidates)} instruments")

    end = datetime.now(UTC)
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    tasks = []
    for _, r in candidates.iterrows():
        last_dt = last_dates.get(r["instrument_id"])
        start_dt = (
            last_dt - timedelta(days=DAILY_BACKFILL_DAYS)
            if last_dt is not None
            else end - timedelta(days=MAX_LOOKBACK_DAYS)
        )

        payload = (
            f"{r['symbol']}.L" if r["market"] == "GB" else r["symbol"],
            r["instrument_id"],
            r["symbol"],
            r["market"],
            start_dt.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        )

        tasks.append(fetch_async(loop, executor, payload))

    ok = no_data = 0
    frames: list[pd.DataFrame] = []
    start_time = time.time()

    for idx, coro in enumerate(asyncio.as_completed(tasks), 1):
        payload, result = await coro
        _, instrument_id, symbol, market, *_ = payload

        if isinstance(result, pd.DataFrame):
            frames.append(result)
            ok += 1
        else:
            no_data += 1
            record_no_data_failure(
                instrument_id,
                symbol,
                market,
                "no usable yahoo price data",
            )

        if idx % CHECKPOINT_EVERY == 0:
            checkpoint_prices(frames)

        elapsed = time.time() - start_time
        eta = int((elapsed / idx) * (len(tasks) - idx))
        print(f"[{idx}/{len(tasks)}] ok={ok} no_data={no_data} ETA≈{eta}s")

    checkpoint_prices(frames)
    executor.shutdown(wait=True)

    print(
        f"\n✓ Run complete | tasks={len(tasks)} | "
        f"ok={ok} | no_data={no_data} | "
        f"elapsed={int(time.time() - start_time)}s"
    )


def run():
    try:
        with FileLock(str(LOCK_FILE), timeout=LOCK_TIMEOUT_SECONDS):
            asyncio.run(run_async())
    except Timeout:
        print("⚠ Another ingest already running")


if __name__ == "__main__":
    run()