from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, UTC
from typing import Dict, Tuple, Optional, Callable, cast
import time
import gc
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from filelock import FileLock, Timeout


# =====================================================================
# Types
# =====================================================================

PricePayload = Tuple[str, str, str, str, str, str]  # ticker, instrument_id, symbol, market, start, end
LastDateMap = Dict[str, pd.Timestamp]


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

PRICE_FRESHNESS_HOURS = 12

DAILY_BACKFILL_DAYS = 10
MIN_BACKFILL_DAYS = 2
MAX_LOOKBACK_DAYS = 365

MAX_WORKERS = 2
REQUEST_SLEEP_SECONDS = 1.0
LOCK_TIMEOUT_SECONDS = 5

RATE_LIMIT_SLEEP_SECONDS = 120
RATE_LIMIT_MAX_RETRIES = 3
RATE_LIMIT_ABORT_THRESHOLD = 5

CHECKPOINT_EVERY = 25


# =====================================================================
# Incremental state
# =====================================================================

def load_last_price_dates() -> LastDateMap:
    if not PRICES_OUT.exists():
        return {}

    prices = pd.read_parquet(PRICES_OUT, columns=["instrument_id", "date"])
    prices["date"] = pd.to_datetime(prices["date"], utc=True, errors="coerce")

    raw = (
        prices
        .dropna(subset=["instrument_id", "date"])
        .groupby("instrument_id")["date"]
        .max()
        .to_dict()
    )

    return cast(LastDateMap, raw)


def is_fresh(last_dt: Optional[pd.Timestamp], now: pd.Timestamp) -> bool:
    return last_dt is not None and (now - last_dt) < timedelta(hours=PRICE_FRESHNESS_HOURS)


# =====================================================================
# Yahoo fetch (sync, thread-safe)
# =====================================================================

def fetch_prices_yahoo(
    ticker: str,
    instrument_id: str,
    symbol: str,
    market: str,
    start: str,
    end: str,
) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            progress=False,
            threads=False,
        )
    except YFRateLimitError:
        raise
    except (ValueError, KeyError, TypeError):
        return None

    # yfinance often rate-limits silently
    if df is None or df.empty:
        raise YFRateLimitError()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.reset_index()

    if "Date" not in df.columns or "Close" not in df.columns:
        return None

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
# Executor wrapper (STRICT zero-arg callable)
# =====================================================================

def make_fetch_call(payload: PricePayload) -> Callable[[], Optional[pd.DataFrame]]:
    def _call() -> Optional[pd.DataFrame]:
        return fetch_prices_yahoo(*payload)
    return _call


async def fetch_one(
    loop: asyncio.AbstractEventLoop,
    executor: ThreadPoolExecutor,
    payload: PricePayload,
) -> Optional[pd.DataFrame]:
    await asyncio.sleep(REQUEST_SLEEP_SECONDS)
    call = make_fetch_call(payload)
    return await loop.run_in_executor(executor, call)


# =====================================================================
# Checkpoint
# =====================================================================

def checkpoint_prices(frames: list[pd.DataFrame]) -> None:
    if not frames:
        return

    prices = pd.concat(frames, ignore_index=True)
    frames.clear()

    prices["date"] = pd.to_datetime(prices["date"], utc=True, errors="coerce")
    prices = prices.dropna(subset=["instrument_id", "date"])

    if PRICES_OUT.exists():
        existing = pd.read_parquet(PRICES_OUT)
        existing["date"] = pd.to_datetime(existing["date"], utc=True, errors="coerce")
        prices = pd.concat([existing, prices], ignore_index=True)

    prices = prices.drop_duplicates(["instrument_id", "date"], keep="last")
    prices.to_parquet(PRICES_OUT, index=False)

    print(f"✓ Prices written ({len(prices):,} rows)")
    gc.collect()


# =====================================================================
# Main
# =====================================================================

async def run_async() -> None:
    universe = pd.read_csv(UNIVERSE)
    last_dates = load_last_price_dates()

    now = pd.Timestamp(datetime.now(utc))

    candidates = universe[
        (universe["active"] == True)
        & (universe["price_eligible"] == True)
    ]

    tasks: list[PricePayload] = []
    skipped = 0

    for _, r in candidates.iterrows():
        last_dt = last_dates.get(r["instrument_id"])

        if is_fresh(last_dt, now):
            skipped += 1
            continue

        start_dt = (
            max(last_dt - timedelta(days=DAILY_BACKFILL_DAYS), now - timedelta(days=MIN_BACKFILL_DAYS))
            if last_dt is not None
            else now - timedelta(days=MAX_LOOKBACK_DAYS)
        )

        tasks.append((
            f"{r['symbol']}.L" if r["market"] == "GB" else r["symbol"],
            r["instrument_id"],
            r["symbol"],
            r["market"],
            start_dt.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        ))

    print(f"✓ Skipped fresh (<{PRICE_FRESHNESS_HOURS}h): {skipped:,}")
    print(f"✓ Fetching prices for: {len(tasks):,}")

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    frames: list[pd.DataFrame] = []
    rate_limit_hits = 0
    start_time = time.time()

    for idx, payload in enumerate(tasks, 1):
        retries = 0

        while True:
            try:
                result = await fetch_one(loop, executor, payload)
                break
            except YFRateLimitError:
                rate_limit_hits += 1
                retries += 1

                if rate_limit_hits >= RATE_LIMIT_ABORT_THRESHOLD:
                    raise RuntimeError("Yahoo rate-limit circuit breaker tripped")

                if retries > RATE_LIMIT_MAX_RETRIES:
                    raise RuntimeError("Yahoo rate-limit persists")

                print(f"⚠ Yahoo rate limit — sleeping {RATE_LIMIT_SLEEP_SECONDS}s")
                await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)

        if isinstance(result, pd.DataFrame):
            frames.append(result)

        if idx % CHECKPOINT_EVERY == 0:
            checkpoint_prices(frames)

        eta = int((time.time() - start_time) / idx * (len(tasks) - idx))
        print(f"[{idx}/{len(tasks)}] ETA≈{eta}s")

    checkpoint_prices(frames)
    executor.shutdown(wait=True)


def run() -> None:
    try:
        with FileLock(str(LOCK_FILE), timeout=LOCK_TIMEOUT_SECONDS):
            asyncio.run(run_async())
    except Timeout:
        print("⚠ Another ingest already running")


if __name__ == "__main__":
    run()
