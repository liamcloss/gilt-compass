from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Optional, Callable, cast
import time
import gc
import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextlib
import os
import sys

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from filelock import FileLock, Timeout


# =====================================================================
# MODE
# =====================================================================
# Set once, run, forget.
#   "BOOTSTRAP" → first big universe ingest
#   "DAILY"     → normal incremental operation
# =====================================================================

MODE = "BOOTSTRAP"   # ← change to "DAILY" after first run


# =====================================================================
# Types
# =====================================================================

PricePayload = Tuple[str, str, str, Optional[str], str, str]
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
ELIGIBILITY_OUT = OUT_DIR / "price_eligibility_state.parquet"
LOCK_FILE = OUT_DIR / ".price_ingest.lock"


# =====================================================================
# Config (mode dependent)
# =====================================================================

if MODE == "BOOTSTRAP":
    MAX_WORKERS = 16
    REQUEST_SLEEP_SECONDS = 0.05
    BURST_SIZE = 400
    BURST_COOLDOWN_SECONDS = 45
    MAX_FAILURES_BEFORE_EXCLUDE = 3
else:  # DAILY
    MAX_WORKERS = 8
    REQUEST_SLEEP_SECONDS = 0.05
    BURST_SIZE = 100
    BURST_COOLDOWN_SECONDS = 120
    MAX_FAILURES_BEFORE_EXCLUDE = 3

DAILY_BACKFILL_DAYS = 10
MIN_BACKFILL_DAYS = 2
MAX_LOOKBACK_DAYS = 365

LOCK_TIMEOUT_SECONDS = 5
RATE_LIMIT_SLEEP_SECONDS = 120
CHECKPOINT_EVERY = 25


# =====================================================================
# Market close model
# =====================================================================

MARKET_CLOSES = {
    "GB": {"tz": ZoneInfo("Europe/London"), "close_time": dtime(16, 30)},
    "US": {"tz": ZoneInfo("America/New_York"), "close_time": dtime(16, 0)},
}


def normalise_market(market: Optional[str]) -> Optional[str]:
    if not isinstance(market, str):
        return None
    m = market.strip().upper()
    if m in {"GB", "GBX", "LSE", "UK"}:
        return "GB"
    if m in {"US", "NYSE", "NASDAQ"}:
        return "US"
    return None


# =====================================================================
# Stdout suppression (Yahoo noise)
# =====================================================================

@contextlib.contextmanager
def suppress_stdout_stderr():
    with open(os.devnull, "w") as devnull:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        try:
            sys.stdout = devnull
            sys.stderr = devnull
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


# =====================================================================
# Incremental state
# =====================================================================

def load_last_price_dates() -> LastDateMap:
    if not PRICES_OUT.exists():
        return {}

    prices = pd.read_parquet(PRICES_OUT, columns=["instrument_id", "date"])
    prices["date"] = pd.to_datetime(prices["date"], utc=True, errors="coerce")

    return cast(
        LastDateMap,
        prices.dropna()
        .groupby("instrument_id")["date"]
        .max()
        .to_dict()
    )


def load_eligibility_state() -> pd.DataFrame:
    if not ELIGIBILITY_OUT.exists():
        return pd.DataFrame(columns=[
            "instrument_id",
            "yahoo_ticker",
            "yahoo_verified",
            "fail_count",
            "last_attempt_utc",
            "last_success_utc",
        ])
    df = pd.read_parquet(ELIGIBILITY_OUT)
    if "fail_count" not in df.columns:
        df["fail_count"] = 0
    return df


def save_eligibility_state(state: pd.DataFrame) -> None:
    state.to_parquet(ELIGIBILITY_OUT, index=False)


def upsert_state(state: pd.DataFrame, update: dict) -> pd.DataFrame:
    iid = update["instrument_id"]
    if state.empty or iid not in set(state["instrument_id"]):
        return pd.concat([state, pd.DataFrame([update])], ignore_index=True)

    idx = state.index[state["instrument_id"] == iid][0]
    for k, v in update.items():
        state.at[idx, k] = v
    return state


def is_fresh(
    last_dt: Optional[pd.Timestamp],
    now: pd.Timestamp,
    market: Optional[str],
) -> bool:
    if last_dt is None:
        return False

    norm = normalise_market(market)
    if norm is None:
        return False

    cfg = MARKET_CLOSES[norm]
    now_local = now.tz_convert(cfg["tz"])

    latest_close = (
        now_local.date()
        if now_local.time() >= cfg["close_time"]
        else now_local.date() - timedelta(days=1)
    )

    return last_dt.date() >= latest_close


# =====================================================================
# Failure logging (audit)
# =====================================================================

def log_failure(
    instrument_id: str,
    symbol: str,
    market: Optional[str],
    reason: str,
) -> None:
    row = pd.DataFrame([{
        "ts_utc": pd.Timestamp.utcnow(),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "market": market,
        "reason": reason,
    }])

    if FAILURES_OUT.exists():
        row = pd.concat([pd.read_csv(FAILURES_OUT), row], ignore_index=True)

    row.to_csv(FAILURES_OUT, index=False)


# =====================================================================
# Yahoo fetch
# =====================================================================

def fetch_prices_yahoo(
    ticker: str,
    instrument_id: str,
    symbol: str,
    market: Optional[str],
    start: str,
    end: str,
) -> Optional[pd.DataFrame]:
    try:
        with suppress_stdout_stderr():
            df = yf.download(
                ticker,
                start=start,
                end=end,
                progress=False,
                threads=False,
            )
    except YFRateLimitError:
        raise
    except Exception:
        return None

    if df is None or df.empty:
        return None

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

    if "adj_close" not in df.columns:
        df["adj_close"] = df["close"]

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
# Async executor
# =====================================================================

def make_fetch_call(payload: PricePayload) -> Callable[[], Optional[pd.DataFrame]]:
    return lambda: fetch_prices_yahoo(*payload)


async def fetch_one(loop, executor, payload):
    await asyncio.sleep(REQUEST_SLEEP_SECONDS)
    return await loop.run_in_executor(executor, make_fetch_call(payload))


# =====================================================================
# Checkpoint
# =====================================================================

def checkpoint_prices(frames: list[pd.DataFrame]) -> None:
    if not frames:
        return

    prices = pd.concat(frames, ignore_index=True)
    frames.clear()

    prices["date"] = pd.to_datetime(prices["date"], utc=True)
    prices = prices.dropna(subset=["instrument_id", "date"])

    if PRICES_OUT.exists():
        prices = pd.concat([pd.read_parquet(PRICES_OUT), prices], ignore_index=True)

    prices.drop_duplicates(["instrument_id", "date"], keep="last", inplace=True)
    prices.to_parquet(PRICES_OUT, index=False)

    print(f"✓ Checkpoint written — total rows: {len(prices):,}")
    gc.collect()


# =====================================================================
# Main
# =====================================================================

async def run_async() -> None:
    universe = pd.read_csv(UNIVERSE)

    REQUIRED_COLUMNS = {"instrument_id", "symbol", "active", "price_eligible"}
    missing = REQUIRED_COLUMNS - set(universe.columns)
    if missing:
        raise ValueError(f"Universe missing required columns: {missing}")

    if "market" not in universe.columns:
        universe["market"] = None

    candidates = universe[
        (universe["active"] == True)
        & (universe["price_eligible"] == True)
    ]

    last_dates = load_last_price_dates()
    state = load_eligibility_state()
    state_idx = state.set_index("instrument_id") if not state.empty else pd.DataFrame()

    now = pd.Timestamp(datetime.now(timezone.utc))
    tasks: list[PricePayload] = []

    skipped_fresh = skipped_failed = 0

    for _, r in candidates.iterrows():
        iid = r["instrument_id"]
        market = r.get("market")

        if iid in state_idx.index:
            if int(state_idx.loc[iid, "fail_count"]) >= MAX_FAILURES_BEFORE_EXCLUDE:
                skipped_failed += 1
                continue

        if is_fresh(last_dates.get(iid), now, market):
            skipped_fresh += 1
            continue

        norm = normalise_market(market)
        ticker = f"{r['symbol']}.L" if norm == "GB" else r["symbol"]

        start_dt = (
            min(
                last_dates[iid] - timedelta(days=DAILY_BACKFILL_DAYS),
                now - timedelta(days=MIN_BACKFILL_DAYS),
            )
            if iid in last_dates
            else now - timedelta(days=MAX_LOOKBACK_DAYS)
        )

        tasks.append((
            ticker,
            iid,
            r["symbol"],
            market,
            start_dt.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        ))

    print(f"Mode: {MODE}")
    print(f"✓ Skipped (fresh): {skipped_fresh:,}")
    print(f"✓ Skipped (failed ≥{MAX_FAILURES_BEFORE_EXCLUDE}): {skipped_failed:,}")
    print(f"✓ Fetching prices for: {len(tasks):,}")

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    frames: list[pd.DataFrame] = []
    success = failures = rate_limits = 0
    start_time = time.time()

    try:
        for idx, payload in enumerate(tasks, 1):
            iid = payload[1]
            now_utc = pd.Timestamp.utcnow()

            try:
                result = await fetch_one(loop, executor, payload)
            except YFRateLimitError:
                rate_limits += 1
                await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            if isinstance(result, pd.DataFrame):
                frames.append(result)
                success += 1
                state = upsert_state(state, {
                    "instrument_id": iid,
                    "yahoo_ticker": payload[0],
                    "yahoo_verified": True,
                    "fail_count": 0,
                    "last_attempt_utc": now_utc,
                    "last_success_utc": now_utc,
                })
            else:
                failures += 1
                log_failure(iid, payload[2], payload[3], "no_data")
                prev = int(state_idx.loc[iid, "fail_count"]) if iid in state_idx.index else 0
                state = upsert_state(state, {
                    "instrument_id": iid,
                    "yahoo_ticker": payload[0],
                    "yahoo_verified": False,
                    "fail_count": prev + 1,
                    "last_attempt_utc": now_utc,
                    "last_success_utc": pd.NaT,
                })

            if idx % CHECKPOINT_EVERY == 0:
                checkpoint_prices(frames)
                save_eligibility_state(state)

            if idx % BURST_SIZE == 0:
                print(f"⏸ Cooling down after {idx:,} requests")
                await asyncio.sleep(BURST_COOLDOWN_SECONDS)

            eta = int((time.time() - start_time) / idx * (len(tasks) - idx))
            print(f"[{idx}/{len(tasks)}] ETA≈{eta}s")

        checkpoint_prices(frames)
        save_eligibility_state(state)

        print(
            f"\n✓ Ingest complete — "
            f"{success:,} succeeded | "
            f"{failures:,} failed | "
            f"{rate_limits:,} rate-limit events"
        )

    finally:
        executor.shutdown(wait=True)


def run() -> None:
    try:
        with FileLock(str(LOCK_FILE), timeout=LOCK_TIMEOUT_SECONDS):
            asyncio.run(run_async())
    except Timeout:
        print("⚠ Another ingest already running")


if __name__ == "__main__":
    run()
