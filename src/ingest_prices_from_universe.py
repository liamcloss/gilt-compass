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
# "BOOTSTRAP" → first full universe ingest
# "DAILY"     → incremental runs
# =====================================================================

MODE = "BOOTSTRAP"


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
else:
    MAX_WORKERS = 8
    REQUEST_SLEEP_SECONDS = 0.05
    BURST_SIZE = 100
    BURST_COOLDOWN_SECONDS = 120

MAX_FAILURES_BEFORE_EXCLUDE = 3
DAILY_BACKFILL_DAYS = 10
MIN_BACKFILL_DAYS = 2
MAX_LOOKBACK_DAYS = 365
CHECKPOINT_EVERY = 25
LOCK_TIMEOUT_SECONDS = 5
RATE_LIMIT_SLEEP_SECONDS = 120
SUMMARY_EVERY = 50


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
# Trading212 → Yahoo ticker derivation (fail closed)
# =====================================================================

def derive_yahoo_ticker(
    instrument_id: str,
    symbol: str,
    market: Optional[str],
) -> Optional[str]:
    # Clean symbol already suitable
    if symbol.isupper() and symbol.isalpha() and 1 <= len(symbol) <= 5:
        return symbol

    # LSE / AIM
    if instrument_id.endswith("l_EQ"):
        base = instrument_id.replace("l_EQ", "")
        if base.isalnum():
            return f"{base}.L"

    # US
    if instrument_id.endswith("_US_EQ"):
        base = instrument_id.replace("_US_EQ", "")
        if base.isalnum():
            return base

    # Canada
    if instrument_id.endswith("_CA_EQ"):
        base = instrument_id.replace("_CA_EQ", "")
        if base.isalnum():
            return f"{base}.TO"

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
        # Explicit initialisation (authoritative file)
        df = pd.DataFrame({
            "instrument_id": pd.Series(dtype="string"),
            "yahoo_ticker": pd.Series(dtype="string"),
            "yahoo_verified": pd.Series(dtype="boolean"),
            "fail_count": pd.Series(dtype="int64"),
            "last_attempt_utc": pd.Series(dtype="datetime64[ns, UTC]"),
            "last_success_utc": pd.Series(dtype="datetime64[ns, UTC]"),
        })
        df.to_parquet(ELIGIBILITY_OUT, index=False)
        return df
    return pd.read_parquet(ELIGIBILITY_OUT)


def save_eligibility_state(state: pd.DataFrame) -> None:
    state.to_parquet(ELIGIBILITY_OUT, index=False)


def upsert_state(state: pd.DataFrame, update: dict) -> pd.DataFrame:
    iid = update["instrument_id"]
    matches = state.index[state["instrument_id"] == iid]
    if len(matches) == 0:
        # append without concat
        new_idx = len(state)
        for col in state.columns:
            state.at[new_idx, col] = update.get(col, pd.NA)
        return state
    idx = matches[0]
    for k, v in update.items():
        if k in state.columns:
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
            "instrument_id", "symbol", "market", "price_source",
            "date", "open", "high", "low", "close", "adj_close", "volume",
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
    gc.collect()


# =====================================================================
# Per-ticker CLI status helper
# =====================================================================

def ticker_status(idx: int, total: int, symbol: str, status: str, detail: str = "") -> None:
    msg = f"[{idx}/{total}] {symbol:<10} → {status}"
    if detail:
        msg += f" ({detail})"
    print(msg)


# =====================================================================
# Main
# =====================================================================

async def run_async() -> None:
    universe = pd.read_csv(UNIVERSE)

    required = {"instrument_id", "symbol", "active", "price_eligible"}
    if not required.issubset(universe.columns):
        raise ValueError(f"Universe missing required columns: {required - set(universe.columns)}")

    if "market" not in universe.columns:
        universe["market"] = None

    print(f"[phase] universe loaded — {len(universe):,} rows")

    candidates = universe[
        (universe["active"] == True)
        & (universe["price_eligible"] == True)
    ]
    print(f"[phase] candidates selected — {len(candidates):,}")

    last_dates = load_last_price_dates()
    state = load_eligibility_state()
    state_idx = state.set_index("instrument_id") if not state.empty else pd.DataFrame()

    now = pd.Timestamp(datetime.now(timezone.utc))
    tasks: list[PricePayload] = []

    for _, r in candidates.iterrows():
        iid = r["instrument_id"]

        if iid in state_idx.index and int(state_idx.loc[iid, "fail_count"]) >= MAX_FAILURES_BEFORE_EXCLUDE:
            continue

        if is_fresh(last_dates.get(iid), now, r.get("market")):
            continue

        ticker = derive_yahoo_ticker(iid, r["symbol"], r.get("market"))
        if ticker is None:
            log_failure(iid, r["symbol"], r.get("market"), "no_yahoo_mapping")
            continue

        start_dt = (
            min(
                last_dates[iid] - timedelta(days=DAILY_BACKFILL_DAYS),
                now - timedelta(days=MIN_BACKFILL_DAYS),
            )
            if iid in last_dates
            else now - timedelta(days=MAX_LOOKBACK_DAYS)
        )

        tasks.append((
            ticker, iid, r["symbol"], r.get("market"),
            start_dt.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d"),
        ))

    total = len(tasks)
    print(f"[phase] tasks built — {total:,}")
    print(f"[phase] fetch loop started (mode={MODE}, workers={MAX_WORKERS})")

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    frames: list[pd.DataFrame] = []
    success = failures = rate_limits = 0
    start_time = time.time()

    try:
        for idx, payload in enumerate(tasks, 1):
            iid = payload[1]
            symbol = payload[2]
            now_utc = pd.Timestamp.utcnow()

            try:
                result = await fetch_one(loop, executor, payload)
            except YFRateLimitError:
                rate_limits += 1
                ticker_status(idx, total, symbol, "RATE_LIMIT")
                print(f"[rate-limit] sleeping {RATE_LIMIT_SLEEP_SECONDS}s")
                await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            if isinstance(result, pd.DataFrame):
                success += 1
                frames.append(result)
                ticker_status(idx, total, symbol, "FETCHED", f"{len(result)} rows")
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
                prev = int(state_idx.loc[iid, "fail_count"]) if iid in state_idx.index else 0
                ticker_status(idx, total, symbol, "NO_DATA", f"fail={prev+1}")
                log_failure(iid, symbol, payload[3], "no_data")
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
                print(f"[cooldown] burst pause {BURST_COOLDOWN_SECONDS}s after {idx:,}")
                await asyncio.sleep(BURST_COOLDOWN_SECONDS)

            if idx % SUMMARY_EVERY == 0:
                elapsed = int(time.time() - start_time)
                print(
                    f"[summary] {idx}/{total} | "
                    f"ok={success} no_data={failures} rl={rate_limits} | "
                    f"elapsed={elapsed}s"
                )

        checkpoint_prices(frames)
        save_eligibility_state(state)

        elapsed = int(time.time() - start_time)
        print(
            f"[done] {success} fetched | {failures} no_data | "
            f"{rate_limits} rate_limit | elapsed={elapsed}s"
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
