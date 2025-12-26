"""
Trading 212 Universe Ingest (v0)

Purpose:
- Pull full Trading 212 instruments catalogue
- Derive ISA-permitted universe (heuristic, explicit)
- Persist raw + ISA-filtered snapshots with date versioning

No pipeline integration yet.
No scoring.
No opinions.
"""

from pathlib import Path
from datetime import date
import os
import base64
import requests
import pandas as pd
import time

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data" / "universe" / "raw"
ISA_DIR = BASE_DIR / "data" / "universe" / "isa"

RAW_DIR.mkdir(parents=True, exist_ok=True)
ISA_DIR.mkdir(parents=True, exist_ok=True)

TODAY = date.today().isoformat()

RAW_OUT = RAW_DIR / f"trading212_raw_{TODAY}.parquet"
ISA_OUT = ISA_DIR / f"trading212_isa_{TODAY}.parquet"

# ---------------------------------------------------------------------
# Trading 212 API config
# ---------------------------------------------------------------------

BASE_URL = "https://live.trading212.com/api/v0"

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "API_KEY and API_SECRET must be set as env vars"
    )

auth = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
HEADERS = {
    "Authorization": f"Basic {auth}",
    "Accept": "application/json",
}

# ---------------------------------------------------------------------
# ISA eligibility rules (v0 — explicit and conservative)
# ---------------------------------------------------------------------

ALLOWED_TYPES = {
    "STOCK",
    "ETF",
    "INVESTMENT_TRUST",
    "REIT",
}

# Recognised exchanges (expand cautiously)
ALLOWED_EXCHANGES = {
    "LSE",
    "NASDAQ",
    "NYSE",
    "XETRA",
    "EURONEXT",
}

# ---------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------
def fetch_instruments(max_retries: int = 5, base_delay: float = 30.0) -> pd.DataFrame:
    """
    Fetch Trading 212 instruments with polite retry + backoff.
    Handles 429 rate limiting explicitly.
    """

    url = f"{BASE_URL}/equity/metadata/instruments"

    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=HEADERS, timeout=30)

        if resp.status_code == 200:
            return pd.DataFrame(resp.json())

        if resp.status_code == 429:
            delay = base_delay * attempt
            print(
                f"⚠ Rate limited by Trading 212 (429). "
                f"Retry {attempt}/{max_retries} in {delay:.1f}s…"
            )
            time.sleep(delay)
            continue

        # Any other error → fail fast
        resp.raise_for_status()

    raise RuntimeError(
        "Exceeded Trading 212 rate limits after multiple retries. "
        "Try again later."
    )


# ---------------------------------------------------------------------
# Derive ISA universe (heuristic, auditable)
# ---------------------------------------------------------------------

def derive_isa_universe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive an ISA-candidate universe from Trading 212 instruments.

    This is a heuristic filter:
    - Conservative
    - Auditable
    - Designed for opportunity discovery, not order validation
    """

    # Required base columns
    required = {"ticker", "type", "isin", "currencyCode"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # 1️⃣ Instrument type filter (primary)
    ALLOWED_TYPES = {
        "STOCK",
        "ETF",
        "INVESTMENT_TRUST",
        "REIT",
    }

    isa = df[df["type"].isin(ALLOWED_TYPES)].copy()

    # 2️⃣ Currency sanity check (optional, conservative)
    # ISA rules allow non-GBP assets, but this helps avoid noise early
    ALLOWED_CURRENCIES = {
        "GBP", "USD", "EUR"
    }

    isa = isa[isa["currencyCode"].isin(ALLOWED_CURRENCIES)]

    # 3️⃣ ISIN presence check (ISA instruments always have ISINs)
    isa = isa[isa["isin"].notna() & (isa["isin"].str.len() > 0)]

    # 4️⃣ Mark as derived, not authoritative
    isa["isa_eligible_derived"] = True

    return isa

# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run() -> None:
    print("▶ Fetching Trading 212 instruments")
    instruments = fetch_instruments()
    print("Columns:", instruments.columns.tolist())
    print(f"✓ Retrieved {len(instruments):,} instruments")

    print(f"▶ Writing raw snapshot → {RAW_OUT}")
    instruments.to_parquet(RAW_OUT, index=False)

    print("▶ Deriving ISA-permitted universe")
    isa = derive_isa_universe(instruments)

    print(f"✓ ISA-derived universe size: {len(isa):,}")

    print(f"▶ Writing ISA snapshot → {ISA_OUT}")
    isa.to_parquet(ISA_OUT, index=False)

    print("\n✓ Universe ingest completed successfully")


if __name__ == "__main__":
    run()
