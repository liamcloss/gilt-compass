"""
Tag price source eligibility in Gilt Compass universe.

Adds:
- price_source (yahoo | none)
- price_eligible (bool)

Safe to re-run.
Does not remove instruments.
"""

from pathlib import Path
import pandas as pd


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent
UNIVERSE = BASE_DIR / "data" / "universe" / "gilt_universe.csv"


# ---------------------------------------------------------------------
# Yahoo eligibility logic (authoritative)
# ---------------------------------------------------------------------

def is_yahoo_eligible(symbol) -> bool:
    """
    Conservative filter for Yahoo-priceable equities / ETFs.
    Defensive against NaN / non-string values.
    """

    if not isinstance(symbol, str):
        return False

    symbol = symbol.strip()

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
# Run
# ---------------------------------------------------------------------

def run() -> None:
    if not UNIVERSE.exists():
        raise RuntimeError("gilt_universe.csv not found")

    df = pd.read_csv(UNIVERSE)

    if "symbol" not in df.columns:
        raise RuntimeError("Universe missing 'symbol' column")

    print(f"▶ Tagging price sources for {len(df):,} instruments")

    df["price_eligible"] = df["symbol"].apply(is_yahoo_eligible)
    df["price_source"] = df["price_eligible"].map(
        lambda x: "yahoo" if x else "none"
    )

    df.to_csv(UNIVERSE, index=False)

    print("✓ Universe price source tagging complete")
    print(df["price_source"].value_counts().to_string())


if __name__ == "__main__":
    run()
