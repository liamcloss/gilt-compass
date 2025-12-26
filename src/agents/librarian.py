"""
Librarian Agent (v0)

Role:
- Build a factual reference sheet per ticker
- No judgement, no scoring, no recommendations
- Inputs are limited to existing pipeline data

Outputs are used by Patrician for context.
"""

from pathlib import Path
import pandas as pd
import textwrap

BASE_DIR = Path(__file__).resolve().parent.parent.parent

PRICES = BASE_DIR / "data" / "raw" / "prices_1y.parquet"
RANKED = BASE_DIR / "outputs" / "ranked_v0.csv"
OUT = BASE_DIR / "outputs" / "library"

OUT.mkdir(exist_ok=True)


def build_entry(ticker: str, prices: pd.DataFrame, row: pd.Series) -> str:
    px = prices[prices["ticker"] == ticker].sort_values("date")

    start_date = px["date"].iloc[0]
    end_date = px["date"].iloc[-1]

    hi = px["close"].max()
    lo = px["close"].min()
    last = px["close"].iloc[-1]

    md = f"""
    # Library entry: {ticker}

    ## Identification
    - **Ticker:** {ticker}
    - **Data window:** {start_date.date()} → {end_date.date()}
    - **Observations:** {len(px)}

    ## Price facts (1Y)
    - **Last close:** {last:.2f}
    - **52w high:** {hi:.2f}
    - **52w low:** {lo:.2f}
    - **Range:** {(hi / lo - 1):.1%}

    ## Derived metrics (from scorer)
    - **1Y return:** {row["ret_1y"]:.1%}
    - **20d volatility:** {row["vol_20d"]:.2%}
    - **Avg £ turnover (20d):** £{row["avg_turnover_20d"]:,.0f}

    ## Notes
    This entry contains **descriptive facts only**.
    No forward-looking statements are made.
    """

    return textwrap.dedent(md).strip()


def run(top_n: int = 10) -> None:
    prices = pd.read_parquet(PRICES)
    ranked = pd.read_csv(RANKED)

    top = ranked.sort_values("score", ascending=False).head(top_n)

    for _, row in top.iterrows():
        ticker = row["ticker"]
        entry = build_entry(ticker, prices, row)

        out = OUT / f"{ticker}_library.md"
        out.write_text(entry, encoding="utf-8")

    print(f"Generated {len(top)} library entries in {OUT}/")


if __name__ == "__main__":
    run()
