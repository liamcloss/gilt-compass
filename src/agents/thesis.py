"""
Thesis Agent (v0)

Reads ranked_v0.csv and emits a short, sceptical markdown thesis per ticker.
Decision-support only. No recommendations. No APIs. Fully deterministic.
"""

from pathlib import Path
import pandas as pd
import textwrap

IN = Path("outputs/ranked_v0.csv")
OUT = Path("outputs/theses")
OUT.mkdir(exist_ok=True)


def build_thesis(row: pd.Series) -> str:
    """
    Produce a compact markdown thesis for a single ticker.
    """

    ticker = row["ticker"]
    score = row["score"]
    ret_1y = row["ret_1y"]
    vol = row["vol_20d"]
    liq = row["avg_turnover_20d"]

    momentum_read = (
        "strong" if ret_1y > 0.3 else
        "moderate" if ret_1y > 0.1 else
        "weak"
    )

    vol_read = (
        "elevated" if vol > 0.04 else
        "normal" if vol > 0.02 else
        "low"
    )

    liq_read = (
        "adequate" if liq > 1_000_000 else
        "thin"
    )

    sceptic = (
        "Is this genuine momentum, or a late-cycle chase?"
        if momentum_read == "strong"
        else
        "Is the upside sufficient to justify attention?"
    )

    md = f"""
    # {ticker}

    **Composite score:** {score:.2f}

    ## Snapshot
    - **1Y return:** {ret_1y:.1%} ({momentum_read} momentum)
    - **20d volatility:** {vol:.2%} ({vol_read})
    - **Avg £ turnover (20d):** £{liq:,.0f} ({liq_read})

    ## Interpretation
    {ticker} screens as a **{momentum_read} momentum** name with
    **{vol_read} volatility** and **{liq_read} liquidity**.

    The score is driven primarily by trailing returns, with liquidity acting
    as a secondary filter and volatility applied as a risk penalty.

    ## Sceptical question
    > {sceptic}

    ## What would change the view?
    - Momentum rolling over on a 3–6 month basis
    - Liquidity deteriorating below recent norms
    - Volatility expanding without return acceleration
    """

    return textwrap.dedent(md).strip()


def run(top_n: int = 10) -> None:
    df = pd.read_csv(IN)

    top = df.sort_values("score", ascending=False).head(top_n)

    for _, row in top.iterrows():
        thesis = build_thesis(row)
        out = OUT / f"{row['ticker']}.md"
        out.write_text(thesis, encoding="utf-8")

    print(f"Generated {len(top)} theses in {OUT}/")


if __name__ == "__main__":
    run()
