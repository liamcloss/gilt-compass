"""
Auditor Agent (v0)

Purpose:
- Read ranked_v0.csv
- Identify risk flags that momentum scoring can hide
- Emit a short, sceptical markdown audit per ticker

No recommendations. No APIs. Fully deterministic.
"""

from pathlib import Path
import pandas as pd
import textwrap

IN = Path("outputs/ranked_v0.csv")
OUT = Path("outputs/audits")
OUT.mkdir(exist_ok=True)


def audit_row(row: pd.Series) -> str:
    ticker = row["ticker"]
    score = row["score"]
    ret_1y = row["ret_1y"]
    vol = row["vol_20d"]
    liq = row["avg_turnover_20d"]

    flags = []

    # --- Risk rules (explicit and inspectable) ---

    if vol > 0.05:
        flags.append("Very high short-term volatility")

    if liq < 500_000:
        flags.append("Thin liquidity — execution risk")

    if ret_1y > 0.5 and liq < 1_000_000:
        flags.append("Strong momentum combined with weak liquidity")

    if ret_1y > 0.6 and vol > 0.04:
        flags.append("Momentum appears volatility-driven")

    if not flags:
        flags.append("No major structural red flags detected")

    flag_text = "\n".join(f"- {f}" for f in flags)

    judgement = (
        "High-risk momentum profile — deserves scepticism"
        if any("Very high" in f or "Thin liquidity" in f for f in flags)
        else "Acceptable risk, but still momentum-dependent"
    )

    md = f"""
    # Audit: {ticker}

    **Composite score:** {score:.2f}

    ## Key metrics
    - **1Y return:** {ret_1y:.1%}
    - **20d volatility:** {vol:.2%}
    - **Avg £ turnover (20d):** £{liq:,.0f}

    ## Risk flags
    {flag_text}

    ## Auditor judgement
    {judgement}

    ## Sceptical stance
    Momentum signals can decay quickly. This name should only
    be trusted while both **liquidity and volatility remain stable**.
    """

    return textwrap.dedent(md).strip()


def run(top_n: int = 10) -> None:
    df = pd.read_csv(IN)
    top = df.sort_values("score", ascending=False).head(top_n)

    for _, row in top.iterrows():
        audit = audit_row(row)
        out = OUT / f"{row['ticker']}_audit.md"
        out.write_text(audit, encoding="utf-8")

    print(f"Generated {len(top)} audits in {OUT}/")


if __name__ == "__main__":
    run()
