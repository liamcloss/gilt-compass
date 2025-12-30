"""
Auditor Agent (v1 — realigned)

Purpose:
- Observe instruments already queued for attention
- Surface structural risks and data conditions
- Provide sceptical context WITHOUT decisions

Inputs:
- outputs/thesis_queue.csv
- outputs/scores_current.parquet

Outputs:
- outputs/audits/{instrument_id}_audit.md

Deterministic. Event-driven. Observational only.
"""

from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import textwrap


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent.parent

QUEUE = BASE_DIR / "outputs" / "thesis_queue.csv"
SCORES = BASE_DIR / "outputs" / "scores_current.parquet"

OUT_DIR = BASE_DIR / "outputs" / "audits"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Audit logic (OBSERVATIONS ONLY)
# ---------------------------------------------------------------------

def audit_row(row: pd.Series) -> str:
    instrument_id = row["instrument_id"]

    score = row["score"]
    delta = row["score_delta"]
    vol = row["vol_20d"]
    liq = row["avg_turnover_20d"]

    flags: list[str] = []

    # --- Structural observations (explicit & inspectable) ---

    if vol > 0.05:
        flags.append("Short-term volatility exceeds typical momentum regime")

    if liq < 500_000:
        flags.append("Liquidity below £500k/day — execution sensitivity")

    if delta > 1.25 and vol > 0.04:
        flags.append("Score acceleration coincides with elevated volatility")

    if delta > 1.25 and liq < 1_000_000:
        flags.append("Strong score movement on moderate liquidity")

    if not flags:
        flags.append("No immediate structural stress signals detected")

    flag_text = "\n".join(f"- {f}" for f in flags)

    md = f"""
    # Audit: {instrument_id}

    ## Context
    - **Current score:** {score:.2f}
    - **Score delta:** {delta:+.2f}
    - **20d volatility:** {vol:.2%}
    - **Avg £ turnover (20d):** £{liq:,.0f}

    ## Observations
    {flag_text}

    ## Auditor note
    These observations highlight **conditions that can reduce the
    durability of momentum signals**. They are **not a recommendation**
    and do not invalidate the trigger by themselves.

    The role of the Auditor is to surface **context and fragility** so
    downstream judgement can be better informed.
    """

    return textwrap.dedent(md).strip()


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

def run(max_per_run: int = 10) -> None:
    if not QUEUE.exists():
        print("✓ No thesis queue found — nothing to audit")
        return

    queue = pd.read_csv(QUEUE)

    if "status" not in queue.columns:
        print("✓ Queue has no lifecycle state — skipping audit")
        return

    pending = queue[queue["status"] == "queued"]

    if pending.empty:
        print("✓ No queued instruments to audit")
        return

    scores = pd.read_parquet(SCORES)

    merged = pending.merge(
        scores,
        on="instrument_id",
        how="left",
        suffixes=("", "_score"),
    )

    audited = 0

    for _, row in merged.head(max_per_run).iterrows():
        instrument_id = row["instrument_id"]
        print(f"▶ Auditing {instrument_id}")

        audit_md = audit_row(row)

        out = OUT_DIR / f"{instrument_id}_audit.md"
        out.write_text(audit_md, encoding="utf-8")

        audited += 1

    print(f"✓ Audited {audited} queued instruments → {OUT_DIR}")


if __name__ == "__main__":
    run()
