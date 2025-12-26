"""
Patrician Agent (v1)

Role:
- Read ranked_v0.csv
- Read Thesis, Auditor, and Librarian artefacts (symmetrically)
- Arbitrate to a single conservative verdict per ticker

Verdicts:
- WATCH
- NEEDS_DEEPER_RESEARCH
- IGNORE

Decision-support only. Deterministic. Inspectable.
"""

from pathlib import Path
import pandas as pd
import textwrap

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent

RANKED = BASE_DIR / "outputs" / "ranked_v0.csv"
THESES_DIR = BASE_DIR / "outputs" / "theses"
AUDITS_DIR = BASE_DIR / "outputs" / "audits"
LIBRARY_DIR = BASE_DIR / "outputs" / "library"
OUT_DIR = BASE_DIR / "outputs" / "verdicts"

OUT_DIR.mkdir(exist_ok=True)


# --- Helpers ---
def read_optional(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


# --- Arbitration logic ---
def verdict_logic(
    row: pd.Series,
    audit_text: str,
    library_text: str
) -> tuple[str, str]:
    """
    Conservative, text-based arbitration.
    Patrician does not compute facts — it reacts to reported artefacts.
    """

    score = row["score"]
    ret_1y = row["ret_1y"]
    vol = row["vol_20d"]
    liq = row["avg_turnover_20d"]

    audit_l = audit_text.lower()
    library_l = library_text.lower()

    # --- Auditor red flags ---
    audit_red_flags = any(
        phrase in audit_l
        for phrase in [
            "very high",
            "thin liquidity",
            "volatility-driven",
        ]
    )

    # --- Librarian stability cues (facts-as-text) ---
    unstable_history = "range:" in library_l
    thin_history = any(
        f"observations: {n}" in library_l
        for n in ["1", "2", "3", "4", "5"]
    )

    # --- Arbitration (ordered, conservative) ---
    if audit_red_flags:
        return (
            "IGNORE",
            "Auditor flags structural risk that outweighs the momentum signal."
        )

    if thin_history:
        return (
            "IGNORE",
            "Librarian indicates insufficient price history for confidence."
        )

    if unstable_history:
        return (
            "NEEDS_DEEPER_RESEARCH",
            "Librarian suggests an unstable price regime despite positive momentum."
        )

    if score > 1.0 and ret_1y > 0.2 and liq > 1_000_000 and vol < 0.04:
        return (
            "WATCH",
            "Momentum is strong with acceptable liquidity and controlled volatility."
        )

    if score > 0.5:
        return (
            "NEEDS_DEEPER_RESEARCH",
            "Positive signal, but conviction insufficient without deeper context."
        )

    return (
        "IGNORE",
        "Composite signal too weak after risk adjustment."
    )


# --- Runner ---
def run(top_n: int = 10) -> None:
    df = pd.read_csv(RANKED)
    top = df.sort_values("score", ascending=False).head(top_n)

    for _, row in top.iterrows():
        ticker = row["ticker"]

        thesis_path = THESES_DIR / f"{ticker}.md"
        audit_path = AUDITS_DIR / f"{ticker}_audit.md"
        library_path = LIBRARY_DIR / f"{ticker}_library.md"

        thesis_text = read_optional(thesis_path)
        audit_text = read_optional(audit_path)
        library_text = read_optional(library_path)

        verdict, rationale = verdict_logic(
            row=row,
            audit_text=audit_text,
            library_text=library_text,
        )

        md = f"""
        # Verdict: {ticker}

        **Final classification:** `{verdict}`

        ## Rationale
        {rationale}

        ## Signal summary
        - **Score:** {row["score"]:.2f}
        - **1Y return:** {row["ret_1y"]:.1%}
        - **20d volatility:** {row["vol_20d"]:.2%}
        - **Avg £ turnover (20d):** £{row["avg_turnover_20d"]:,.0f}

        ## Thesis (excerpt)
        {thesis_text[:500] or "_No thesis available._"}

        ## Auditor (excerpt)
        {audit_text[:500] or "_No audit available._"}

        ## Librarian (excerpt)
        {library_text[:500] or "_No library entry available._"}

        ## Patrician note
        The Patrician prioritises **signal durability and capital preservation**
        over raw momentum. Absence of red flags is necessary but not sufficient
        for continued attention.
        """

        out = OUT_DIR / f"{ticker}_verdict.md"
        out.write_text(textwrap.dedent(md).strip(), encoding="utf-8")

    print(f"Generated {len(top)} verdicts in {OUT_DIR}/")


if __name__ == "__main__":
    run()
