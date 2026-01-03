"""
Run Market Pipeline – Gilt Compass

Prices, scoring, attention governance, and artefact generation.
Run daily.
"""

import src.bootstrap  # noqa: F401
import subprocess
import sys
from pathlib import Path

from src.manifest import write_manifest

BASE_DIR = Path(__file__).resolve().parent.parent


def run(module: str) -> None:
    print(f"\n▶ Running {module}")
    subprocess.check_call(
        [sys.executable, "-m", module],
        cwd=BASE_DIR,
    )


def main() -> None:
    steps = [
        # --- Market data & signals ---
        "src.ingest_prices_from_universe",
        "src.score_incremental",
        "src.thesis.trigger_thesis_on_score_change",

        # --- Attention governance ---
        "src.agents.patrician",

        # --- Artefact generation (only for active attention) ---
        "src.agents.librarian",
        "src.agents.auditor",
        "src.agents.thesis",
    ]

    try:
        for step in steps:
            run(step)

        manifest = write_manifest(steps=steps, status="SUCCESS")
        print("\n✓ Market pipeline completed")
        print(f"✓ Manifest written: {manifest}")

    except Exception:
        manifest = write_manifest(steps=steps, status="FAILED")
        print("\n✗ Market pipeline failed")
        print(f"✗ Manifest written: {manifest}")
        raise


if __name__ == "__main__":
    main()
