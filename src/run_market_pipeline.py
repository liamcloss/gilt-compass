"""
Daily Market Pipeline – Gilt Compass

Prices + scoring only.
Run daily.
"""

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
        "src.ingest_prices_from_universe",
        "src.score_incremental",
        "src.thesis.trigger_thesis_on_score_change",
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
