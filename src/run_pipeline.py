"""
Run Pipeline

Single entry point for The Gilt Compass.
Runs the full deterministic pipeline and emits a run manifest.
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
        "src.ingest_prices",
        "src.score",
        "src.agents.librarian",
        "src.agents.thesis",
        "src.agents.auditor",
        "src.agents.patrician",
    ]

    try:
        for step in steps:
            run(step)

        manifest = write_manifest(steps=steps, status="SUCCESS")
        print(f"\n✓ Pipeline completed successfully")
        print(f"✓ Manifest written: {manifest}")

    except Exception as e:
        manifest = write_manifest(steps=steps, status="FAILED")
        print(f"\n✗ Pipeline failed")
        print(f"✗ Manifest written: {manifest}")
        raise


if __name__ == "__main__":
    main()
