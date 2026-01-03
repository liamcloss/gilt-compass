"""
Weekly Universe Pipeline – Gilt Compass

Discovers and manages instrument lifecycle.
Run weekly.
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
        "src.ingest_universe_trading212",
        "src.universe.diff_trading212_universe",
        "src.universe.enqueue_new_trading212",
        "src.universe.mark_inactive_trading212",
        "src.universe.tag_price_source",  # safety net
    ]

    try:
        for step in steps:
            run(step)

        manifest = write_manifest(steps=steps, status="SUCCESS")
        print("\n✓ Universe pipeline completed")
        print(f"✓ Manifest written: {manifest}")

    except Exception:
        manifest = write_manifest(steps=steps, status="FAILED")
        print("\n✗ Universe pipeline failed")
        print(f"✗ Manifest written: {manifest}")
        raise


if __name__ == "__main__":
    main()
