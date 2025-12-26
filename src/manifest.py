"""
Run Manifest

Creates a lightweight, deterministic manifest for each pipeline run.
No agent logic. No side effects beyond writing JSON.
"""

from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import uuid
import subprocess



BASE_DIR = Path(__file__).resolve().parent.parent
MANIFEST_DIR = BASE_DIR / "outputs" / "manifests"
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

def git_commit_hash() -> str:
    """
    Return current git commit hash if available, else 'unknown'.
    """
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=BASE_DIR,
                stderr=subprocess.DEVNULL,
            )
            .decode("utf-8")
            .strip()
        )
    except Exception:
        return "unknown"


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def collect_artefacts() -> list[dict]:
    artefacts = []
    outputs_dir = BASE_DIR / "outputs"

    for path in outputs_dir.rglob("*"):
        if path.is_file() and path.suffix in {".csv", ".md", ".parquet"}:
            artefacts.append({
                "path": str(path.relative_to(BASE_DIR)),
                "sha256": file_hash(path),
            })

    return sorted(artefacts, key=lambda x: x["path"])


def write_manifest(steps: list[str], status: str) -> Path:
    manifest = {
        "run_id": str(uuid.uuid4()),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "git_commit": git_commit_hash(),
        "steps": steps,
        "artefacts": collect_artefacts(),
    }

    out = MANIFEST_DIR / f"run_{manifest['run_id']}.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out
