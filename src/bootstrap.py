from pathlib import Path
from dotenv import load_dotenv
import os

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"

load_dotenv(ENV_FILE)

def require(*names: str) -> None:
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
