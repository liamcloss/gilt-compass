from pathlib import Path
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
TEXT_FILE = BASE_DIR / "outputs" / "daily_attention.txt"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


text = TEXT_FILE.read_text(encoding="utf-8")

requests.post(
    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
    json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
)
