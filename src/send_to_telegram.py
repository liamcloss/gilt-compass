from pathlib import Path
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
TEXT_FILE = BASE_DIR / "outputs" / "daily_attention.txt"

BOT_TOKEN = "8429986921:AAHf5gRyIDZ5bDQGOFRzTDkOuSIWzE66Ot4"
CHAT_ID = "YOUR_CHAT_ID"
8429986921:AAHf5gRyIDZ5bDQGOFRzTDkOuSIWzE66Ot4
text = TEXT_FILE.read_text(encoding="utf-8")

requests.post(
    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
    json={
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
)
