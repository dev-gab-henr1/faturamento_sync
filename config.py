"""
Configurações centrais do projeto Faturamento Sync.
Variáveis de ambiente + constantes.
"""
import os
import json
from dotenv import load_dotenv

load_dotenv()

# ── ClickUp ──────────────────────────────────────────────
CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN", "")
CLICKUP_BASE_URL = "https://api.clickup.com/api/v2"

CLICKUP_LIST_IDS = [
    "901322296001",
    "901321549851",
    "901324691177",
]

# ── Google Sheets ────────────────────────────────────────
SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1ea2_iw2_GCK1_p2qrIxL-y_EdCY78BreFi_HzuPkjsg",
)
SHEET_TAB_NAME = "Faturamento"

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

def get_google_credentials_info() -> dict | None:
    if GOOGLE_CREDENTIALS_JSON:
        return json.loads(GOOGLE_CREDENTIALS_JSON)
    if os.path.exists(GOOGLE_CREDENTIALS_FILE):
        with open(GOOGLE_CREDENTIALS_FILE) as f:
            return json.load(f)
    return None

# ── PowerRev ─────────────────────────────────────────────
POWERREV_BASE_URL = os.getenv("POWERREV_BASE_URL", "")
POWERREV_AUTH_URL = os.getenv("POWERREV_AUTH_URL", "")
POWERREV_ACCOUNT_ID = os.getenv("POWERREV_ACCOUNT_ID", "")
POWERREV_API_KEY = os.getenv("POWERREV_API_KEY", "")
POWERREV_TIMEOUT = int(os.getenv("POWERREV_TIMEOUT", "30"))
POWERREV_DELAY = float(os.getenv("POWERREV_DELAY", "1.0"))
POWERREV_MAX_RETRIES = int(os.getenv("POWERREV_MAX_RETRIES", "3"))
POWERREV_PAGE_LIMIT = int(os.getenv("POWERREV_PAGE_LIMIT", "100"))

# ── Sync timings ─────────────────────────────────────────
FULL_SYNC_INTERVAL_S = 7200     # 2 h
DELTA_SYNC_INTERVAL_S = 600     # 10 min

# ── Sheets write tuning ─────────────────────────────────
CHUNK_SIZE = 300
CHUNK_PAUSE_S = 2