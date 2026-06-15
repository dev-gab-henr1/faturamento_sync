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
CLICKUP_TEAM_ID = os.getenv("CLICKUP_TEAM_ID", "9013290037")

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
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Faturamento")

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
# Modo teste: força sync apenas em um único mês de referência (YYYYMM).
# Ajustado para acelerar validações locais.
POWERREV_REFERENCE_MONTH_ONLY = os.getenv("POWERREV_REFERENCE_MONTH_ONLY", "").strip()

# ── Sync timings ─────────────────────────────────────────
DELTA_SYNC_INTERVAL_S = 600     # 10 min
FULL_SYNC_DAILY_TIME = os.getenv("FULL_SYNC_DAILY_TIME", "00:10").strip()
FULL_SYNC_TIMEZONE = os.getenv("FULL_SYNC_TIMEZONE", "America/Sao_Paulo").strip()
FULL_SYNC_RETRY_BASE_S = int(os.getenv("FULL_SYNC_RETRY_BASE_S", "60"))
FULL_SYNC_RETRY_MAX_S = int(os.getenv("FULL_SYNC_RETRY_MAX_S", "900"))

# Distributed lock (multi-replica safety)
_DIST_LOCK_ENABLED_RAW = os.getenv("DISTRIBUTED_LOCK_ENABLED", "1").strip().lower()
DISTRIBUTED_LOCK_ENABLED = _DIST_LOCK_ENABLED_RAW in {"1", "true", "yes", "y", "on"}
DISTRIBUTED_LOCK_TTL_S = int(os.getenv("DISTRIBUTED_LOCK_TTL_S", "900"))         # 15 min
DISTRIBUTED_LOCK_REFRESH_S = int(os.getenv("DISTRIBUTED_LOCK_REFRESH_S", "120")) # 2 min
DISTRIBUTED_LOCK_TAB_NAME = os.getenv("DISTRIBUTED_LOCK_TAB_NAME", "__sync_lock").strip() or "__sync_lock"

# ── Sheets write tuning ─────────────────────────────────
CHUNK_SIZE = 300
CHUNK_PAUSE_S = 2
SHEETS_MAX_WRITE_REQUESTS_PER_MIN = int(os.getenv("SHEETS_MAX_WRITE_REQUESTS_PER_MIN", "45"))
SHEETS_MAX_RETRIES = int(os.getenv("SHEETS_MAX_RETRIES", "10"))
SHEETS_RETRY_BASE_S = float(os.getenv("SHEETS_RETRY_BASE_S", "2"))
SHEETS_RETRY_MAX_BACKOFF_S = float(os.getenv("SHEETS_RETRY_MAX_BACKOFF_S", "90"))
