"""
Gerenciador do Google Sheets.
Escreve com RAW, retry com backoff, chunking de 300 linhas.
Protege colunas editáveis (Q, R) e não limpa formatações.
"""
import time
import logging

import gspread
from google.oauth2.service_account import Credentials

from config import (
    SPREADSHEET_ID,
    SHEET_TAB_NAME,
    CHUNK_SIZE,
    CHUNK_PAUSE_S,
    get_google_credentials_info,
)
from field_map import get_headers, COLUMN_ORDER

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_CLIENT: gspread.Client | None = None

# Colunas protegidas (editáveis manualmente na planilha)
# São as últimas 2: "validacao" e "observacoes"
_PROTECTED_KEYS = {"validacao", "observacoes"}
WRITE_COL_COUNT = len([k for k in COLUMN_ORDER if k not in _PROTECTED_KEYS])


def _get_client() -> gspread.Client:
    global _CLIENT
    if _CLIENT is None:
        creds_info = get_google_credentials_info()
        if creds_info is None:
            raise RuntimeError(
                "Google credentials não configuradas. "
                "Defina GOOGLE_CREDENTIALS_JSON ou GOOGLE_CREDENTIALS_FILE."
            )
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        _CLIENT = gspread.authorize(creds)
    return _CLIENT


def _retry(fn, *args, max_retries: int = 4, **kwargs):
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            code = exc.response.status_code if hasattr(exc, "response") else 0
            if code in (500, 502, 503) and attempt < max_retries - 1:
                wait = 2 ** attempt
                logger.warning("Sheets API %s, retry in %ds", code, wait)
                time.sleep(wait)
                continue
            raise


def _write_col_letter() -> str:
    """Retorna a letra da última coluna de escrita (ex: 'P' para 16 colunas)."""
    return gspread.utils.rowcol_to_a1(1, WRITE_COL_COUNT).replace("1", "")


def get_worksheet() -> gspread.Worksheet:
    client = _get_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(SHEET_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=SHEET_TAB_NAME, rows=1000, cols=20,
        )
        logger.info("Aba '%s' criada.", SHEET_TAB_NAME)
    from stats import stats
    stats.sheets_read_requests += 1
    return ws


def ensure_headers(ws: gspread.Worksheet) -> None:
    """Garante que a primeira linha tem os headers corretos (todas as colunas)."""
    from stats import stats
    headers = get_headers()
    existing = _retry(ws.row_values, 1)
    stats.sheets_read_requests += 1
    if existing != headers:
        _retry(
            ws.update,
            range_name="A1",
            values=[headers],
            value_input_option="RAW",
        )
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += len(headers)
        logger.info("Headers atualizados.")


def read_all_rows(ws: gspread.Worksheet) -> list[list[str]]:
    from stats import stats
    all_data = _retry(
        ws.get_all_values,
        value_render_option="UNFORMATTED_VALUE",
    )
    stats.sheets_read_requests += 1
    if len(all_data) <= 1:
        return []
    return all_data[1:]


def write_all_rows(ws: gspread.Worksheet, rows: list[list[str]]) -> None:
    """
    Reescreve colunas A até P (protege Q e R).
    Salva Q e R indexados por (UC, Mês Referência) antes de limpar,
    e restaura na posição correta depois.
    """
    from stats import stats
    headers = get_headers()
    end_col = _write_col_letter()

    uc_col = headers.index("UC")
    mes_col = headers.index("Mês de Referencia")

    # ── 1) Salvar Q e R existentes, indexados por (UC, Mês Ref) ──
    protected_start = WRITE_COL_COUNT  # coluna Q (0-based)
    saved: dict[str, list[str]] = {}

    existing = _retry(ws.get_all_values, value_render_option="UNFORMATTED_VALUE")
    stats.sheets_read_requests += 1

    if len(existing) > 1:
        for row in existing[1:]:
            if len(row) > max(uc_col, mes_col):
                uc = str(row[uc_col]).strip()
                mes = str(row[mes_col]).strip()
                if uc and mes:
                    key = f"{uc}|{mes}"
                    protected_vals = row[protected_start:] if len(row) > protected_start else []
                    # Só salvar se tem conteúdo real
                    if any(str(v).strip() for v in protected_vals):
                        saved[key] = [str(v) for v in protected_vals]

    if saved:
        logger.info("Colunas protegidas: %d linhas salvas.", len(saved))

    # ── 2) Limpar A-P abaixo do header ───────────────────
    current_rows = ws.row_count
    if current_rows > 1:
        clear_range = f"A2:{end_col}{current_rows}"
        _retry(ws.batch_clear, [clear_range])
        stats.sheets_write_requests += 1

    if not rows:
        return

    # ── 3) Expandir planilha se necessário ────────────────
    needed = len(rows) + 1
    if ws.row_count < needed:
        _retry(ws.resize, rows=needed)
        stats.sheets_write_requests += 1

    # ── 4) Escrever A-P em chunks ────────────────────────
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = [row[:WRITE_COL_COUNT] for row in rows[i : i + CHUNK_SIZE]]
        start_row = i + 2
        range_start = f"A{start_row}"
        _retry(
            ws.update,
            range_name=range_start,
            values=chunk,
            value_input_option="RAW",
        )
        cells = sum(len(r) for r in chunk)
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += cells
        logger.info(
            "Chunk escrito: linhas %d–%d (%d rows, %d cells)",
            start_row, start_row + len(chunk) - 1, len(chunk), cells,
        )
        if i + CHUNK_SIZE < len(rows):
            time.sleep(CHUNK_PAUSE_S)

    # ── 5) Restaurar Q e R nas posições corretas ─────────
    if not saved:
        return

    protected_updates: list[dict] = []
    restored = 0

    for i, row in enumerate(rows):
        if len(row) > max(uc_col, mes_col):
            uc = str(row[uc_col]).strip()
            mes = str(row[mes_col]).strip()
            key = f"{uc}|{mes}"
            vals = saved.get(key)
            if vals:
                sheet_row = i + 2
                # Coluna Q = WRITE_COL_COUNT + 1 (1-based)
                start_cell = gspread.utils.rowcol_to_a1(sheet_row, protected_start + 1)
                end_cell = gspread.utils.rowcol_to_a1(sheet_row, protected_start + len(vals))
                protected_updates.append({
                    "range": f"{start_cell}:{end_cell}",
                    "values": [vals],
                })
                restored += 1

    if protected_updates:
        for i in range(0, len(protected_updates), CHUNK_SIZE):
            chunk = protected_updates[i : i + CHUNK_SIZE]
            _retry(
                ws.batch_update,
                chunk,
                value_input_option="RAW",
            )
            stats.sheets_write_requests += 1
            stats.sheets_cells_written += sum(len(u["values"][0]) for u in chunk)
            if i + CHUNK_SIZE < len(protected_updates):
                time.sleep(CHUNK_PAUSE_S)

        logger.info("Colunas protegidas: %d linhas restauradas.", restored)

    orphaned = len(saved) - restored
    if orphaned:
        logger.warning(
            "Colunas protegidas: %d linhas não encontraram correspondência (dados órfãos).",
            orphaned,
        )


def append_rows(ws: gspread.Worksheet, rows: list[list[str]]) -> None:
    """Adiciona linhas no final, apenas colunas A-P."""
    from stats import stats
    if not rows:
        return

    all_data = _retry(ws.get_all_values)
    stats.sheets_read_requests += 1
    next_row = len(all_data) + 1

    needed = next_row + len(rows) - 1
    if ws.row_count < needed:
        _retry(ws.resize, rows=needed)
        stats.sheets_write_requests += 1

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = [row[:WRITE_COL_COUNT] for row in rows[i : i + CHUNK_SIZE]]
        start_row = next_row + i
        range_start = f"A{start_row}"
        _retry(
            ws.update,
            range_name=range_start,
            values=chunk,
            value_input_option="RAW",
        )
        cells = sum(len(r) for r in chunk)
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += cells
        logger.info(
            "Append chunk: linhas %d–%d (%d rows, %d cells)",
            start_row, start_row + len(chunk) - 1, len(chunk), cells,
        )
        if i + CHUNK_SIZE < len(rows):
            time.sleep(CHUNK_PAUSE_S)


def update_rows_in_place(
    ws: gspread.Worksheet,
    updates: dict[int, list[str]],
) -> None:
    """Atualiza linhas específicas, apenas colunas A-P."""
    from stats import stats
    if not updates:
        return

    end_col = _write_col_letter()
    batch: list[dict] = []
    for sheet_row, row_data in updates.items():
        truncated = row_data[:WRITE_COL_COUNT]
        range_name = f"A{sheet_row}:{end_col}{sheet_row}"
        batch.append({
            "range": range_name,
            "values": [truncated],
        })

    for i in range(0, len(batch), CHUNK_SIZE):
        chunk = batch[i : i + CHUNK_SIZE]
        _retry(
            ws.batch_update,
            chunk,
            value_input_option="RAW",
        )
        cells = sum(len(item["values"][0]) for item in chunk)
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += cells
        if i + CHUNK_SIZE < len(batch):
            time.sleep(CHUNK_PAUSE_S)


def update_columns_in_place(
    ws: gspread.Worksheet,
    updates: dict[int, dict[int, str]],
) -> None:
    """Atualiza células específicas (PowerRev: colunas N, O, P)."""
    from stats import stats
    if not updates:
        return

    batch: list[dict] = []
    total_cells = 0
    for sheet_row, col_map in updates.items():
        for col_idx, value in col_map.items():
            cell = gspread.utils.rowcol_to_a1(sheet_row, col_idx + 1)
            batch.append({
                "range": cell,
                "values": [[value]],
            })
            total_cells += 1

    for i in range(0, len(batch), CHUNK_SIZE):
        chunk = batch[i : i + CHUNK_SIZE]
        _retry(
            ws.batch_update,
            chunk,
            value_input_option="RAW",
        )
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += len(chunk)
        if i + CHUNK_SIZE < len(batch):
            time.sleep(CHUNK_PAUSE_S)

    logger.info("Columns update: %d células em %d linhas", total_cells, len(updates))