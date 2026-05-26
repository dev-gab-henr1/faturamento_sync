"""
Gerenciador do Google Sheets.
Escreve com RAW, retry com backoff, chunking de 300 linhas.
Protege colunas editaveis e nao limpa formatacoes.
"""
import time
import logging
import re
from collections import deque

import gspread
from google.oauth2.service_account import Credentials

from config import (
    SPREADSHEET_ID,
    SHEET_TAB_NAME,
    CHUNK_SIZE,
    CHUNK_PAUSE_S,
    SHEETS_MAX_WRITE_REQUESTS_PER_MIN,
    SHEETS_MAX_RETRIES,
    SHEETS_RETRY_BASE_S,
    SHEETS_RETRY_MAX_BACKOFF_S,
    get_google_credentials_info,
)
from field_map import get_headers, COLUMN_ORDER

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_CLIENT: gspread.Client | None = None
_CREDS_CREATED_AT: float = 0.0
_WRITE_TIMESTAMPS: deque[float] = deque()
_CURRENCY_FORMAT_APPLIED: bool = False

# Colunas protegidas (editaveis manualmente na planilha).
# A restauracao usa chave primaria UC|Mes|InvoiceID e fallback UC|Mes.
_PROTECTED_KEYS = {"observacoes", "valor_final", "data_emissao_final"}
_PROTECTED_COL_INDEXES = [i for i, key in enumerate(COLUMN_ORDER) if key in _PROTECTED_KEYS]
_WRITABLE_COL_INDEXES = [i for i, key in enumerate(COLUMN_ORDER) if key not in _PROTECTED_KEYS]
_INVOICE_ID_HEADER = "Invoice ID"
_CURRENCY_COL_INDEX = COLUMN_ORDER.index("valor_boleto") if "valor_boleto" in COLUMN_ORDER else None
_CURRENCY_COL_FORMAT = {
    "numberFormat": {
        "type": "CURRENCY",
        "pattern": "[$R$-416] #,##0.00",
    },
}

# Mantido para compatibilidade com poll.py (_merge_with_disappeared).
WRITE_COL_COUNT = len(COLUMN_ORDER)

# Refresh credentials a cada 45 min (expiram em ~60 min)
_CREDS_REFRESH_INTERVAL = 45 * 60
_TRANSIENT_SHEETS_CODES = {429, 500, 502, 503, 504}


def _get_client() -> gspread.Client:
    global _CLIENT, _CREDS_CREATED_AT
    now = time.time()
    if _CLIENT is not None and (now - _CREDS_CREATED_AT) >= _CREDS_REFRESH_INTERVAL:
        logger.info("Refresh proativo de credenciais Google (%d min desde criacao).",
                    int((now - _CREDS_CREATED_AT) / 60))
        _CLIENT = None
    if _CLIENT is None:
        creds_info = get_google_credentials_info()
        if creds_info is None:
            raise RuntimeError(
                "Google credentials nao configuradas. "
                "Defina GOOGLE_CREDENTIALS_JSON ou GOOGLE_CREDENTIALS_FILE."
            )
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        _CLIENT = gspread.authorize(creds)
        _CREDS_CREATED_AT = now
    return _CLIENT


def reset_client() -> None:
    """Forca re-criacao do client na proxima chamada."""
    global _CLIENT, _CREDS_CREATED_AT, _CURRENCY_FORMAT_APPLIED
    _CLIENT = None
    _CREDS_CREATED_AT = 0.0
    _CURRENCY_FORMAT_APPLIED = False
    _WRITE_TIMESTAMPS.clear()
    logger.info("Google Sheets client resetado.")


def _throttle_write_requests() -> None:
    """Limita escrita para evitar 429 de quota por minuto."""
    if SHEETS_MAX_WRITE_REQUESTS_PER_MIN <= 0:
        return

    now = time.monotonic()
    cutoff = now - 60.0
    while _WRITE_TIMESTAMPS and _WRITE_TIMESTAMPS[0] <= cutoff:
        _WRITE_TIMESTAMPS.popleft()

    if len(_WRITE_TIMESTAMPS) >= SHEETS_MAX_WRITE_REQUESTS_PER_MIN:
        wait_s = (_WRITE_TIMESTAMPS[0] + 60.0) - now + 0.15
        if wait_s > 0:
            logger.warning(
                "Sheets write throttle: aguardando %.1fs para respeitar limite (%d req/min).",
                wait_s,
                SHEETS_MAX_WRITE_REQUESTS_PER_MIN,
            )
            time.sleep(wait_s)

        now = time.monotonic()
        cutoff = now - 60.0
        while _WRITE_TIMESTAMPS and _WRITE_TIMESTAMPS[0] <= cutoff:
            _WRITE_TIMESTAMPS.popleft()

    _WRITE_TIMESTAMPS.append(time.monotonic())


def _retry_after_from_response(resp) -> float | None:
    if resp is None:
        return None
    header = resp.headers.get("Retry-After")
    if not header:
        return None
    try:
        return max(float(header), 0.0)
    except ValueError:
        return None


def _compute_retry_wait(code: int, attempt: int, response=None) -> float:
    if code == 429:
        retry_after = _retry_after_from_response(response)
        if retry_after is not None:
            return min(max(retry_after, 1.0), SHEETS_RETRY_MAX_BACKOFF_S)
    expo = SHEETS_RETRY_BASE_S * (2 ** max(attempt - 1, 0))
    return min(max(expo, 1.0), SHEETS_RETRY_MAX_BACKOFF_S)


def _retry(fn, *args, max_retries: int | None = None, is_write: bool = False, **kwargs):
    attempts = max_retries if max_retries is not None else SHEETS_MAX_RETRIES
    for attempt in range(1, attempts + 1):
        try:
            if is_write:
                _throttle_write_requests()
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            code = exc.response.status_code if hasattr(exc, "response") else 0
            if code in _TRANSIENT_SHEETS_CODES and attempt < attempts:
                wait = _compute_retry_wait(code, attempt, getattr(exc, "response", None))
                logger.warning(
                    "Sheets API %s, tentativa %d/%d falhou. Retry em %.1fs.",
                    code,
                    attempt,
                    attempts,
                    wait,
                )
                time.sleep(wait)
                continue
            raise
        except Exception as exc:
            module = exc.__class__.__module__
            is_network = module.startswith("requests") or module.startswith("urllib3") or isinstance(exc, TimeoutError)
            if is_network and attempt < attempts:
                wait = _compute_retry_wait(503, attempt, None)
                logger.warning(
                    "Sheets erro de rede (%s), tentativa %d/%d. Retry em %.1fs.",
                    exc.__class__.__name__,
                    attempt,
                    attempts,
                    wait,
                )
                time.sleep(wait)
                continue
            raise


def _build_segments(col_indexes: list[int]) -> list[tuple[int, int]]:
    if not col_indexes:
        return []
    segments: list[tuple[int, int]] = []
    start = prev = col_indexes[0]
    for idx in col_indexes[1:]:
        if idx == prev + 1:
            prev = idx
            continue
        segments.append((start, prev))
        start = prev = idx
    segments.append((start, prev))
    return segments


_WRITABLE_SEGMENTS = _build_segments(_WRITABLE_COL_INDEXES)
_ALL_SEGMENTS = _build_segments(list(range(len(COLUMN_ORDER))))


def _col_letter(col_idx: int) -> str:
    return gspread.utils.rowcol_to_a1(1, col_idx + 1).replace("1", "")


def _ensure_currency_column_format(ws: gspread.Worksheet) -> None:
    """Garante formato de moeda para a coluna de valor do boleto (M)."""
    global _CURRENCY_FORMAT_APPLIED
    if _CURRENCY_FORMAT_APPLIED or _CURRENCY_COL_INDEX is None:
        return

    from stats import stats

    col = _col_letter(_CURRENCY_COL_INDEX)
    _retry(
        ws.format,
        f"{col}2:{col}",
        _CURRENCY_COL_FORMAT,
        is_write=True,
    )
    stats.sheets_write_requests += 1
    _CURRENCY_FORMAT_APPLIED = True


def _slice_segment(row: list[str], start_idx: int, end_idx: int) -> list[str]:
    width = end_idx - start_idx + 1
    if len(row) > end_idx:
        return row[start_idx:end_idx + 1]
    if len(row) <= start_idx:
        return [""] * width
    out = row[start_idx:]
    if len(out) < width:
        out = out + ([""] * (width - len(out)))
    return out


def _row_value(row: list[str], col_idx: int | None) -> str:
    if col_idx is None or col_idx < 0 or len(row) <= col_idx:
        return ""
    return str(row[col_idx]).strip()


def _safe_token(value: str) -> str:
    return str(value or "").strip().replace("|", "/")


def _synthetic_invoice_id_from_row(
    *,
    uc: str,
    issue_date: str,
    provider_name: str,
) -> str:
    if not any([issue_date, provider_name]):
        return ""
    parts = [
        "SYN",
        "R",
        _safe_token(uc),
        _safe_token(issue_date),
        _safe_token(provider_name),
    ]
    return "|".join(parts)


def _derive_invoice_id_from_row(
    row: list[str],
    *,
    uc_col: int,
    invoice_id_col: int | None,
    provider_col: int | None,
    issue_col: int | None,
    parent_col: int | None,
) -> str:
    explicit = _row_value(row, invoice_id_col)
    if explicit:
        return explicit
    # parent_col mantido na assinatura para compatibilidade de chamadas legadas.
    _ = parent_col
    return _synthetic_invoice_id_from_row(
        uc=_row_value(row, uc_col),
        issue_date=_row_value(row, issue_col),
        provider_name=_row_value(row, provider_col),
    )


def _fallback_key(uc: str, mes: str) -> str:
    if not uc or not mes:
        return ""
    return f"{uc}|{mes}"


def _primary_key(uc: str, mes: str, invoice_id: str) -> str:
    if not uc or not mes or not invoice_id:
        return ""
    return f"{uc}|{mes}|{invoice_id}"


def _normalize_money_token(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    cleaned = re.sub(r"[^0-9,.\-]", "", text)
    if not cleaned:
        return ""
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return f"{float(cleaned):.2f}"
    except (TypeError, ValueError):
        return _safe_token(text)


def _secondary_key(
    *,
    uc: str,
    mes: str,
    issue_date: str,
    provider_name: str,
    status: str,
    total: str,
) -> str:
    if not uc or not mes:
        return ""
    parts = [
        "SK",
        _safe_token(uc),
        _safe_token(mes),
        _safe_token(issue_date),
        _safe_token(provider_name),
        _safe_token(status),
        _normalize_money_token(total),
    ]
    return "|".join(parts)


def _secondary_key_from_row(
    row: list[str],
    *,
    uc_col: int,
    mes_col: int,
    issue_col: int | None,
    provider_col: int | None,
    status_col: int | None,
    total_col: int | None,
) -> str:
    return _secondary_key(
        uc=_row_value(row, uc_col),
        mes=_row_value(row, mes_col),
        issue_date=_row_value(row, issue_col),
        provider_name=_row_value(row, provider_col),
        status=_row_value(row, status_col),
        total=_row_value(row, total_col),
    )


def _merge_saved_values(current: list[str] | None, candidate: list[str]) -> list[str]:
    """Mantem valores ja salvos; preenche apenas buracos vazios com candidate."""
    if current is None:
        return candidate
    out = current[:]
    for idx, val in enumerate(candidate):
        if idx >= len(out):
            out.append(val)
            continue
        if not str(out[idx]).strip() and str(val).strip():
            out[idx] = val
    return out


def _build_protected_snapshot_from_rows(data_rows: list[list[str]]) -> dict[str, object]:
    """Captura snapshot dos valores protegidos (L/N/P) por chaves primaria/fallback."""
    uc_col = COLUMN_ORDER.index("uc")
    mes_col = COLUMN_ORDER.index("mes_referencia")
    invoice_id_col = COLUMN_ORDER.index("invoice_id") if "invoice_id" in COLUMN_ORDER else None
    provider_col = COLUMN_ORDER.index("provider_name") if "provider_name" in COLUMN_ORDER else None
    issue_col = COLUMN_ORDER.index("data_emissao_fatura") if "data_emissao_fatura" in COLUMN_ORDER else None
    parent_col = None
    status_col = COLUMN_ORDER.index("status_faturamento") if "status_faturamento" in COLUMN_ORDER else None
    total_col = COLUMN_ORDER.index("valor_boleto") if "valor_boleto" in COLUMN_ORDER else None
    saved_primary: dict[str, list[str]] = {}
    saved_fallback: dict[str, list[str]] = {}
    saved_fallback_rows: dict[str, list[list[str]]] = {}
    saved_secondary: dict[str, list[str]] = {}


    if data_rows and _PROTECTED_COL_INDEXES:
        for row in data_rows:
            uc = _row_value(row, uc_col)
            mes = _row_value(row, mes_col)
            if not uc or not mes:
                continue

            invoice_id = _derive_invoice_id_from_row(
                row,
                uc_col=uc_col,
                invoice_id_col=invoice_id_col,
                provider_col=provider_col,
                issue_col=issue_col,
                parent_col=parent_col,
            )
            explicit_invoice_id = _row_value(row, invoice_id_col)
            primary_key = _primary_key(uc, mes, invoice_id)
            fallback_key = _fallback_key(uc, mes)
            secondary_key = _secondary_key_from_row(
                row,
                uc_col=uc_col,
                mes_col=mes_col,
                issue_col=issue_col,
                provider_col=provider_col,
                status_col=status_col,
                total_col=total_col,
            )
            protected_vals = [
                str(row[idx]) if len(row) > idx else ""
                for idx in _PROTECTED_COL_INDEXES
            ]
            if not any(str(v).strip() for v in protected_vals):
                continue

            if primary_key:
                saved_primary[primary_key] = _merge_saved_values(
                    saved_primary.get(primary_key),
                    protected_vals,
                )
            # Fallback UC|Mes:
            # - linhas sem primary key
            # - linhas de origem sintetica (invoice_id explicito vazio)
            # Permite migrar com seguranca para ID real quando houver match unico.
            is_synthetic_origin = bool(invoice_id) and (
                (not explicit_invoice_id) or explicit_invoice_id.startswith("SYN|")
            )
            if fallback_key and ((not primary_key) or is_synthetic_origin):
                saved_fallback_rows.setdefault(fallback_key, []).append(protected_vals)
                saved_fallback[fallback_key] = _merge_saved_values(
                    saved_fallback.get(fallback_key),
                    protected_vals,
                )
            if secondary_key and ((not primary_key) or is_synthetic_origin):
                saved_secondary[secondary_key] = _merge_saved_values(
                    saved_secondary.get(secondary_key),
                    protected_vals,
                )

    return {
        "primary": saved_primary,
        "fallback": saved_fallback,
        "fallback_rows": saved_fallback_rows,
        "secondary": saved_secondary,
    }


def capture_protected_snapshot(
    ws: gspread.Worksheet,
    existing_data_rows: list[list[str]] | None = None,
) -> dict[str, object]:
    """Lê e captura snapshot dos valores protegidos (L/N/P)."""
    from stats import stats

    if existing_data_rows is None:
        existing = _retry(ws.get_all_values, value_render_option="UNFORMATTED_VALUE")
        stats.sheets_read_requests += 1
        data_rows = existing[1:] if len(existing) > 1 else []
    else:
        data_rows = existing_data_rows

    return _build_protected_snapshot_from_rows(data_rows)


def get_worksheet() -> gspread.Worksheet:
    from stats import stats
    client = _get_client()
    spreadsheet = _retry(client.open_by_key, SPREADSHEET_ID)
    try:
        ws = _retry(spreadsheet.worksheet, SHEET_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = _retry(
            spreadsheet.add_worksheet,
            title=SHEET_TAB_NAME, rows=1000, cols=20,
            is_write=True,
        )
        stats.sheets_write_requests += 1
        logger.info("Aba '%s' criada.", SHEET_TAB_NAME)
    # Nao altera estrutura de colunas automaticamente para nao arriscar layout/formatações.
    needed_cols = len(COLUMN_ORDER)
    if ws.col_count < needed_cols:
        logger.warning(
            "Aba '%s' possui %d colunas, mas o mapeamento exige %d. "
            "Ajuste manualmente as colunas para evitar impacto visual.",
            SHEET_TAB_NAME,
            ws.col_count,
            needed_cols,
        )
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
            is_write=True,
        )
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += len(headers)
        logger.info("Headers atualizados.")

    _ensure_currency_column_format(ws)


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


def read_column_values(ws: gspread.Worksheet, col_idx: int) -> list[str]:
    """Lê apenas uma coluna (0-based), retornando valores a partir da linha 2."""
    from stats import stats
    values = _retry(ws.col_values, col_idx + 1)
    stats.sheets_read_requests += 1
    if len(values) <= 1:
        return []
    return values[1:]




def write_all_rows(
    ws: gspread.Worksheet,
    rows: list[list[str]],
    existing_data_rows: list[list[str]] | None = None,
    protected_snapshot: dict[str, object] | None = None,
) -> None:
    """
    Reescreve toda a grade de valores (sem mexer em formatacao), projetando L/N/P por chave.

    A projecao de L/N/P acontece ANTES da primeira escrita. Assim, cada linha escrita
    ja sai consistente em um unico passo, reduzindo risco de vazamento de valores
    protegidos em caso de interrupcao no meio do full sync.

    Prioridade de chave:
      1) UC|Mes de Referencia|Invoice ID
      2) UC|Mes de Referencia (fallback)
    """
    from stats import stats

    uc_col = COLUMN_ORDER.index("uc")
    mes_col = COLUMN_ORDER.index("mes_referencia")
    invoice_id_col = COLUMN_ORDER.index("invoice_id") if "invoice_id" in COLUMN_ORDER else None
    provider_col = COLUMN_ORDER.index("provider_name") if "provider_name" in COLUMN_ORDER else None
    issue_col = COLUMN_ORDER.index("data_emissao_fatura") if "data_emissao_fatura" in COLUMN_ORDER else None
    parent_col = None
    status_col = COLUMN_ORDER.index("status_faturamento") if "status_faturamento" in COLUMN_ORDER else None
    total_col = COLUMN_ORDER.index("valor_boleto") if "valor_boleto" in COLUMN_ORDER else None

    saved_primary: dict[str, list[str]]
    saved_fallback: dict[str, list[str]]
    saved_fallback_rows: dict[str, list[list[str]]]
    saved_secondary: dict[str, list[str]]

    if existing_data_rows is not None:
        data_rows = existing_data_rows
    else:
        existing = _retry(ws.get_all_values, value_render_option="UNFORMATTED_VALUE")
        stats.sheets_read_requests += 1
        data_rows = existing[1:] if len(existing) > 1 else []

    data_rows_count = len(data_rows)

    if protected_snapshot is None:
        protected_snapshot = _build_protected_snapshot_from_rows(data_rows)

    saved_primary = dict(protected_snapshot.get("primary", {}))
    saved_fallback = dict(protected_snapshot.get("fallback", {}))
    saved_fallback_rows = {
        key: list(val_list)
        for key, val_list in protected_snapshot.get("fallback_rows", {}).items()
    }
    saved_secondary = dict(protected_snapshot.get("secondary", {}))

    if saved_primary or saved_fallback or saved_secondary:
        logger.info(
            "Colunas protegidas: %d chaves primarias, %d fallback, %d sequencias fallback e %d secundarias.",
            len(saved_primary),
            len(saved_fallback),
            len(saved_fallback_rows),
            len(saved_secondary),
        )

    matched_primary_keys: set[str] = set()
    matched_fallback_keys: set[str] = set()
    matched_secondary_keys: set[str] = set()
    fallback_cursors: dict[str, int] = {}
    projected_rows: list[list[str]] = []
    restored = 0

    for row in rows:
        row_out = list(row[:WRITE_COL_COUNT])
        while len(row_out) < WRITE_COL_COUNT:
            row_out.append("")

        vals: list[str] | None = None

        if _PROTECTED_COL_INDEXES:
            uc = _row_value(row, uc_col)
            mes = _row_value(row, mes_col)
            if uc and mes:
                invoice_id = _derive_invoice_id_from_row(
                    row,
                    uc_col=uc_col,
                    invoice_id_col=invoice_id_col,
                    provider_col=provider_col,
                    issue_col=issue_col,
                    parent_col=parent_col,
                )
                explicit_invoice_id = _row_value(row, invoice_id_col)
                secondary_key = _secondary_key_from_row(
                    row,
                    uc_col=uc_col,
                    mes_col=mes_col,
                    issue_col=issue_col,
                    provider_col=provider_col,
                    status_col=status_col,
                    total_col=total_col,
                )
                is_synthetic_target = (not explicit_invoice_id) or invoice_id.startswith("SYN|")
                primary_key = _primary_key(uc, mes, invoice_id)
                fallback_key = _fallback_key(uc, mes)

                vals = saved_primary.get(primary_key) if primary_key else None
                if vals is None and is_synthetic_target and secondary_key:
                    vals = saved_secondary.get(secondary_key)
                    if vals is not None:
                        matched_secondary_keys.add(secondary_key)
                        if fallback_key:
                            matched_fallback_keys.add(fallback_key)
                if vals is None:
                    seq = saved_fallback_rows.get(fallback_key, [])
                    cursor = fallback_cursors.get(fallback_key, 0)

                    if primary_key:
                        # Migracao segura null/sintetico -> ID real:
                        # so aceita fallback quando ha exatamente 1 candidato.
                        if len(seq) == 1 and cursor < 1:
                            vals = seq[0]
                            fallback_cursors[fallback_key] = 1
                            matched_fallback_keys.add(fallback_key)
                    else:
                        if cursor < len(seq):
                            vals = seq[cursor]
                            fallback_cursors[fallback_key] = cursor + 1
                            matched_fallback_keys.add(fallback_key)
                        else:
                            vals = saved_fallback.get(fallback_key)
                            if vals is not None:
                                matched_fallback_keys.add(fallback_key)

                if primary_key and vals is not None and primary_key in saved_primary:
                    matched_primary_keys.add(primary_key)

            if vals:
                normalized = list(vals[:len(_PROTECTED_COL_INDEXES)])
                while len(normalized) < len(_PROTECTED_COL_INDEXES):
                    normalized.append("")
                for offset, col_idx in enumerate(_PROTECTED_COL_INDEXES):
                    row_out[col_idx] = normalized[offset]
                if any(str(v).strip() for v in normalized):
                    restored += 1

        projected_rows.append(row_out)

    if restored:
        logger.info("Colunas protegidas: %d linhas restauradas.", restored)

    needed = len(projected_rows) + 1 if projected_rows else 1
    if ws.row_count < needed:
        _retry(ws.resize, rows=needed, is_write=True)
        stats.sheets_write_requests += 1

    if projected_rows:
        for i in range(0, len(projected_rows), CHUNK_SIZE):
            chunk_rows = projected_rows[i:i + CHUNK_SIZE]
            start_row = i + 2
            end_row = start_row + len(chunk_rows) - 1

            batch_ranges: list[dict] = []
            cells_written = 0
            for seg_start, seg_end in _ALL_SEGMENTS:
                seg_chunk = [_slice_segment(rowv, seg_start, seg_end) for rowv in chunk_rows]
                batch_ranges.append({
                    "range": f"{_col_letter(seg_start)}{start_row}:{_col_letter(seg_end)}{end_row}",
                    "values": seg_chunk,
                })
                cells_written += sum(len(r) for r in seg_chunk)

            _retry(
                ws.batch_update,
                batch_ranges,
                value_input_option="RAW",
                is_write=True,
            )
            stats.sheets_write_requests += 1
            stats.sheets_cells_written += cells_written

            logger.info(
                "Chunk escrito: linhas %d-%d (%d rows, %d cells)",
                start_row,
                end_row,
                len(chunk_rows),
                cells_written,
            )
            if i + CHUNK_SIZE < len(projected_rows):
                time.sleep(CHUNK_PAUSE_S)

    if data_rows_count > len(projected_rows):
        start_row = len(projected_rows) + 2
        end_row = data_rows_count + 1
        clear_ranges = [
            f"{_col_letter(seg_start)}{start_row}:{_col_letter(seg_end)}{end_row}"
            for seg_start, seg_end in _ALL_SEGMENTS
        ]
        _retry(ws.batch_clear, clear_ranges, is_write=True)
        stats.sheets_write_requests += 1
        cleared_cells = (end_row - start_row + 1) * sum(
            (seg_end - seg_start + 1) for seg_start, seg_end in _ALL_SEGMENTS
        )
        stats.sheets_cells_written += max(cleared_cells, 0)

    orphaned_primary = len(
        [
            key for key in saved_primary.keys()
            if key not in matched_primary_keys and "|SYN|" not in key
        ]
    )
    orphaned_fallback = len(set(saved_fallback.keys()) - matched_fallback_keys)
    orphaned_secondary = len(set(saved_secondary.keys()) - matched_secondary_keys)
    orphaned = orphaned_primary + orphaned_fallback + orphaned_secondary
    if orphaned:
        logger.warning(
            "Colunas protegidas: %d chaves nao encontraram correspondencia (dados orfaos).",
            orphaned,
        )

def append_rows(ws: gspread.Worksheet, rows: list[list[str]]) -> None:
    """Adiciona linhas no final, escrevendo apenas colunas nao protegidas."""
    from stats import stats
    if not rows:
        return

    all_data = _retry(ws.get_all_values)
    stats.sheets_read_requests += 1
    next_row = len(all_data) + 1

    needed = next_row + len(rows) - 1
    if ws.row_count < needed:
        _retry(ws.resize, rows=needed, is_write=True)
        stats.sheets_write_requests += 1

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk_rows = rows[i:i + CHUNK_SIZE]
        start_row = next_row + i
        end_row = start_row + len(chunk_rows) - 1

        batch_ranges: list[dict] = []
        cells_written = 0
        for seg_start, seg_end in _WRITABLE_SEGMENTS:
            seg_chunk = [_slice_segment(row, seg_start, seg_end) for row in chunk_rows]
            batch_ranges.append({
                "range": f"{_col_letter(seg_start)}{start_row}:{_col_letter(seg_end)}{end_row}",
                "values": seg_chunk,
            })
            cells_written += sum(len(r) for r in seg_chunk)

        _retry(
            ws.batch_update,
            batch_ranges,
            value_input_option="RAW",
            is_write=True,
        )
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += cells_written

        logger.info(
            "Append chunk: linhas %d-%d (%d rows, %d cells)",
            start_row, end_row, len(chunk_rows), cells_written,
        )
        if i + CHUNK_SIZE < len(rows):
            time.sleep(CHUNK_PAUSE_S)


def update_rows_in_place(
    ws: gspread.Worksheet,
    updates: dict[int, list[str]],
) -> None:
    """Atualiza linhas especificas, escrevendo apenas colunas nao protegidas."""
    from stats import stats
    if not updates:
        return

    ordered = sorted(updates.items(), key=lambda item: item[0])

    for i in range(0, len(ordered), CHUNK_SIZE):
        rows_chunk = ordered[i:i + CHUNK_SIZE]
        batch: list[dict] = []

        for seg_start, seg_end in _WRITABLE_SEGMENTS:
            for sheet_row, row_data in rows_chunk:
                seg_values = _slice_segment(row_data, seg_start, seg_end)
                start_cell = gspread.utils.rowcol_to_a1(sheet_row, seg_start + 1)
                end_cell = gspread.utils.rowcol_to_a1(sheet_row, seg_end + 1)
                batch.append({
                    "range": f"{start_cell}:{end_cell}",
                    "values": [seg_values],
                })

        if batch:
            _retry(
                ws.batch_update,
                batch,
                value_input_option="RAW",
                is_write=True,
            )
            cells = sum(len(item["values"][0]) for item in batch)
            stats.sheets_write_requests += 1
            stats.sheets_cells_written += cells

        if i + CHUNK_SIZE < len(ordered):
            time.sleep(CHUNK_PAUSE_S)


def update_columns_in_place(
    ws: gspread.Worksheet,
    updates: dict[int, dict[int, str]],
) -> None:
    """Atualiza celulas especificas por indice de coluna (0-based)."""
    from stats import stats
    if not updates:
        return

    total_cells = 0
    forbidden_writes: list[tuple[int, int]] = []
    for sheet_row, col_map in updates.items():
        for col_idx, value in col_map.items():
            if col_idx in _PROTECTED_COL_INDEXES:
                forbidden_writes.append((sheet_row, col_idx))
                continue
            total_cells += 1

    if forbidden_writes:
        sample = ", ".join(
            f"R{r}C{c+1}" for r, c in forbidden_writes[:10]
        )
        raise RuntimeError(
            "Tentativa bloqueada de escrita em coluna protegida "
            f"(L/N/P). Células: {sample}"
        )

    chunk: list[dict] = []
    sent_cells = 0
    for sheet_row, col_map in updates.items():
        for col_idx, value in col_map.items():
            if col_idx in _PROTECTED_COL_INDEXES:
                continue
            cell = gspread.utils.rowcol_to_a1(sheet_row, col_idx + 1)
            chunk.append({
                "range": cell,
                "values": [[value]],
            })
            if len(chunk) >= CHUNK_SIZE:
                _retry(
                    ws.batch_update,
                    chunk,
                    value_input_option="RAW",
                    is_write=True,
                )
                stats.sheets_write_requests += 1
                stats.sheets_cells_written += len(chunk)
                sent_cells += len(chunk)
                chunk = []
                if sent_cells < total_cells:
                    time.sleep(CHUNK_PAUSE_S)

    if chunk:
        _retry(
            ws.batch_update,
            chunk,
            value_input_option="RAW",
            is_write=True,
        )
        stats.sheets_write_requests += 1
        stats.sheets_cells_written += len(chunk)

    logger.info("Columns update: %d celulas em %d linhas", total_cells, len(updates))

