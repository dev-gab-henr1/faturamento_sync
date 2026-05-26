"""
Faturamento Sync – loop principal.

Fluxo full sync:
  1. Fetch ClickUp tasks (slim) → mapa UC → task
  2. Fetch PowerRev invoices mês a mês → agrupar por UC
  3. Montar linhas: cada invoice = uma linha, enriquecida com ClickUp
  4. Calcular Mês de Atendimento (meses únicos por UC)
  5. Escrever no Sheets

Delta sync:
  - Tasks atualizadas no ClickUp → update campos ClickUp in-place
  - PowerRev: checa 3 meses (anterior, atual, próximo)
"""
import os
import signal
import sys
import time
import logging
import atexit
import socket
import random
import re
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil.tz import gettz

try:
    import msvcrt  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    msvcrt = None

try:
    import fcntl  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    fcntl = None

from config import (
    DELTA_SYNC_INTERVAL_S,
    FULL_SYNC_DAILY_TIME,
    FULL_SYNC_TIMEZONE,
    FULL_SYNC_RETRY_BASE_S,
    FULL_SYNC_RETRY_MAX_S,
    POWERREV_BASE_URL,
    POWERREV_REFERENCE_MONTH_ONLY,
    DISTRIBUTED_LOCK_ENABLED,
    DISTRIBUTED_LOCK_TTL_S,
    DISTRIBUTED_LOCK_REFRESH_S,
    DISTRIBUTED_LOCK_TAB_NAME,
)
from clickup_client import fetch_all_tasks, iter_team_tasks_with_uc, reset_session as reset_clickup_session
from row_expander import (
    slim_task,
    get_inicio_operacao,
    get_fim_operacao,
    get_fim_operacao_display,
    compute_data_vencimento_for_task,
    compute_envio_boleto_for_task,
    extract_task_uc,
    build_row,
    yyyymm_to_label,
    label_to_yyyymm,
    _extract_field_value,
    _build_observacoes,
    _resolve_dropdown_value,
)
from sheets_manager import (
    get_worksheet,
    ensure_headers,
    read_all_rows,
    read_column_values,
    write_all_rows,
    append_rows,
    update_columns_in_place,
    reset_client as reset_sheets_client,
)
from field_map import get_headers, COLUMN_ORDER, FIELD_MAP
from stats import (
    stats,
    log_memory,
    log_sync_stats,
    force_free_memory,
    begin_memory_cycle,
    end_memory_cycle,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("faturamento_sync")

# comentario normalizado
_shutdown_requested = False


def _handle_sigterm(signum, frame):
    global _shutdown_requested
    logger.info("Sinal SIGTERM recebido - shutdown graceful solicitado.")
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_sigterm)
# SIGINT (Ctrl+C) mantém comportamento padrão: levanta KeyboardInterrupt

# comentario normalizado
_MAX_CONSECUTIVE_ERRORS = 5  # após N erros seguidos, reset total de sessions
_ERROR_BACKOFF_BASE = 30     # backoff base em segundos
_ERROR_BACKOFF_MAX = 300     # backoff máximo (5 min)
_ERRO_SISTEMA = "Erro no sistema"  # marca em Q para invoices que sumiram da PowerRev
_NAO_PROCESSADO = "Não processado"  # marca em Q para fatura do mes seguinte ainda nao gerada
_MISSING_DISTRIBUTOR_STATUS_ALIASES = {
    "MISSING DISTRIBUTOR INVOICE",
    "SEM FATURA DISTRIBUIDORA",
    "SEM FATURA DA DISTRIBUIDORA",
}

_known_task_ids: set[str] = set()
_FAR_FUTURE = datetime(9999, 1, 1)
_LOCK_FILE_PATH = os.path.join(os.path.dirname(__file__), ".faturamento_sync.lock")
_LOCK_HANDLE = None
_DLOCK_OWNER_ID = f"{socket.gethostname()}:{os.getpid()}:{int(time.time())}:{random.randint(1000,9999)}"
_DLOCK_LAST_REFRESH_TS = 0.0


def _is_missing_distributor_status(status_value: str) -> bool:
    normalized = " ".join(str(status_value or "").strip().upper().replace("_", " ").split())
    return normalized in _MISSING_DISTRIBUTOR_STATUS_ALIASES


def _resolve_full_sync_tz(tz_name: str):
    zone_name = (tz_name or "America/Sao_Paulo").strip() or "America/Sao_Paulo"
    try:
        return ZoneInfo(zone_name)
    except Exception:
        tz_fallback = gettz(zone_name)
        if tz_fallback is not None:
            logger.info(
                "ZoneInfo indisponivel para '%s' neste ambiente; usando dateutil.tz.",
                zone_name,
            )
            return tz_fallback

        logger.warning(
            "Timezone '%s' indisponivel no ambiente. Fallback para UTC-03:00 fixo.",
            zone_name,
        )
        return timezone(timedelta(hours=-3), name="BRT")


_FULL_SYNC_TZ = _resolve_full_sync_tz(FULL_SYNC_TIMEZONE)

_MONTH_NUM_PT = {
    "jan.": 1, "fev.": 2, "mar.": 3, "abr.": 4,
    "mai.": 5, "jun.": 6, "jul.": 7, "ago.": 8,
    "set.": 9, "out.": 10, "nov.": 11, "dez.": 12,
}

def _extract_task_id_from_link(link: str) -> str:
    if not link:
        return ""
    if link.startswith("https://app.clickup.com/t/"):
        return link.split("/t/")[-1]
    return link


def _normalize_header_name(text: str) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""

    # Tentativa de corrigir mojibake comum (UTF-8 lido como latin-1/cp1252).
    try:
        fixed = raw.encode("latin-1").decode("utf-8")
        if fixed:
            raw = fixed.lower()
    except Exception:
        pass

    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())


def _header_index(headers: list[str], *candidates: str) -> int:
    # Match exato primeiro (mais rápido e preserva comportamento atual).
    for c in candidates:
        if c in headers:
            return headers.index(c)

    # Match normalizado (aceita acento/sem acento e mojibake).
    norm_to_idx: dict[str, int] = {}
    for i, h in enumerate(headers):
        norm_to_idx.setdefault(_normalize_header_name(h), i)
    for c in candidates:
        idx = norm_to_idx.get(_normalize_header_name(c))
        if idx is not None:
            return idx

    raise ValueError(f"Nenhum header encontrado para aliases: {candidates!r}")


def _has_header(headers: list[str], *candidates: str) -> bool:
    try:
        _header_index(headers, *candidates)
        return True
    except Exception:
        return False


def _parse_daily_time(value: str) -> tuple[int, int]:
    raw = (value or "").strip()
    try:
        hour_s, minute_s = raw.split(":")
        hour = int(hour_s)
        minute = int(minute_s)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass
    logger.warning("FULL_SYNC_DAILY_TIME inválido (%s). Usando 00:10.", value)
    return 0, 10


_FULL_SYNC_DAILY_HOUR, _FULL_SYNC_DAILY_MINUTE = _parse_daily_time(FULL_SYNC_DAILY_TIME)


def _next_full_sync_timestamp(now_local: datetime | None = None) -> float:
    now = now_local or datetime.now(_FULL_SYNC_TZ)
    candidate = now.replace(
        hour=_FULL_SYNC_DAILY_HOUR,
        minute=_FULL_SYNC_DAILY_MINUTE,
        second=0,
        microsecond=0,
    )
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate.timestamp()


def _format_local_dt(ts: float) -> str:
    return datetime.fromtimestamp(ts, _FULL_SYNC_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _acquire_single_instance_lock() -> None:
    global _LOCK_HANDLE
    if _LOCK_HANDLE is not None:
        return

    lock_handle = open(_LOCK_FILE_PATH, "a+")
    try:
        lock_handle.seek(0)
        lock_handle.truncate()
        lock_handle.write(str(os.getpid()))
        lock_handle.flush()

        if msvcrt is not None:
            lock_handle.seek(0)
            msvcrt.locking(lock_handle.fileno(), msvcrt.LK_NBLCK, 1)
        elif fcntl is not None:  # pragma: no cover
            lock_handle.seek(0)
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:  # pragma: no cover
            raise RuntimeError("Lock de processo não suportado neste ambiente.")
    except Exception as exc:
        try:
            lock_handle.close()
        except Exception:
            pass
        raise RuntimeError(
            f"Já existe outra instância do sync em execução (lock: {_LOCK_FILE_PATH})."
        ) from exc

    _LOCK_HANDLE = lock_handle


def _release_single_instance_lock() -> None:
    global _LOCK_HANDLE
    if _LOCK_HANDLE is None:
        return
    try:
        if msvcrt is not None:
            _LOCK_HANDLE.seek(0)
            msvcrt.locking(_LOCK_HANDLE.fileno(), msvcrt.LK_UNLCK, 1)
        elif fcntl is not None:  # pragma: no cover
            fcntl.flock(_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        _LOCK_HANDLE.close()
    finally:
        _LOCK_HANDLE = None


def _get_distributed_lock_worksheet():
    ws_main = get_worksheet()
    spreadsheet = ws_main.spreadsheet
    try:
        return spreadsheet.worksheet(DISTRIBUTED_LOCK_TAB_NAME)
    except Exception:
        ws_lock = spreadsheet.add_worksheet(title=DISTRIBUTED_LOCK_TAB_NAME, rows=5, cols=6)
        logger.info("Aba de lock distribuido criada: %s", DISTRIBUTED_LOCK_TAB_NAME)
        return ws_lock


def _read_distributed_lock_state(ws_lock) -> tuple[str, float]:
    row = ws_lock.row_values(1)
    owner = row[0].strip() if len(row) >= 1 else ""
    expires_raw = row[1].strip() if len(row) >= 2 else ""
    try:
        expires = float(expires_raw) if expires_raw else 0.0
    except Exception:
        expires = 0.0
    return owner, expires


def _write_distributed_lock_state(ws_lock, owner: str, expires_ts: float) -> None:
    now_ts = time.time()
    ws_lock.update(
        range_name="A1:D1",
        values=[[owner, f"{expires_ts:.3f}", f"{now_ts:.3f}", socket.gethostname()]],
        value_input_option="RAW",
    )


def _acquire_distributed_lock() -> bool:
    global _DLOCK_LAST_REFRESH_TS
    if not DISTRIBUTED_LOCK_ENABLED:
        return True

    for attempt in range(1, 6):
        now_ts = time.time()
        ws_lock = _get_distributed_lock_worksheet()
        owner, expires = _read_distributed_lock_state(ws_lock)

        if owner and owner != _DLOCK_OWNER_ID and expires > now_ts:
            logger.error(
                "Lock distribuido ativo por outra instancia (owner=%s, expira_em=%.0f).",
                owner,
                expires,
            )
            return False

        _write_distributed_lock_state(ws_lock, _DLOCK_OWNER_ID, now_ts + float(DISTRIBUTED_LOCK_TTL_S))
        time.sleep(0.15)
        owner_after, expires_after = _read_distributed_lock_state(ws_lock)
        if owner_after == _DLOCK_OWNER_ID and expires_after > now_ts:
            _DLOCK_LAST_REFRESH_TS = now_ts
            logger.info("Lock distribuido adquirido (owner=%s).", _DLOCK_OWNER_ID)
            return True

        time.sleep(0.2 * attempt)

    logger.error("Falha ao adquirir lock distribuido apos retries.")
    return False


def _refresh_distributed_lock_if_needed(force: bool = False) -> None:
    global _DLOCK_LAST_REFRESH_TS, _shutdown_requested
    if not DISTRIBUTED_LOCK_ENABLED:
        return

    now_ts = time.time()
    if not force and (now_ts - _DLOCK_LAST_REFRESH_TS) < max(10, DISTRIBUTED_LOCK_REFRESH_S):
        return

    ws_lock = _get_distributed_lock_worksheet()
    owner, expires = _read_distributed_lock_state(ws_lock)
    if owner and owner != _DLOCK_OWNER_ID and expires > now_ts:
        _shutdown_requested = True
        raise RuntimeError(
            f"Lock distribuido foi tomado por outra instancia (owner={owner}). Encerrando para evitar concorrencia."
        )

    _write_distributed_lock_state(ws_lock, _DLOCK_OWNER_ID, now_ts + float(DISTRIBUTED_LOCK_TTL_S))
    _DLOCK_LAST_REFRESH_TS = now_ts


def _release_distributed_lock() -> None:
    if not DISTRIBUTED_LOCK_ENABLED:
        return
    try:
        ws_lock = _get_distributed_lock_worksheet()
        owner, _ = _read_distributed_lock_state(ws_lock)
        if owner == _DLOCK_OWNER_ID:
            _write_distributed_lock_state(ws_lock, "", 0.0)
            logger.info("Lock distribuido liberado.")
    except Exception:
        logger.warning("Falha ao liberar lock distribuido.", exc_info=True)


def _rehydrate_known_task_ids_from_sheet() -> int:
    global _known_task_ids
    ws = get_worksheet()
    ensure_headers(ws)
    headers = get_headers()
    task_id_col = _header_index(headers, "Task ID")
    task_id_values = read_column_values(ws, task_id_col)

    restored = 0
    for cell_value in task_id_values:
        tid = _extract_task_id_from_link(str(cell_value).strip())
        if not tid or tid in _known_task_ids:
            continue
        _known_task_ids.add(tid)
        restored += 1

    logger.info("Reidratação _known_task_ids: %d IDs recuperados da planilha.", restored)
    return restored


def _run_full_sync_until_success(
    *,
    reason: str,
) -> bool:
    attempt = 0
    while not _shutdown_requested:
        attempt += 1

        try:
            _refresh_distributed_lock_if_needed(force=True)
            full_sync()
            return True
        except MemoryError:
            end_memory_cycle("FULL SYNC")
            logger.critical(
                "MemoryError no full sync (%s, tentativa %d).",
                reason,
                attempt,
            )
        except Exception:
            end_memory_cycle("FULL SYNC")
            logger.exception(
                "Erro no full sync (%s, tentativa %d).",
                reason,
                attempt,
            )

        _reset_all_sessions(f"full_sync_{reason}_tentativa_{attempt}")
        wait_s = min(
            FULL_SYNC_RETRY_BASE_S * (2 ** max(attempt - 1, 0)),
            FULL_SYNC_RETRY_MAX_S,
        )
        logger.warning("Full sync (%s) retry em %ds.", reason, int(wait_s))
        _interruptible_sleep(wait_s)

    return False

_STATUS_TROCA_PLANO = "25a28dc4-16ff-4ecf-b94f-a7b3a6eef42c"
_STATUS_PLANEJAMENTO_BLACK = "29e28b58-2922-49c9-a8d0-f2a83d398d0a"
_STATUS_CF_ID = "1a5118f7-b9a0-466f-889d-37edd76bd304"
_STATUS_TROCA_PLANO_LABEL = "Encerrado - Troca de Plano"
_STATUS_PLANEJAMENTO_BLACK_LABEL = "Planejamento - Black"
_PLANEJAMENTO_LIST_ID = "901321549851"
_NEVER_JOINED_STATUS_LABELS = (
    "Eliminado",
    "Excluido",
    "Demitido",
    "Encerrado - Financeiro",
    "Baixo Consumo",
)


def _get_task_status_raw(task: dict) -> str:
    """Retorna o value bruto do custom field Status Detalhado.

    ClickUp pode retornar:
      - string UUID
      - int (orderindex)
      - dict com id/name
    """
    for cf in task.get("custom_fields", []):
        if cf.get("id") == _STATUS_CF_ID:
            val = cf.get("value")
            if isinstance(val, dict):
                # Preferir id se existir
                for key in ("id", "value", "uuid", "key", "name"):
                    if val.get(key):
                        return str(val.get(key))
                return ""
            return str(val) if val is not None else ""
    return ""


def _normalize_status_label(text: str) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())


def _status_matches(task: dict, target_id: str, target_label: str) -> bool:
    target_norm = _normalize_status_label(target_label)
    raw = _get_task_status_raw(task)

    if raw == target_id:
        return True
    if _normalize_status_label(raw) == target_norm:
        return True

    if raw:
        try:
            resolved = _resolve_dropdown_value(_STATUS_CF_ID, raw)
            if _normalize_status_label(resolved) == target_norm:
                return True
        except Exception:
            pass

    # Se vier como orderindex, resolver usando options.
    for cf in task.get("custom_fields", []):
        if cf.get("id") != _STATUS_CF_ID:
            continue
        val = cf.get("value")
        options = cf.get("type_config", {}).get("options", [])
        if not options:
            try:
                from clickup_client import get_custom_field_options
                options = get_custom_field_options(_STATUS_CF_ID)
            except Exception:
                options = []
        if not options:
            break
        for opt in options:
            if str(opt.get("orderindex")) == str(val) or str(opt.get("id")) == str(val):
                if _normalize_status_label(opt.get("name", "")) == target_norm:
                    return True
        break

    return False


def _is_troca_plano(task: dict) -> bool:
    """Detecta se a task está no status 'Encerrado - Troca de Plano'."""
    return _status_matches(task, _STATUS_TROCA_PLANO, _STATUS_TROCA_PLANO_LABEL)


def _is_planejamento_black(task: dict) -> bool:
    return _status_matches(
        task,
        _STATUS_PLANEJAMENTO_BLACK,
        _STATUS_PLANEJAMENTO_BLACK_LABEL,
    )


def _is_planejamento_black_text(value: str) -> bool:
    return _normalize_status_label(value) == _normalize_status_label(_STATUS_PLANEJAMENTO_BLACK_LABEL)


def _task_list_id(task: dict) -> str:
    return str(task.get("list_id", "") or "").strip()


def _is_planejamento_list_task(task: dict) -> bool:
    return _task_list_id(task) == _PLANEJAMENTO_LIST_ID


def _task_covers_reference_month(task: dict, reference_ym: str) -> bool:
    if len(reference_ym) < 6:
        return False

    try:
        ref_year = int(reference_ym[:4])
        ref_month = int(reference_ym[4:6])
    except (TypeError, ValueError):
        return False

    if ref_year <= 0 or not (1 <= ref_month <= 12):
        return False

    ref_date = datetime(ref_year, ref_month, 1)
    inicio = get_inicio_operacao(task)
    fim = get_fim_operacao(task)

    if inicio is not None:
        inicio_ym = datetime(inicio.year, inicio.month, 1)
        if inicio_ym > ref_date:
            return False

    if fim is not None:
        fim_ym = datetime(fim.year, fim.month, 1)
        if fim_ym < ref_date:
            return False

    return True


def _should_use_transition_resolution(task_list: list[dict]) -> bool:
    if len(task_list) <= 1:
        return False
    if any(_is_troca_plano(t) for t in task_list):
        return True
    return any(_is_planejamento_list_task(t) for t in task_list)


def _is_never_joined_terminated_task(task: dict) -> bool:
    """Task que nunca entrou na cooperativa: sem inicio/fim e status terminal específico."""
    if get_inicio_operacao(task) is not None:
        return False
    if get_fim_operacao(task) is not None:
        return False
    return any(_status_matches(task, lbl, lbl) for lbl in _NEVER_JOINED_STATUS_LABELS)


def _filter_out_never_joined_terminated(tasks: list[dict]) -> tuple[list[dict], int, set[str]]:
    filtered: list[dict] = []
    blocked = 0
    blocked_ids: set[str] = set()
    for t in tasks:
        if _is_never_joined_terminated_task(t):
            blocked += 1
            tid = str(t.get("id", "")).strip()
            if tid:
                blocked_ids.add(tid)
            continue
        filtered.append(t)
    return filtered, blocked, blocked_ids


def _filter_out_planejamento_black(tasks: list[dict]) -> tuple[list[dict], int, set[str]]:
    filtered: list[dict] = []
    blocked = 0
    blocked_ids: set[str] = set()
    for t in tasks:
        if _is_planejamento_black(t):
            blocked += 1
            tid = str(t.get("id", "")).strip()
            if tid:
                blocked_ids.add(tid)
            continue
        filtered.append(t)
    return filtered, blocked, blocked_ids


def _remove_rows_by_task_ids(ws, headers: list[str], task_ids: set[str]) -> int:
    if not task_ids:
        return 0
    try:
        task_id_col = _header_index(headers, "Task ID")
    except ValueError:
        return 0

    existing = read_all_rows(ws)
    if not existing:
        return 0

    kept_rows: list[list[str]] = []
    removed = 0
    for row in existing:
        raw_task = str(row[task_id_col]).strip() if len(row) > task_id_col else ""
        tid = _extract_task_id_from_link(raw_task)
        if tid and tid in task_ids:
            removed += 1
            continue
        kept_rows.append(row)

    if removed > 0:
        write_all_rows(ws, kept_rows, existing_data_rows=existing)
    return removed


def _build_uc_task_map(tasks: list[dict]) -> dict[str, list[dict]]:
    """Constrói mapa UC → lista de tasks.

    UCs com múltiplos cards (troca de plano) terão mais de uma task na lista.
    A lista é ordenada por inicio_operacao para facilitar resolução por mês.
    """
    uc_map: dict[str, list[dict]] = {}
    for task in tasks:
        uc = extract_task_uc(task)
        if uc:
            uc_map.setdefault(uc, []).append(task)

    # Ordenar tasks por inicio_operacao para UCs com múltiplas tasks
    for uc, task_list in uc_map.items():
        if len(task_list) > 1:
            task_list.sort(key=lambda t: get_inicio_operacao(t) or datetime(1900, 1, 1))

    return uc_map


def _resolve_task_for_month(
    task_list: list[dict], reference_ym: str,
) -> dict | None:
    """Resolve qual task representa uma UC em um mês de referência.

    Em modo de transição (troca de plano ou card na lista de Planejamento),
    retorna apenas task que cobre explicitamente o mês; se não houver, retorna None.
    Fora do modo de transição, mantém comportamento legado (última task da UC).
    """
    if not task_list:
        return None
    if len(task_list) == 1:
        return task_list[0]

    # Em modo transição (troca de plano ou presença de card na lista de Planejamento),
    # o mês só é válido se houver cobertura explícita de vigência.
    if not _should_use_transition_resolution(task_list):
        return task_list[-1]

    # Encontrar a task cujo período cobre o referenceMonth
    best: dict | None = None
    for task in task_list:
        if _task_covers_reference_month(task, reference_ym):
            # lista está ordenada por início; o último válido é o mais recente.
            best = task

    return best


def _get_powerrev_date_range(tasks: list[dict]) -> tuple[str, str]:
    """Determina range de meses para consultar PowerRev baseado em inicio_operacao."""
    if POWERREV_REFERENCE_MONTH_ONLY and len(POWERREV_REFERENCE_MONTH_ONLY) == 6:
        return POWERREV_REFERENCE_MONTH_ONLY, POWERREV_REFERENCE_MONTH_ONLY

    min_date: datetime | None = None
    for task in tasks:
        dt = get_inicio_operacao(task)
        if dt and (min_date is None or dt < min_date):
            min_date = dt

    if min_date is None:
        min_date = datetime(2023, 1, 1)

    start_ym = min_date.strftime("%Y%m")

    now = datetime.now()
    end_month = now.month + 1
    end_year = now.year
    if end_month > 12:
        end_month = 1
        end_year += 1
    end_ym = f"{end_year}{end_month:02d}"

    return start_ym, end_ym


def _build_uc_periods(uc_to_tasks: dict[str, list[dict]]) -> dict[str, tuple[str | None, str | None]]:
    """Calcula período global (inicio_ym, fim_ym) por UC.

    inicio_ym: menor inicio_operacao (YYYYMM) ou None
    fim_ym: maior fim_operacao (YYYYMM) ou None (período aberto)
    """
    uc_periods: dict[str, tuple[str | None, str | None]] = {}
    for uc, task_list in uc_to_tasks.items():
        inicio_global: datetime | None = None
        fim_global: datetime | None = None
        has_open_end = False

        for t in task_list:
            inicio = get_inicio_operacao(t)
            fim = get_fim_operacao(t)

            if inicio is not None:
                if inicio_global is None or inicio < inicio_global:
                    inicio_global = inicio

            if fim is None:
                has_open_end = True
            else:
                if fim_global is None or fim > fim_global:
                    fim_global = fim

        if has_open_end:
            fim_global = None

        inicio_ym = f"{inicio_global.year}{inicio_global.month:02d}" if inicio_global else None
        fim_ym = f"{fim_global.year}{fim_global.month:02d}" if fim_global else None
        uc_periods[uc] = (inicio_ym, fim_ym)

    return uc_periods


def _fetch_invoices_grouped(
    start_ym: str,
    end_ym: str,
    *,
    allowed_ucs: set[str] | None = None,
    uc_periods: dict[str, tuple[str | None, str | None]] | None = None,
) -> dict[str, list[dict]]:
    """Busca invoices mês a mês, retorna agrupado por UC.

    Filtros (para reduzir memória):
      - allowed_ucs: ignora UCs não presentes no ClickUp
      - uc_periods: ignora invoices fora do período global da UC
    """
    from powerrev_client import fetch_invoices_for_month, _load_consumer_units, reset_caches

    _load_consumer_units()

    uc_invoices: dict[str, list[dict]] = {}

    year = int(start_ym[:4])
    month = int(start_ym[4:6])
    end_year = int(end_ym[:4])
    end_month = int(end_ym[4:6])

    while (year < end_year) or (year == end_year and month <= end_month):
        _refresh_distributed_lock_if_needed()
        ref = f"{year}{month:02d}"
        attempt = 0
        while True:
            try:
                invoices = fetch_invoices_for_month(ref)
                for inv in invoices:
                    uc = inv.get("uc", "").strip()
                    if not uc:
                        continue

                    if allowed_ucs is not None and uc not in allowed_ucs:
                        continue

                    if uc_periods is not None:
                        inicio_ym, fim_ym = uc_periods.get(uc, (None, None))
                        if inicio_ym and ref < inicio_ym:
                            continue
                        if fim_ym and ref > fim_ym:
                            continue

                    uc_invoices.setdefault(uc, []).append(inv)
                del invoices
                break
            except RuntimeError:
                attempt += 1
                if attempt >= 3:
                    raise
                wait_s = 90
                logger.warning(
                    "PowerRev falhou para %s (tentativa %d/3). Aguardando %ds e retry do mesmo mês.",
                    ref, attempt + 1, wait_s,
                )
                _refresh_distributed_lock_if_needed(force=True)
                time.sleep(wait_s)

        month += 1
        if month > 12:
            month = 1
            year += 1

    reset_caches()

    total = sum(len(v) for v in uc_invoices.values())
    logger.info("PowerRev: %d invoices agrupados em %d UCs.", total, len(uc_invoices))
    return uc_invoices


def _build_rows_from_invoices(
    uc_invoices: dict[str, list[dict]],
    uc_to_tasks: dict[str, list[dict]],
) -> tuple[list[list[str]], set[tuple[str, str]]]:
    """
    Build rows by monthly validity window (inicio/fim) up to current month + 1.
    For months without PowerRev invoice, create one synthetic placeholder row.
    """
    now = datetime.now()
    upper_month = now.month + 1
    upper_year = now.year
    if upper_month > 12:
        upper_month = 1
        upper_year += 1
    upper_ym = f"{upper_year}{upper_month:02d}"

    def _ym_to_int(ym: str) -> int:
        if not ym or len(ym) < 6:
            return 0
        try:
            return int(ym[:6])
        except Exception:
            return 0

    def _next_ym(ym: str) -> str:
        y = int(ym[:4])
        m = int(ym[4:6])
        m += 1
        if m > 12:
            m = 1
            y += 1
        return f"{y}{m:02d}"

    def uc_sort_key(uc: str):
        task_list = uc_to_tasks.get(uc)
        if task_list:
            dt = get_inicio_operacao(task_list[0])
            return dt if dt is not None else _FAR_FUTURE
        return _FAR_FUTURE

    all_rows: list[list[str]] = []
    placeholder_keys: set[tuple[str, str]] = set()

    for uc in sorted(uc_to_tasks.keys(), key=uc_sort_key):
        invoices = uc_invoices.get(uc, [])
        task_list = uc_to_tasks[uc]

        inicio_global: datetime | None = None
        fim_global: datetime | None = None
        has_open_end = False

        for t in task_list:
            inicio = get_inicio_operacao(t)
            fim = get_fim_operacao(t)

            if inicio is not None and (inicio_global is None or inicio < inicio_global):
                inicio_global = inicio

            if fim is None:
                has_open_end = True
            elif fim_global is None or fim > fim_global:
                fim_global = fim

        if has_open_end:
            fim_global = None

        start_ym = f"{inicio_global.year}{inicio_global.month:02d}" if inicio_global else ""
        if not start_ym:
            invoice_months = sorted(
                {str(inv.get("referenceMonth", "")).strip() for inv in invoices if str(inv.get("referenceMonth", "")).strip()},
            )
            if not invoice_months:
                continue
            start_ym = invoice_months[0]

        start_int = _ym_to_int(start_ym)
        upper_int = _ym_to_int(upper_ym)
        if fim_global is not None:
            fim_ym = f"{fim_global.year}{fim_global.month:02d}"
            end_int = min(_ym_to_int(fim_ym), upper_int)
        else:
            end_int = upper_int

        if not start_int or start_int > end_int:
            continue

        invoices_by_month: dict[str, list[dict]] = {}
        for inv in invoices:
            rm = str(inv.get("referenceMonth", "")).strip()
            if not rm:
                continue
            invoices_by_month.setdefault(rm, []).append(inv)

        attendance_counter = 0
        ym = start_ym
        while _ym_to_int(ym) <= end_int:
            task = _resolve_task_for_month(task_list, ym)
            if task is not None:
                attendance_counter += 1
                month_invoices = invoices_by_month.get(ym, [])
                if month_invoices:
                    month_invoices.sort(
                        key=lambda inv: (
                            str(inv.get("issueDate", "")).strip(),
                            str(inv.get("invoiceId", "")).strip(),
                        ),
                    )
                    for inv in month_invoices:
                        all_rows.append(build_row(task, inv, attendance_counter))
                else:
                    placeholder_invoice = {
                        "referenceMonth": ym,
                        "status": "",
                        "issueDate": "",
                        "total": 0.0,
                        "invoiceId": "",
                        "providerName": "",
                    }
                    all_rows.append(build_row(task, placeholder_invoice, attendance_counter))
                    placeholder_keys.add((uc, yyyymm_to_label(ym)))

            ym = _next_ym(ym)

    if placeholder_keys:
        logger.info(
            "Placeholders: %d linhas '%s' geradas (sem fatura ate mes atual+1).",
            len(placeholder_keys),
            _NAO_PROCESSADO,
        )

    return all_rows, placeholder_keys

def _delta_powerrev_check(ws, headers: list[str]) -> None:
    """
    Check previous/current/next month in PowerRev and update existing rows only.
    """
    from powerrev_client import fetch_invoices_for_month, _load_consumer_units
    from sheets_manager import _derive_invoice_id_from_row

    if POWERREV_REFERENCE_MONTH_ONLY and len(POWERREV_REFERENCE_MONTH_ONLY) == 6:
        months_to_check = [POWERREV_REFERENCE_MONTH_ONLY]
    else:
        now = datetime.now()
        months_to_check = []
        for delta in (-1, 0, 1):
            m = now.month + delta
            y = now.year
            if m > 12:
                m -= 12
                y += 1
            elif m < 1:
                m += 12
                y -= 1
            months_to_check.append(f"{y}{m:02d}")

    logger.info("Delta PowerRev: checando meses %s", ", ".join(months_to_check))

    _load_consumer_units()

    all_invoices: list[dict] = []
    for ym in months_to_check:
        invoices = fetch_invoices_for_month(ym)
        if invoices:
            all_invoices.extend(invoices)
        del invoices

    if not all_invoices:
        logger.debug("Delta PowerRev: sem faturas nos 3 meses.")
        return

    existing_rows = read_all_rows(ws)
    uc_col = _header_index(headers, "UC")
    mes_col = _header_index(headers, "MÃªs de Referencia", "Mes de Referencia", "MÃƒÂªs de Referencia")
    status_fat_col = _header_index(headers, "Status de faturamento")
    emissao_col = _header_index(headers, "Data de EmissÃ£o da fatura", "Data de Emissao da fatura", "Data de EmissÃƒÂ£o da fatura")
    valor_col = _header_index(headers, "Valor do boleto")
    val_col = _header_index(headers, "ValidaÃ§Ã£o", "Validacao", "ValidaÃƒÂ§ÃƒÂ£o")
    invoice_col = _header_index(headers, "Invoice ID") if _has_header(headers, "Invoice ID") else None
    provider_col = _header_index(headers, "Provider") if _has_header(headers, "Provider") else None
    parent_col = COLUMN_ORDER.index("parentesco_agrupado") if "parentesco_agrupado" in COLUMN_ORDER else None

    months_to_check_set = set(months_to_check)

    def _fallback_key_delta(uc: str, yyyymm: str) -> str:
        if not uc or not yyyymm:
            return ""
        return f"{uc}|{yyyymm}"

    def _primary_key_delta(uc: str, yyyymm: str, invoice_id: str) -> str:
        if not uc or not yyyymm or not invoice_id:
            return ""
        return f"{uc}|{yyyymm}|{invoice_id}"

    def _money_token(value: object) -> str:
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
            return cleaned

    def _secondary_key_delta(
        uc: str,
        yyyymm: str,
        issue_date: str,
        provider_name: str,
        status: str,
        total: object,
    ) -> str:
        if not uc or not yyyymm:
            return ""
        total_token = _money_token(total)
        return "|".join([
            "SKD",
            uc.strip(),
            yyyymm.strip(),
            str(issue_date or "").strip(),
            str(provider_name or "").strip(),
            str(status or "").strip(),
            total_token,
        ])

    def _pop_unmatched(queue_map: dict[str, deque[int]], key: str, consumed: set[int]) -> int | None:
        if not key:
            return None
        q = queue_map.get(key)
        if not q:
            return None
        while q and q[0] in consumed:
            q.popleft()
        if not q:
            return None
        idx = q.popleft()
        consumed.add(idx)
        return idx

    def _pop_unique_unmatched(queue_map: dict[str, deque[int]], key: str, consumed: set[int]) -> int | None:
        if not key:
            return None
        q = queue_map.get(key)
        if not q:
            return None
        unmatched = [idx for idx in q if idx not in consumed]
        if len(unmatched) != 1:
            return None
        idx = unmatched[0]
        consumed.add(idx)
        return idx

    inv_primary_map: dict[str, deque[int]] = defaultdict(deque)
    inv_fallback_map: dict[str, deque[int]] = defaultdict(deque)
    inv_secondary_map: dict[str, deque[int]] = defaultdict(deque)
    for idx, inv in enumerate(all_invoices):
        uc = str(inv.get("uc", "")).strip()
        ref = str(inv.get("referenceMonth", "")).strip()
        inv_id = str(inv.get("invoiceId", "")).strip()
        if not uc or not ref:
            continue
        fallback = _fallback_key_delta(uc, ref)
        primary = _primary_key_delta(uc, ref, inv_id)
        secondary = str(inv.get("rowKey") or "").strip() or _secondary_key_delta(
            uc=uc,
            yyyymm=ref,
            issue_date=inv.get("issueDate", ""),
            provider_name=inv.get("providerName", ""),
            status=inv.get("status", ""),
            total=inv.get("total", ""),
        )
        if primary:
            inv_primary_map[primary].append(idx)
        if fallback:
            inv_fallback_map[fallback].append(idx)
        if secondary:
            inv_secondary_map[secondary].append(idx)

    compact_sheet_rows: list[tuple[int, str, str, str, str, str, list[str]]] = []
    required_len = max(uc_col, mes_col)
    for i, row in enumerate(existing_rows):
        if len(row) <= required_len:
            continue

        uc = str(row[uc_col]).strip()
        mes = str(row[mes_col]).strip()
        ref = label_to_yyyymm(mes)
        if not uc or not ref or ref not in months_to_check_set:
            continue

        current_q = str(row[val_col]).strip() if len(row) > val_col else ""
        invoice_id = _derive_invoice_id_from_row(
            row,
            uc_col=uc_col,
            invoice_id_col=invoice_col,
            provider_col=provider_col,
            issue_col=emissao_col,
            parent_col=parent_col,
        )
        row_secondary = _secondary_key_delta(
            uc=uc,
            yyyymm=ref,
            issue_date=(row[emissao_col] if len(row) > emissao_col else ""),
            provider_name=(row[provider_col] if provider_col is not None and len(row) > provider_col else ""),
            status=(row[status_fat_col] if len(row) > status_fat_col else ""),
            total=(row[valor_col] if len(row) > valor_col else ""),
        )
        compact_sheet_rows.append((i + 2, uc, ref, current_q, invoice_id, row_secondary, row))

    updates: dict[int, dict[int, object]] = {}
    consumed_invoices: set[int] = set()

    rows_evaluated = len(compact_sheet_rows)
    rows_matched = 0
    rows_no_match = 0
    rows_unchanged = 0
    changed_details: list[tuple[str, str]] = []

    for sheet_row, uc, ref, current_q, invoice_id, row_secondary, current_row in compact_sheet_rows:
        primary = _primary_key_delta(uc, ref, invoice_id)
        fallback = _fallback_key_delta(uc, ref)

        match_idx = None
        if primary:
            match_idx = _pop_unmatched(inv_primary_map, primary, consumed_invoices)
            if match_idx is None and invoice_id.startswith("SYN|"):
                match_idx = _pop_unique_unmatched(inv_secondary_map, row_secondary, consumed_invoices)
            if match_idx is None and invoice_id.startswith("SYN|"):
                match_idx = _pop_unique_unmatched(inv_fallback_map, fallback, consumed_invoices)
        else:
            match_idx = _pop_unique_unmatched(inv_secondary_map, row_secondary, consumed_invoices)
            if match_idx is None:
                match_idx = _pop_unique_unmatched(inv_fallback_map, fallback, consumed_invoices)

        if match_idx is None:
            rows_no_match += 1
            if current_q != _NAO_PROCESSADO:
                proposed = {val_col: _ERRO_SISTEMA}
                effective, diffs = _build_effective_updates(
                    row=current_row,
                    proposed=proposed,
                    headers=headers,
                )
                if effective:
                    updates[sheet_row] = effective
                    mes_label = _get_row_cell_value(current_row, mes_col)
                    detail_text = (
                        f"linha={sheet_row} | UC={uc or '[vazio]'} | MesRef={mes_label or '[vazio]'} | "
                        + "; ".join(diffs)
                    )
                    changed_details.append((uc or "[vazio]", detail_text))
                else:
                    rows_unchanged += 1
            else:
                rows_unchanged += 1
            continue

        rows_matched += 1
        inv = all_invoices[match_idx]
        proposed: dict[int, object] = {
            uc_col: str(inv.get("uc", "")).strip(),
            mes_col: yyyymm_to_label(str(inv.get("referenceMonth", "")).strip()),
            status_fat_col: inv.get("status") or "",
            emissao_col: inv.get("issueDate") or "",
            valor_col: "" if inv.get("total") is None else inv.get("total"),
            val_col: "",
        }
        if provider_col is not None:
            proposed[provider_col] = inv.get("providerName") or ""
        if invoice_col is not None:
            proposed[invoice_col] = inv.get("invoiceId") or ""

        effective, diffs = _build_effective_updates(
            row=current_row,
            proposed=proposed,
            headers=headers,
            money_cols={valor_col},
        )
        if not effective:
            rows_unchanged += 1
            continue

        updates[sheet_row] = effective
        log_uc = str(proposed.get(uc_col) or _get_row_cell_value(current_row, uc_col)).strip()
        log_mes = str(proposed.get(mes_col) or _get_row_cell_value(current_row, mes_col)).strip()
        detail_text = (
            f"linha={sheet_row} | UC={log_uc or '[vazio]'} | MesRef={log_mes or '[vazio]'} | "
            + "; ".join(diffs)
        )
        changed_details.append((log_uc or "[vazio]", detail_text))

    if updates:
        update_columns_in_place(ws, updates)
        q_cleared = sum(
            1
            for cols in updates.values()
            if val_col in cols and str(cols[val_col]).strip() == ""
        )
        q_erro = sum(1 for cols in updates.values() if val_col in cols and cols[val_col] == _ERRO_SISTEMA)
        logger.info(
            "Delta PowerRev: linhas avaliadas=%d, com_match=%d, alteradas=%d, sem_alteracao=%d, sem_match=%d",
            rows_evaluated,
            rows_matched,
            len(updates),
            rows_unchanged,
            rows_no_match,
        )
        logger.info(
            "Delta PowerRev: alteracoes em validacao -> Q limpo=%d, Q erro=%d.",
            q_cleared,
            q_erro,
        )
        _emit_delta_change_logs("Delta PowerRev ALTERACAO", changed_details, summary_threshold=3)
    else:
        logger.info(
            "Delta PowerRev: linhas avaliadas=%d, com_match=%d, alteradas=0, sem_alteracao=%d, sem_match=%d",
            rows_evaluated,
            rows_matched,
            rows_unchanged,
            rows_no_match,
        )

    new_count = sum(
        1 for idx, inv in enumerate(all_invoices)
        if str(inv.get("uc", "")).strip()
        and str(inv.get("referenceMonth", "")).strip() in months_to_check_set
        and idx not in consumed_invoices
    )

    if new_count:
        logger.info(
            "Delta PowerRev: %d faturas sem linha (incluidas no proximo full sync)",
            new_count,
        )

    del compact_sheet_rows, inv_primary_map, inv_fallback_map, inv_secondary_map, consumed_invoices, updates, all_invoices, existing_rows, changed_details
    force_free_memory()

def _delta_clickup_update(ws, headers: list[str], updated_tasks: list[dict]) -> None:
    """
    Atualiza campos do ClickUp nas linhas existentes.
    Toca colunas ClickUp e fim de operação, preserva o resto.
    """
    if not updated_tasks:
        return

    task_id_col = _header_index(headers, "Task ID")
    mes_ref_col = _header_index(headers, "Mês de Referencia", "Mes de Referencia", "MÃƒÂªs de Referencia")
    envio_col = _header_index(headers, "Envio do boleto")
    data_venc_col = _header_index(headers, "Data de Vencimento")

    existing_rows = read_all_rows(ws)
    if not existing_rows:
        return

    # Mapear task_id para linhas na planilha
    task_rows: dict[str, list[int]] = {}
    row_by_sheet: dict[int, list[str]] = {}
    mes_ref_by_row: dict[int, str] = {}
    for i, row in enumerate(existing_rows):
        sheet_row = i + 2
        row_by_sheet[sheet_row] = row
        raw_task_value = str(row[task_id_col]).strip() if len(row) > task_id_col else ""
        tid = _extract_task_id_from_link(raw_task_value)
        if tid:
            task_rows.setdefault(tid, []).append(sheet_row)
        mes_ref_by_row[sheet_row] = str(row[mes_ref_col]).strip() if len(row) > mes_ref_col else ""

    # Colunas ClickUp a atualizar
    status_col = _header_index(headers, "Status Detalhado")
    uc_col = _header_index(headers, "UC")
    razao_col = _header_index(headers, "Razão Social", "Razao Social", "RazÃƒÂ£o Social")
    plano_col = _header_index(headers, "Plano de Adesão", "Plano de Adesao", "Plano de AdesÃƒÂ£o")
    dist_col = _header_index(headers, "Distribuidora")
    tipo_col = _header_index(headers, "Tipo de faturamento")
    obs_col = _header_index(headers, "Observações ClickUp", "Observacoes ClickUp", "ObservaÃƒÂ§ÃƒÂµes ClickUp")
    fim_operacao_col = COLUMN_ORDER.index("parentesco_agrupado") if "parentesco_agrupado" in COLUMN_ORDER else None

    clickup_cols = {
        "status": status_col,
        "uc": uc_col,
        "razao_social": razao_col,
        "plano": plano_col,
        "distribuidora": dist_col,
        "tipo_faturamento": tipo_col,
    }

    updates: dict[int, dict[int, str]] = {}
    changed_details: list[tuple[str, str]] = []
    evaluated_rows = 0
    unchanged_rows = 0

    for task in updated_tasks:
        tid = task.get("id", "")
        rows_for_task = task_rows.get(tid, [])
        if not rows_for_task:
            continue

        # Extrair valores atuais dos campos ClickUp
        base_values: dict[int, str] = {}
        for key, col_idx in clickup_cols.items():
            val = _extract_field_value(task, key)
            # Requisito: valores vazios também devem atualizar (limpar célula).
            base_values[col_idx] = val if val is not None else ""

        # Observações: campo computed, precisa de lógica própria
        obs_val = _build_observacoes(task)
        base_values[obs_col] = obs_val  # sempre atualizar (pode limpar)
        if fim_operacao_col is not None:
            base_values[fim_operacao_col] = get_fim_operacao_display(task)

        if base_values:
            for sheet_row in rows_for_task:
                evaluated_rows += 1
                row_values = dict(base_values)
                mes_label = mes_ref_by_row.get(sheet_row, "")
                row_values[envio_col] = compute_envio_boleto_for_task(task, mes_label)
                row_values[data_venc_col] = compute_data_vencimento_for_task(task, mes_label)

                current_row = row_by_sheet.get(sheet_row, [])
                effective, diffs = _build_effective_updates(
                    row=current_row,
                    proposed=row_values,
                    headers=headers,
                )
                if not effective:
                    unchanged_rows += 1
                    continue

                updates[sheet_row] = effective
                log_uc = _get_row_cell_value(current_row, uc_col) or str(effective.get(uc_col, "")).strip()
                log_mes = mes_label or _get_row_cell_value(current_row, mes_ref_col)
                detail_text = (
                    f"linha={sheet_row} | UC={log_uc or '[vazio]'} | MesRef={log_mes or '[vazio]'} | "
                    + "; ".join(diffs)
                )
                changed_details.append((log_uc or "[vazio]", detail_text))

    if updates:
        update_columns_in_place(ws, updates)
        logger.info(
            "Delta ClickUp: linhas avaliadas=%d, alteradas=%d, sem_alteracao=%d",
            evaluated_rows,
            len(updates),
            unchanged_rows,
        )
        _emit_delta_change_logs("Delta ClickUp ALTERACAO", changed_details, summary_threshold=3)
    else:
        logger.info(
            "Delta ClickUp: linhas avaliadas=%d, alteradas=0, sem_alteracao=%d",
            evaluated_rows,
            unchanged_rows,
        )

    del existing_rows, task_rows, row_by_sheet, mes_ref_by_row, updates, changed_details
    force_free_memory()

def _normalize_int_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def _normalize_money_compare(value: object) -> str:
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
        return cleaned


def _get_row_cell_value(row: list[str], col_idx: int) -> str:
    if col_idx < 0 or len(row) <= col_idx:
        return ""
    return str(row[col_idx]).strip()


def _format_log_value(value: object) -> str:
    text = str(value or "").strip()
    return text if text else "[vazio]"


def _build_effective_updates(
    *,
    row: list[str],
    proposed: dict[int, object],
    headers: list[str],
    money_cols: set[int] | None = None,
) -> tuple[dict[int, object], list[str]]:
    """Mantem apenas mudancas reais e retorna texto de diff para log."""
    effective: dict[int, object] = {}
    diffs: list[str] = []
    money_cols = money_cols or set()

    for col_idx, new_val in proposed.items():
        old_text = _get_row_cell_value(row, col_idx)
        new_text = str(new_val or "").strip()

        if col_idx in money_cols:
            same = _normalize_money_compare(old_text) == _normalize_money_compare(new_text)
        else:
            same = old_text == new_text

        if same:
            continue

        effective[col_idx] = new_val
        col_name = headers[col_idx] if 0 <= col_idx < len(headers) else f"C{col_idx + 1}"
        diffs.append(f"{col_name}: {_format_log_value(old_text)} -> {_format_log_value(new_text)}")

    return effective, diffs


def _emit_delta_change_logs(prefix: str, details: list[tuple[str, str]], summary_threshold: int = 3) -> None:
    """
    Emite logs detalhados de alteracao no delta.
    Se uma UC tiver mais de `summary_threshold` alteracoes, loga apenas resumo por UC.
    """
    if not details:
        return

    by_uc: dict[str, list[str]] = defaultdict(list)
    uc_order: list[str] = []
    for uc, detail in details:
        uc_key = str(uc or "[vazio]").strip() or "[vazio]"
        if uc_key not in by_uc:
            uc_order.append(uc_key)
        by_uc[uc_key].append(detail)

    for uc in uc_order:
        uc_details = by_uc.get(uc, [])
        if len(uc_details) > summary_threshold:
            logger.info("%s | UC %s teve mudanca em diversas linhas (%d).", prefix, uc, len(uc_details))
            continue
        for detail in uc_details:
            logger.info("%s | %s", prefix, detail)


def _recompute_mes_atendimento(ws, headers: list[str]) -> int:
    """Recalcula coluna de Mês de atandimento com base em UC + Mês de Referencia."""
    try:
        uc_col = _header_index(headers, "UC")
        mes_ref_col = _header_index(headers, "MÃƒÂªs de Referencia", "Mes de Referencia", "MÃƒÆ’Ã‚Âªs de Referencia")
        mes_at_col = _header_index(
            headers,
            "MÃƒÂªs de atandimento",
            "Mes de atandimento",
            "Mês de atandimento",
            "Mes de atendimento",
            "Mês de atendimento",
        )
    except Exception:
        return 0

    rows = read_all_rows(ws)
    if not rows:
        return 0

    months_by_uc: dict[str, set[str]] = defaultdict(set)
    row_refs: list[tuple[int, str, str, str]] = []

    for idx, row in enumerate(rows):
        sheet_row = idx + 2
        uc = str(row[uc_col]).strip() if len(row) > uc_col else ""
        mes_label = str(row[mes_ref_col]).strip() if len(row) > mes_ref_col else ""
        ref_ym = label_to_yyyymm(mes_label)
        if not uc or not ref_ym:
            continue
        months_by_uc[uc].add(ref_ym)
        current = str(row[mes_at_col]).strip() if len(row) > mes_at_col else ""
        row_refs.append((sheet_row, uc, ref_ym, current))

    if not row_refs:
        return 0

    rank_by_uc: dict[str, dict[str, int]] = {}
    for uc, months in months_by_uc.items():
        sorted_months = sorted(months)
        rank_by_uc[uc] = {ym: i + 1 for i, ym in enumerate(sorted_months)}

    updates: dict[int, dict[int, str]] = {}
    changed_details: list[tuple[str, str]] = []
    for sheet_row, uc, ref_ym, current in row_refs:
        expected = str(rank_by_uc[uc][ref_ym])
        if _normalize_int_text(current) != expected:
            updates.setdefault(sheet_row, {})[mes_at_col] = expected
            detail_text = (
                f"linha={sheet_row} | UC={uc or '[vazio]'} | MesRef={yyyymm_to_label(ref_ym) or '[vazio]'} | "
                f"{headers[mes_at_col]}: {_format_log_value(current)} -> {_format_log_value(expected)}"
            )
            changed_details.append((uc or "[vazio]", detail_text))

    if updates:
        update_columns_in_place(ws, updates)
        logger.info("Delta: %d linhas com Mês de atandimento recalculado.", len(updates))
        _emit_delta_change_logs("Delta MesAtendimento ALTERACAO", changed_details, summary_threshold=3)

    return len(updates)

def _merge_with_disappeared(
    new_rows: list[list[str]],
    ws,
    headers: list[str],
    uc_to_tasks: dict[str, list[dict]] | None = None,
    blocked_task_ids: set[str] | None = None,
) -> tuple[list[list[str]], dict[int, dict[int, str]], list[list[str]]]:
    """
    Preserva linhas cujo invoice sumiu da PowerRev.
    "Validação" = "Erro no sistema" para desaparecidos.
    "Validação" de reaparecidos já é "" (escrito por build_row/write_all_rows).

    Retorna (merged_rows, q_marks_desaparecidos, existing_rows).
    existing_rows é repassado a write_all_rows para evitar leitura duplicada.
    """
    from collections import defaultdict, deque
    from sheets_manager import (
        WRITE_COL_COUNT,
        _derive_invoice_id_from_row,
        _fallback_key,
        _primary_key,
    )

    uc_idx = _header_index(headers, "UC")
    mes_idx = _header_index(headers, "Mês de Referencia", "Mes de Referencia", "MÃªs de Referencia")
    val_idx = _header_index(headers, "Validação", "Validacao", "ValidaÃ§Ã£o")
    status_fat_idx = _header_index(headers, "Status de faturamento")
    status_det_idx = _header_index(headers, "Status Detalhado")
    task_id_idx = _header_index(headers, "Task ID") if _has_header(headers, "Task ID") else None
    valor_idx = _header_index(headers, "Valor do boleto")
    obs_idx = _header_index(headers, "Observações", "Observacoes", "ObservaÃ§Ãµes")
    val_final_idx = _header_index(headers, "Valor final")
    emiss_final_idx = _header_index(headers, "Data de emissão final", "Data de emissao final", "Data de emissÃ£o final")

    invoice_idx = _header_index(headers, "Invoice ID") if _has_header(headers, "Invoice ID") else None
    provider_idx = _header_index(headers, "Provider") if _has_header(headers, "Provider") else None
    issue_idx = (
        _header_index(headers, "Data de Emissão da fatura", "Data de Emissao da fatura", "Data de EmissÃ£o da fatura")
        if _has_header(headers, "Data de Emissão da fatura", "Data de Emissao da fatura", "Data de EmissÃ£o da fatura")
        else None
    )
    parent_idx = COLUMN_ORDER.index("parentesco_agrupado") if "parentesco_agrupado" in COLUMN_ORDER else None

    def _row_value_local(row: list[str], idx: int | None) -> str:
        if idx is None or idx < 0 or len(row) <= idx:
            return ""
        return str(row[idx]).strip()

    def _money_token(value: object) -> str:
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
            return cleaned

    def _secondary_key_local(row: list[str], *, yyyymm: str) -> str:
        uc = _row_value_local(row, uc_idx)
        if not uc or not yyyymm:
            return ""
        issue = _row_value_local(row, issue_idx)
        provider = _row_value_local(row, provider_idx)
        status = _row_value_local(row, status_fat_idx)
        total = _money_token(_row_value_local(row, valor_idx))
        return "|".join([
            "SKD",
            uc,
            yyyymm,
            issue,
            provider,
            status,
            total,
        ])

    def _row_keys(row: list[str]) -> tuple[str, str, str, str]:
        uc = _row_value_local(row, uc_idx)
        mes = _row_value_local(row, mes_idx)
        if not uc or not mes:
            return "", "", "", ""
        invoice_id = _derive_invoice_id_from_row(
            row,
            uc_col=uc_idx,
            invoice_id_col=invoice_idx,
            provider_col=provider_idx,
            issue_col=issue_idx,
            parent_col=parent_idx,
        )
        yyyymm = label_to_yyyymm(mes)
        secondary_key = _secondary_key_local(row, yyyymm=yyyymm)
        return _fallback_key(uc, mes), _primary_key(uc, mes, invoice_id), invoice_id, secondary_key

    existing = read_all_rows(ws)
    if not existing:
        return new_rows, {}, existing

    new_uc_mes_keys: set[tuple[str, str]] = set()
    for row in new_rows:
        new_uc = _row_value_local(row, uc_idx)
        new_mes = _row_value_local(row, mes_idx)
        if new_uc and new_mes:
            new_uc_mes_keys.add((new_uc, new_mes))

    def _is_within_uc_period(uc: str, yyyymm: str) -> bool:
        if not uc_to_tasks:
            return True
        task_list = uc_to_tasks.get(uc)
        if not task_list or not yyyymm:
            return True

        if _should_use_transition_resolution(task_list):
            return any(_task_covers_reference_month(t, yyyymm) for t in task_list)

        inicio_global: datetime | None = None
        fim_global: datetime | None = None
        has_open_end = False

        for t in task_list:
            inicio = get_inicio_operacao(t)
            fim = get_fim_operacao(t)

            if inicio is not None:
                if inicio_global is None or inicio < inicio_global:
                    inicio_global = inicio

            if fim is None:
                has_open_end = True
            else:
                if fim_global is None or fim > fim_global:
                    fim_global = fim

        if has_open_end:
            fim_global = None

        if inicio_global is not None:
            inicio_ym = f"{inicio_global.year}{inicio_global.month:02d}"
            if yyyymm < inicio_ym:
                return False

        if fim_global is not None:
            fim_ym = f"{fim_global.year}{fim_global.month:02d}"
            if yyyymm > fim_ym:
                return False

        return True

    # comentario normalizado
    new_primary_map: dict[str, deque[int]] = defaultdict(deque)
    new_fallback_map: dict[str, deque[int]] = defaultdict(deque)
    new_fallback_any_map: dict[str, deque[int]] = defaultdict(deque)
    new_secondary_map: dict[str, deque[int]] = defaultdict(deque)
    for idx, row in enumerate(new_rows):
        fallback_key, primary_key, _, secondary_key = _row_keys(row)
        if primary_key:
            new_primary_map[primary_key].append(idx)
        if fallback_key and (not primary_key):
            new_fallback_map[fallback_key].append(idx)
        if fallback_key:
            new_fallback_any_map[fallback_key].append(idx)
        if secondary_key:
            new_secondary_map[secondary_key].append(idx)

    consumed_new: set[int] = set()

    def _pop_unmatched(queue_map: dict[str, deque[int]], key: str) -> int | None:
        if not key:
            return None
        q = queue_map.get(key)
        if not q:
            return None
        while q and q[0] in consumed_new:
            q.popleft()
        if not q:
            return None
        idx = q.popleft()
        consumed_new.add(idx)
        return idx

    def _pop_unique_unmatched(queue_map: dict[str, deque[int]], key: str) -> int | None:
        if not key:
            return None
        q = queue_map.get(key)
        if not q:
            return None
        unmatched = [idx for idx in q if idx not in consumed_new]
        if len(unmatched) != 1:
            return None
        idx = unmatched[0]
        consumed_new.add(idx)
        return idx

    disappeared_by_uc: dict[str, list[list[str]]] = {}
    disappeared_row_ids: set[int] = set()
    dropped_redundant_missing = 0
    dropped_planejamento_black = 0
    dropped_blocked_task = 0
    dropped_redundant_placeholder = 0
    blocked_task_ids = {str(tid).strip() for tid in (blocked_task_ids or set()) if str(tid).strip()}

    for row in existing:
        if blocked_task_ids:
            row_task_id = _extract_task_id_from_link(_row_value_local(row, task_id_idx))
            if row_task_id and row_task_id in blocked_task_ids:
                dropped_blocked_task += 1
                continue

        uc = _row_value_local(row, uc_idx)
        mes = _row_value_local(row, mes_idx)
        if not uc or not mes:
            continue

        # Não preservar linhas legadas com Status Detalhado = Planejamento - Black.
        if _is_planejamento_black_text(_row_value_local(row, status_det_idx)):
            dropped_planejamento_black += 1
            continue

        # Compatibilidade retroativa: remover linhas mãe legadas do grouped.
        parent_value = _row_value_local(row, parent_idx)
        if parent_value.upper().startswith("UC M"):
            continue

        fallback_key, primary_key, invoice_id, secondary_key = _row_keys(row)

        # Regra de identidade estrita:
        # - linhas com primary key só casam por primary key
        # - fallback UC|Mes só é permitido para linhas sem primary key
        match_idx = None
        if primary_key:
            match_idx = _pop_unmatched(new_primary_map, primary_key)
            if match_idx is None and invoice_id.startswith("SYN|"):
                match_idx = _pop_unique_unmatched(new_secondary_map, secondary_key)
            if match_idx is None and invoice_id.startswith("SYN|"):
                match_idx = _pop_unique_unmatched(new_fallback_any_map, fallback_key)
        else:
            match_idx = _pop_unique_unmatched(new_secondary_map, secondary_key)
            if match_idx is None:
                match_idx = _pop_unmatched(new_fallback_map, fallback_key)
            if match_idx is None:
                match_idx = _pop_unique_unmatched(new_fallback_any_map, fallback_key)

        if match_idx is not None:
            continue  # linha existente já foi correspondida com linha nova

        # Regra de limpeza:
        # se a linha antiga for "Sem Fatura Distribuidora" e já existir qualquer
        # linha nova para o mesmo UC/mês, descarta a antiga (não preservar como erro).
        if _is_missing_distributor_status(_row_value_local(row, status_fat_idx)) and (uc, mes) in new_uc_mes_keys:
            dropped_redundant_missing += 1
            continue
        # Se havia placeholder (Q='Não processado') e agora existe linha nova para o mesmo UC/mês,
        # descarta a linha antiga para evitar sobra em meses que passaram a ter faturas.
        if _row_value_local(row, val_idx) == _NAO_PROCESSADO and (uc, mes) in new_uc_mes_keys:
            dropped_redundant_placeholder += 1
            continue

        yyyymm = label_to_yyyymm(mes)

        # Regra vigente: nada fora da vigencia deve aparecer na planilha.
        if yyyymm and (not _is_within_uc_period(uc, yyyymm)):
            continue

        preserved = [str(v) for v in row[:WRITE_COL_COUNT]]
        while len(preserved) < WRITE_COL_COUNT:
            preserved.append("")
        preserved[val_idx] = ""  # q_marks escreve o status de erro depois
        disappeared_by_uc.setdefault(uc, []).append(preserved)
        disappeared_row_ids.add(id(preserved))

    if dropped_redundant_missing:
        logger.info(
            "Regra MDI (merge): removidas %d linhas antigas redundantes de 'Sem Fatura Distribuidora'.",
            dropped_redundant_missing,
        )
    if dropped_planejamento_black:
        logger.info(
            "Regra Status (merge): removidas %d linhas antigas de '%s'.",
            dropped_planejamento_black,
            _STATUS_PLANEJAMENTO_BLACK_LABEL,
        )
    if dropped_blocked_task:
        logger.info(
            "Regra Status (merge): removidas %d linhas antigas por Task ID bloqueada.",
            dropped_blocked_task,
        )
    if dropped_redundant_placeholder:
        logger.info(
            "Regra Placeholder (merge): removidas %d linhas antigas de 'Nao processado' substituidas por linha nova.",
            dropped_redundant_placeholder,
        )

    if not disappeared_row_ids:
        return new_rows, {}, existing

    logger.warning(
        "Integridade PowerRev: %d linhas sumiram - preservando com '%s' em Q.",
        len(disappeared_row_ids), _ERRO_SISTEMA,
    )

    rows_by_uc: dict[str, list[list[str]]] = {}
    uc_order: list[str] = []
    for row in new_rows:
        uc = _row_value_local(row, uc_idx)
        if uc and uc not in rows_by_uc:
            uc_order.append(uc)
        rows_by_uc.setdefault(uc, []).append(row)

    for uc, dis_rows in disappeared_by_uc.items():
        combined = rows_by_uc.get(uc, []) + dis_rows
        combined.sort(key=lambda r: label_to_yyyymm(str(r[mes_idx]).strip()) if len(r) > mes_idx else "")
        rows_by_uc[uc] = combined
        if uc not in uc_order:
            uc_order.append(uc)

    merged: list[list[str]] = []
    for uc in uc_order:
        merged.extend(rows_by_uc.get(uc, []))

    q_marks: dict[int, dict[int, str]] = {}
    for i, row in enumerate(merged):
        if id(row) in disappeared_row_ids:
            q_marks[i + 2] = {val_idx: _ERRO_SISTEMA}

    return merged, q_marks, existing


def full_sync() -> None:
    global _known_task_ids
    stats.reset()
    begin_memory_cycle("FULL SYNC")
    _refresh_distributed_lock_if_needed(force=True)

    logger.info("=== FULL SYNC inicio ===")
    log_memory("FULL SYNC início")
    t0 = time.time()

    # 1. Fetch ClickUp (já slim)
    tasks = fetch_all_tasks(include_closed=True, transform=slim_task)
    tasks, blocked_black_count, blocked_black_ids = _filter_out_planejamento_black(tasks)
    tasks, blocked_never_joined_count, blocked_never_joined_ids = _filter_out_never_joined_terminated(tasks)
    blocked_task_ids_full = set(blocked_black_ids) | set(blocked_never_joined_ids)
    logger.info("Total tasks recebidas: %d", len(tasks))
    if blocked_black_count:
        logger.info(
            "Filtro Status Detalhado: %d tasks ignoradas por '%s'.",
            blocked_black_count,
            _STATUS_PLANEJAMENTO_BLACK_LABEL,
        )
    if blocked_never_joined_count:
        logger.info(
            "Filtro Status Detalhado: %d tasks ignoradas por status terminal sem início/fim de operação.",
            blocked_never_joined_count,
        )
    log_memory("Pós-fetch ClickUp (slim)")

    _known_task_ids = {t.get("id", "") for t in tasks if t.get("id")}

    # 2. Mapa UC → task
    uc_to_task = _build_uc_task_map(tasks)
    logger.info("UCs mapeadas do ClickUp (listas): %d", len(uc_to_task))

    # 3. Fallback: buscar tasks do workspace inteiro que tenham UC preenchida
    #    Cobre tasks que não aparecem via Get Tasks das 3 listas.
    #    Filtra por list.id no código para garantir que só aceita tasks das listas permitidas.
    from field_map import FIELD_MAP
    from config import CLICKUP_LIST_IDS
    uc_cf_id = FIELD_MAP["uc"]["cf_id"]
    allowed_lists = set(CLICKUP_LIST_IDS)

    # Busca SEM slim_task para ter acesso ao campo list.id
    fallback_count = 0
    fallback_blocked_black_count = 0
    fallback_blocked_never_joined_count = 0
    fallback_seen = 0
    for t in iter_team_tasks_with_uc(uc_cf_id):
        fallback_seen += 1
        if fallback_seen % 500 == 0:
            _refresh_distributed_lock_if_needed()
        # Só aceitar tasks cuja home list está nas listas permitidas
        task_list_id = t.get("list", {}).get("id", "")
        if task_list_id not in allowed_lists:
            continue
        if _is_planejamento_black(t):
            fallback_blocked_black_count += 1
            tid = str(t.get("id", "")).strip()
            if tid:
                blocked_task_ids_full.add(tid)
            continue
        if _is_never_joined_terminated_task(t):
            fallback_blocked_never_joined_count += 1
            tid = str(t.get("id", "")).strip()
            if tid:
                blocked_task_ids_full.add(tid)
            continue

        uc = extract_task_uc(t)
        if not uc:
            continue

        slimmed = slim_task(t)
        tid = t.get("id", "")

        if uc not in uc_to_task:
            uc_to_task[uc] = [slimmed]
            _known_task_ids.add(tid)
            fallback_count += 1
        else:
            # comentario normalizado
            existing_ids = {et.get("id", "") for et in uc_to_task[uc]}
            if tid not in existing_ids:
                uc_to_task[uc].append(slimmed)
                _known_task_ids.add(tid)
                fallback_count += 1
    force_free_memory()
    if fallback_count:
        logger.info("Fallback ClickUp: %d tasks extras recuperadas do workspace.", fallback_count)
    if fallback_blocked_black_count:
        logger.info(
            "Fallback ClickUp: %d tasks ignoradas por '%s'.",
            fallback_blocked_black_count,
            _STATUS_PLANEJAMENTO_BLACK_LABEL,
        )
    if fallback_blocked_never_joined_count:
        logger.info(
            "Fallback ClickUp: %d tasks ignoradas por status terminal sem início/fim de operação.",
            fallback_blocked_never_joined_count,
        )
    logger.info("Fallback ClickUp: %d tasks com UC analisadas (streaming).", fallback_seen)

    # Re-ordenar listas com múltiplas tasks por inicio_operacao
    multi_uc_count = 0
    for uc, tl in uc_to_task.items():
        if len(tl) > 1:
            tl.sort(key=lambda t: get_inicio_operacao(t) or datetime(1900, 1, 1))
            multi_uc_count += 1
    if multi_uc_count:
        logger.info("UCs com múltiplos cards (troca de plano): %d", multi_uc_count)

    logger.info("UCs mapeadas total: %d", len(uc_to_task))

    # 4. Determinar range de meses
    start_ym, end_ym = _get_powerrev_date_range(tasks)
    logger.info("PowerRev: período %s a %s", start_ym, end_ym)

    del tasks
    force_free_memory()
    log_memory("Pós-build UC map + fallback + gc")

    # 5. Fetch PowerRev agrupado por UC
    if POWERREV_BASE_URL:
        allowed_ucs = set(uc_to_task.keys())
        uc_periods = _build_uc_periods(uc_to_task)
        _refresh_distributed_lock_if_needed(force=True)
        uc_invoices = _fetch_invoices_grouped(
            start_ym,
            end_ym,
            allowed_ucs=allowed_ucs,
            uc_periods=uc_periods,
        )
        _refresh_distributed_lock_if_needed(force=True)
        log_memory("Pós-fetch PowerRev")
    else:
        logger.warning("PowerRev nao configurada - nenhuma linha sera gerada.")
        uc_invoices = {}

    if not uc_invoices:
        logger.warning("Nenhum invoice retornado pela PowerRev.")
        ws = get_worksheet()
        ensure_headers(ws)
        headers = get_headers()
        # Sem dados novos, mas preservar linhas existentes como "Erro no sistema"
        rows_empty: list[list[str]] = []
        rows_empty, q_marks, existing_rows = _merge_with_disappeared(
            rows_empty,
            ws,
            headers,
            uc_to_task,
            blocked_task_ids=blocked_task_ids_full,
        )
        if rows_empty:
            write_all_rows(
                ws,
                rows_empty,
                existing_data_rows=existing_rows,
            )
            if q_marks:
                update_columns_in_place(ws, q_marks)
                logger.info("Integridade: %d marcações Q aplicadas.", len(q_marks))
        force_free_memory()
        log_sync_stats("FULL SYNC (sem dados novos)")
        end_memory_cycle("FULL SYNC")
        return

    # 6. Montar linhas (invoice = linha, enriquecido com ClickUp) + placeholders
    rows, placeholder_keys = _build_rows_from_invoices(uc_invoices, uc_to_task)
    logger.info("Total linhas geradas: %d (incl. %d placeholders)", len(rows), len(placeholder_keys))

    # 6. Escrever (com proteção de integridade)
    ws = get_worksheet()
    ensure_headers(ws)
    headers = get_headers()

    # Detectar invoices que sumiram e preservar suas linhas
    rows_before = len(rows)
    rows, q_marks, existing_rows = _merge_with_disappeared(
        rows,
        ws,
        headers,
        uc_to_task,
        blocked_task_ids=blocked_task_ids_full,
    )

    del uc_invoices, uc_to_task
    force_free_memory()
    log_memory("Pós-build rows + gc")

    if len(rows) > rows_before:
        logger.info("Total linhas após merge com desaparecidos: %d (+%d preservadas)",
                     len(rows), len(rows) - rows_before)

    # Adicionar marcações Q para placeholders
    if placeholder_keys:
        val_idx = _header_index(headers, "Validação", "Validacao", "ValidaÃ§Ã£o")
        uc_idx = _header_index(headers, "UC")
        mes_idx = _header_index(headers, "Mês de Referencia", "Mes de Referencia", "MÃªs de Referencia")
        for i, row in enumerate(rows):
            uc = str(row[uc_idx]).strip() if len(row) > uc_idx else ""
            mes = str(row[mes_idx]).strip() if len(row) > mes_idx else ""
            if (uc, mes) in placeholder_keys:
                q_marks[i + 2] = {val_idx: _NAO_PROCESSADO}

    write_all_rows(
        ws,
        rows,
        existing_data_rows=existing_rows,
    )
    _refresh_distributed_lock_if_needed(force=True)

    # Aplicar marcações na coluna Q (Validação)
    if q_marks:
        update_columns_in_place(ws, q_marks)
        logger.info("Marcações Q aplicadas: %d", len(q_marks))

    elapsed = time.time() - t0
    logger.info(
        "=== FULL SYNC concluido em %.1fs - %d linhas, %d tasks ===",
        elapsed, len(rows), len(_known_task_ids),
    )

    del rows
    force_free_memory()

    log_sync_stats("FULL SYNC")
    log_memory("Pós-gc final")
    end_memory_cycle("FULL SYNC")


def delta_sync(last_updated_ts: int) -> int:
    global _known_task_ids
    stats.reset()
    begin_memory_cycle("DELTA SYNC")
    _refresh_distributed_lock_if_needed(force=True)
    now_ms = int(time.time() * 1000)

    tasks = fetch_all_tasks(
        include_closed=True,
        date_updated_gt=last_updated_ts,
        transform=slim_task,
    )
    tasks, blocked_black_count, blocked_black_ids = _filter_out_planejamento_black(tasks)
    tasks, blocked_never_joined_count, blocked_never_joined_ids = _filter_out_never_joined_terminated(tasks)

    ws = get_worksheet()
    ensure_headers(ws)
    headers = get_headers()

    if blocked_black_count:
        logger.info(
            "Delta ClickUp: %d tasks ignoradas por '%s'.",
            blocked_black_count,
            _STATUS_PLANEJAMENTO_BLACK_LABEL,
        )
        removed_black_rows = _remove_rows_by_task_ids(ws, headers, blocked_black_ids)
        if removed_black_rows:
            logger.info(
                "Delta ClickUp: %d linhas removidas da planilha por '%s'.",
                removed_black_rows,
                _STATUS_PLANEJAMENTO_BLACK_LABEL,
            )
    if blocked_never_joined_count:
        logger.info(
            "Delta ClickUp: %d tasks ignoradas por status terminal sem início/fim de operação.",
            blocked_never_joined_count,
        )
        removed_never_joined_rows = _remove_rows_by_task_ids(ws, headers, blocked_never_joined_ids)
        if removed_never_joined_rows:
            logger.info(
                "Delta ClickUp: %d linhas removidas da planilha por status terminal sem início/fim de operação.",
                removed_never_joined_rows,
            )

    if not _known_task_ids:
        try:
            _rehydrate_known_task_ids_from_sheet()
        except Exception:
            logger.exception("Delta: falha ao reidratar _known_task_ids da planilha.")

    # comentario normalizado
    if tasks:
        new_tasks = []
        updated_tasks = []
        for t in tasks:
            tid = t.get("id", "")
            if tid in _known_task_ids:
                updated_tasks.append(t)
            else:
                new_tasks.append(t)
                _known_task_ids.add(tid)

        logger.info(
            "Delta ClickUp: %d atualizadas, %d novas, %d modificadas",
            len(tasks), len(new_tasks), len(updated_tasks),
        )
        del tasks

        # Atualizar campos ClickUp em linhas existentes
        if updated_tasks:
            _delta_clickup_update(ws, headers, updated_tasks)

        # Tasks novas: não geram linhas até ter invoice (próximo full sync)
        if new_tasks:
            logger.info(
                "Delta ClickUp: %d tasks novas (linhas criadas no próximo full sync)",
                len(new_tasks),
            )

        del updated_tasks, new_tasks
    else:
        del tasks

    # comentario normalizado
    if POWERREV_BASE_URL:
        _delta_powerrev_check(ws, headers)

    # Garantir consistencia caso UC/Mes mudem durante delta.
    _recompute_mes_atendimento(ws, headers)

    del ws, headers
    log_sync_stats("DELTA SYNC")
    force_free_memory()
    log_memory("DELTA pós-gc final")
    end_memory_cycle("DELTA SYNC")

    return now_ms


def _reset_all_sessions(reason: str = "") -> None:
    """Reset completo de todas as sessions/clients HTTP."""
    from powerrev_client import reset_session as reset_powerrev_session, reset_caches as reset_powerrev_caches
    prefix = f" ({reason})" if reason else ""
    logger.warning("Reset total de sessions%s", prefix)
    reset_clickup_session()
    reset_powerrev_session()
    reset_powerrev_caches()
    reset_sheets_client()
    force_free_memory()


def main() -> None:
    global _shutdown_requested

    logger.info("Faturamento Sync iniciando (PID %d)...", os.getpid())
    log_memory("Boot")

    try:
        _acquire_single_instance_lock()
        atexit.register(_release_single_instance_lock)
    except RuntimeError as exc:
        logger.error(str(exc))
        return

    if not _acquire_distributed_lock():
        _release_single_instance_lock()
        return
    atexit.register(_release_distributed_lock)

    logger.info(
        "Agendamento de full diario: %02d:%02d (%s)",
        _FULL_SYNC_DAILY_HOUR,
        _FULL_SYNC_DAILY_MINUTE,
        FULL_SYNC_TIMEZONE,
    )

    try:
        _refresh_distributed_lock_if_needed(force=True)
        _rehydrate_known_task_ids_from_sheet()
    except Exception:
        logger.exception("Falha ao reidratar _known_task_ids da planilha.")

    if _shutdown_requested:
        logger.info("Shutdown antes do full sync inicial.")
        return
    if not _run_full_sync_until_success(
        reason="inicial",
    ):
        logger.info("Shutdown durante retries do full inicial.")
        return

    next_full_ts = _next_full_sync_timestamp()
    logger.info("Proximo full diario agendado para %s.", _format_local_dt(next_full_ts))

    last_delta_ts = int(time.time() * 1000)
    consecutive_errors = 0
    cycle_count = 0
    boot_time = time.time()

    while not _shutdown_requested:
        try:
            _interruptible_sleep(DELTA_SYNC_INTERVAL_S)
            if _shutdown_requested:
                break

            now = time.time()
            cycle_count += 1
            _refresh_distributed_lock_if_needed()

            if cycle_count % 10 == 0:
                uptime_h = (now - boot_time) / 3600
                logger.info(
                    "Heartbeat - ciclo %d, uptime %.1fh, RSS %.1f MB, erros consecutivos: %d",
                    cycle_count,
                    uptime_h,
                    stats.get_memory_mb_safe(),
                    consecutive_errors,
                )

            if now >= next_full_ts:
                logger.info("Janela de full diario atingida (%s).", _format_local_dt(now))
                ok = _run_full_sync_until_success(
                    reason="diario",
                )
                if not ok:
                    break

                next_full_ts = _next_full_sync_timestamp()
                logger.info("Proximo full diario agendado para %s.", _format_local_dt(next_full_ts))
                last_delta_ts = int(time.time() * 1000)
                consecutive_errors = 0
                continue

            last_delta_ts = delta_sync(last_delta_ts)
            consecutive_errors = 0

        except KeyboardInterrupt:
            logger.info("Ctrl+C - encerrando.")
            break

        except MemoryError:
            consecutive_errors += 1
            end_memory_cycle("DELTA SYNC")
            logger.critical(
                "MemoryError no ciclo %d (erro consecutivo #%d).",
                cycle_count,
                consecutive_errors,
            )
            force_free_memory()
            _reset_all_sessions("MemoryError_loop")
            _interruptible_sleep(60)

        except Exception:
            consecutive_errors += 1
            end_memory_cycle("DELTA SYNC")
            backoff = min(
                _ERROR_BACKOFF_BASE * (2 ** (consecutive_errors - 1)),
                _ERROR_BACKOFF_MAX,
            )
            logger.exception(
                "Erro no ciclo %d (erro consecutivo #%d), retry em %ds...",
                cycle_count,
                consecutive_errors,
                backoff,
            )
            if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                logger.warning(
                    "Atingiu %d erros consecutivos - reset total de sessions.",
                    consecutive_errors,
                )
                _reset_all_sessions("escalation por erros consecutivos")
                consecutive_errors = 0

            _interruptible_sleep(backoff)

    logger.info(
        "Shutdown graceful - %d ciclos executados, uptime %.1fh",
        cycle_count,
        (time.time() - boot_time) / 3600,
    )
    _release_single_instance_lock()
    _release_distributed_lock()
    log_memory("Shutdown")

def _interruptible_sleep(seconds: float) -> None:
    """Sleep que pode ser interrompido por SIGTERM."""
    end = time.time() + seconds
    next_lock_refresh = time.time() + max(10, DISTRIBUTED_LOCK_REFRESH_S)
    while time.time() < end and not _shutdown_requested:
        if DISTRIBUTED_LOCK_ENABLED and time.time() >= next_lock_refresh:
            _refresh_distributed_lock_if_needed(force=True)
            next_lock_refresh = time.time() + max(10, DISTRIBUTED_LOCK_REFRESH_S)
        time.sleep(min(1.0, end - time.time()))


if __name__ == "__main__":
    main()








