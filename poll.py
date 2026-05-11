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
    extract_task_uc,
    build_row,
    yyyymm_to_label,
    label_to_yyyymm,
    _extract_field_value,
    _build_observacoes,
    _get_cf_raw,
    _resolve_dropdown_value,
    _compute_envio_boleto,
    _compute_data_vencimento,
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
from field_map import get_headers, COLUMN_ORDER, FIELD_MAP, COMPUTATION_FIELDS, RAZAO_SOCIAL_VENCTO_EXTRA
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

_known_task_ids: set[str] = set()
_FAR_FUTURE = datetime(9999, 1, 1)
_LOCK_FILE_PATH = os.path.join(os.path.dirname(__file__), ".faturamento_sync.lock")
_LOCK_HANDLE = None
_DLOCK_OWNER_ID = f"{socket.gethostname()}:{os.getpid()}:{int(time.time())}:{random.randint(1000,9999)}"
_DLOCK_LAST_REFRESH_TS = 0.0


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
    task_id_col = headers.index("Task ID")
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
_STATUS_CF_ID = "1a5118f7-b9a0-466f-889d-37edd76bd304"


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


def _is_troca_plano(task: dict) -> bool:
    """Detecta se a task está no status 'Encerrado - Troca de Plano'."""
    raw = _get_task_status_raw(task)
    if raw == _STATUS_TROCA_PLANO:
        return True

    # Tentar resolver pelo mapa de dropdowns (id -> nome)
    if raw:
        try:
            resolved = _resolve_dropdown_value(_STATUS_CF_ID, raw)
            if resolved == "Encerrado - Troca de Plano":
                return True
        except Exception:
            pass

    # Se value vier como orderindex (ex: 15), usar type_config.options
    for cf in task.get("custom_fields", []):
        if cf.get("id") != _STATUS_CF_ID:
            continue
        val = cf.get("value")
        options = cf.get("type_config", {}).get("options", [])
        if not options:
            # Fallback: buscar options via API (list/{id}/field)
            try:
                from clickup_client import get_custom_field_options
                options = get_custom_field_options(_STATUS_CF_ID)
            except Exception:
                options = []
        if not options:
            break
        try:
            order = int(val)
        except (TypeError, ValueError):
            order = None
        if order is not None and 0 <= order < len(options):
            name = options[order].get("name", "")
            if name == "Encerrado - Troca de Plano":
                return True
        else:
            # tentar por orderindex/id no array
            for opt in options:
                if str(opt.get("orderindex")) == str(val) or opt.get("id") == str(val):
                    if opt.get("name") == "Encerrado - Troca de Plano":
                        return True
            break

    # Fallback: caso value venha como dict com name (já capturado pelo _get_task_status_raw)
    if raw == "Encerrado - Troca de Plano":
        return True

    return False


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
) -> dict:
    """Dado uma lista de tasks (ordenada por inicio_operacao) e um referenceMonth,
    retorna a task correta para aquele mês.

    Regra: a task é válida para um mês se:
      - inicio_operacao ≤ referenceMonth (ou inicio_operacao ausente)
      - E fim_operacao ≥ referenceMonth (ou fim_operacao ausente)
    Entre as tasks válidas, retorna a com inicio_operacao mais recente.

    Só aplica lógica multi-task se pelo menos uma task tem status
    "Encerrado - Troca de Plano". Caso contrário, retorna a última task (ativa).
    """
    if len(task_list) == 1:
        return task_list[0]

    # Verificar se há troca de plano envolvida
    has_troca = any(_is_troca_plano(t) for t in task_list)
    if not has_troca:
        # comentario normalizado
        return task_list[-1]

    # Converter referenceMonth para comparação
    ref_year = int(reference_ym[:4]) if len(reference_ym) >= 4 else 0
    ref_month = int(reference_ym[4:6]) if len(reference_ym) >= 6 else 0
    ref_date = datetime(ref_year, ref_month, 1) if ref_year and ref_month else None

    if not ref_date:
        return task_list[-1]

    # Encontrar a task cujo período cobre o referenceMonth
    best: dict | None = None
    for task in task_list:
        inicio = get_inicio_operacao(task)
        fim = get_fim_operacao(task)

        # Verificar inicio: se preenchido, deve ser ≤ referenceMonth
        if inicio is not None:
            inicio_ym = datetime(inicio.year, inicio.month, 1)
            if inicio_ym > ref_date:
                continue  # task ainda não começou nesse mês

        # Verificar fim: se preenchido, deve ser ≥ referenceMonth
        if fim is not None:
            fim_ym = datetime(fim.year, fim.month, 1)
            if fim_ym < ref_date:
                continue  # task já encerrou antes desse mês

        # comentario normalizado
        best = task

    if best is not None:
        return best

    # comentario normalizado
    return task_list[-1]


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
    Constrói todas as linhas + placeholders para mês seguinte.
    Retorna (rows, placeholder_keys) onde placeholder_keys = {(uc, mes_label), ...}

    Para UCs com múltiplas tasks (troca de plano), cada invoice é enriquecido
    com a task correta baseado no referenceMonth.
    """

    # Mês do placeholder: atual + 1
    now = datetime.now()
    pm = now.month + 1
    py = now.year
    if pm > 12:
        pm = 1
        py += 1
    placeholder_ym = f"{py}{pm:02d}"
    placeholder_label = yyyymm_to_label(placeholder_ym)

    def uc_sort_key(uc: str):
        task_list = uc_to_tasks.get(uc)
        if task_list:
            # Ordenar pelo inicio_operacao da primeira task
            dt = get_inicio_operacao(task_list[0])
            return dt if dt is not None else _FAR_FUTURE
        return _FAR_FUTURE

    valid_ucs = [uc for uc in uc_invoices if uc in uc_to_tasks]
    sorted_ucs = sorted(valid_ucs, key=uc_sort_key)

    skipped = len(uc_invoices) - len(sorted_ucs)
    if skipped:
        logger.info("PowerRev: %d UCs ignoradas (sem task no ClickUp)", skipped)

    all_rows: list[list[str]] = []
    placeholder_keys: set[tuple[str, str]] = set()

    for uc in sorted_ucs:
        invoices = uc_invoices[uc]
        task_list = uc_to_tasks[uc]

        # Calcular período coberto por todas as tasks da UC
        # inicio_global = menor inicio_operacao de todas as tasks
        # fim_global = maior fim_operacao (None = sem fim = ativa)
        inicio_global: datetime | None = None
        fim_global: datetime | None = None
        has_open_end = False  # alguma task sem fim_operacao (ativa)

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

        # Se alguma task não tem fim, o período é aberto (sem limite superior)
        if has_open_end:
            fim_global = None

        # Filtrar invoices fora do período coberto
        before = len(invoices)
        filtered_invoices = []
        for inv in invoices:
            rm = inv.get("referenceMonth", "")
            if not rm:
                continue
            # Filtrar por inicio_global
            if inicio_global is not None:
                inicio_ym = f"{inicio_global.year}{inicio_global.month:02d}"
                if rm < inicio_ym:
                    continue
            # Filtrar por fim_global
            if fim_global is not None:
                fim_ym = f"{fim_global.year}{fim_global.month:02d}"
                if rm > fim_ym:
                    continue
            filtered_invoices.append(inv)

        dropped = before - len(filtered_invoices)
        if dropped:
            logger.debug(
                "UC %s: %d invoices ignorados (fora do período %s a %s)",
                uc, dropped,
                f"{inicio_global.year}{inicio_global.month:02d}" if inicio_global else "?",
                f"{fim_global.year}{fim_global.month:02d}" if fim_global else "aberto",
            )

        invoices = filtered_invoices
        if not invoices:
            continue

        # Ordenar invoices por referenceMonth
        invoices.sort(key=lambda i: i.get("referenceMonth", ""))

        # Calcular meses únicos para Mês de Atendimento
        unique_months: list[str] = []
        for inv in invoices:
            rm = inv.get("referenceMonth", "")
            if rm and rm not in unique_months:
                unique_months.append(rm)

        # comentario normalizado
        for inv in invoices:
            rm = inv.get("referenceMonth", "")
            mes_atendimento = unique_months.index(rm) + 1 if rm in unique_months else 0
            task = _resolve_task_for_month(task_list, rm)
            row = build_row(task, inv, mes_atendimento)
            all_rows.append(row)

        # Gerar placeholder se necessário
        if fim_global and (fim_global.year, fim_global.month) < (py, pm):
            continue  # cooperado encerrou antes do mês do placeholder

        if placeholder_ym in unique_months:
            continue  # já tem fatura real pra esse mês

        placeholder_invoice = {
            "referenceMonth": placeholder_ym,
            "status": "",
            "issueDate": "",
            "total": 0.0,
        }
        mes_atendimento = len(unique_months) + 1
        # Placeholder usa a task mais recente (ativa)
        task = _resolve_task_for_month(task_list, placeholder_ym)
        row = build_row(task, placeholder_invoice, mes_atendimento)
        all_rows.append(row)
        placeholder_keys.add((uc, placeholder_label))

    if placeholder_keys:
        logger.info("Placeholders: %d linhas '%s' geradas para %s",
                     len(placeholder_keys), _NAO_PROCESSADO, placeholder_label)

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
    uc_col = headers.index("UC")
    mes_col = headers.index("Mês de Referencia")
    status_fat_col = headers.index("Status de faturamento")
    emissao_col = headers.index("Data de Emissão da fatura")
    valor_col = headers.index("Valor do boleto")
    val_col = headers.index("Validação")
    invoice_col = headers.index("Invoice ID") if "Invoice ID" in headers else None
    provider_col = headers.index("Provider") if "Provider" in headers else None
    parent_col = headers.index("Parentesco agrupado") if "Parentesco agrupado" in headers else None

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
        parent_grouped_uc: str,
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
            str(parent_grouped_uc or "").strip(),
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
            parent_grouped_uc=inv.get("parentGroupedUc", ""),
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

    compact_sheet_rows: list[tuple[int, str, str, str, str, str]] = []
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
            parent_grouped_uc=(row[parent_col] if parent_col is not None and len(row) > parent_col else ""),
            issue_date=(row[emissao_col] if len(row) > emissao_col else ""),
            provider_name=(row[provider_col] if provider_col is not None and len(row) > provider_col else ""),
            status=(row[status_fat_col] if len(row) > status_fat_col else ""),
            total=(row[valor_col] if len(row) > valor_col else ""),
        )
        compact_sheet_rows.append((i + 2, uc, ref, current_q, invoice_id, row_secondary))

    del existing_rows

    updates: dict[int, dict[int, str]] = {}
    consumed_invoices: set[int] = set()

    for sheet_row, uc, ref, current_q, invoice_id, row_secondary in compact_sheet_rows:
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
            if current_q != _NAO_PROCESSADO:
                updates.setdefault(sheet_row, {})[val_col] = _ERRO_SISTEMA
            continue

        inv = all_invoices[match_idx]
        col_updates: dict[int, str | float] = {}
        col_updates[status_fat_col] = inv.get("status") or ""
        col_updates[emissao_col] = inv.get("issueDate") or ""
        total_val = inv.get("total")
        col_updates[valor_col] = "" if total_val is None else total_val
        col_updates[val_col] = ""
        updates[sheet_row] = col_updates

    if updates:
        update_columns_in_place(ws, updates)
        q_cleared = sum(1 for cols in updates.values() if val_col in cols and cols[val_col] == "")
        q_erro = sum(1 for cols in updates.values() if val_col in cols and cols[val_col] == _ERRO_SISTEMA)
        logger.info(
            "Delta PowerRev: %d linhas atualizadas (%d Q limpo, %d Q erro).",
            len(updates),
            q_cleared,
            q_erro,
        )

    new_count = sum(
        1 for idx, inv in enumerate(all_invoices)
        if str(inv.get("uc", "")).strip()
        and str(inv.get("referenceMonth", "")).strip() in months_to_check_set
        and idx not in consumed_invoices
    )

    if new_count:
        logger.info(
            "Delta PowerRev: %d faturas sem linha (incluídas no próximo full sync)",
            new_count,
        )

    del compact_sheet_rows, inv_primary_map, inv_fallback_map, inv_secondary_map, consumed_invoices, updates, all_invoices
    force_free_memory()


def _delta_clickup_update(ws, headers: list[str], updated_tasks: list[dict]) -> None:
    """
    Atualiza campos do ClickUp nas linhas existentes.
    Toca colunas ClickUp (B, D, I, J, K, S), preserva o resto.
    """
    if not updated_tasks:
        return

    task_id_col = headers.index("Task ID")
    task_id_values = read_column_values(ws, task_id_col)

    # Mapear task_id para linhas na planilha
    task_rows: dict[str, list[int]] = {}
    for i, cell_value in enumerate(task_id_values):
        tid = _extract_task_id_from_link(str(cell_value))
        if tid:
            task_rows.setdefault(tid, []).append(i + 2)

    # Colunas ClickUp a atualizar
    status_col = headers.index("Status Detalhado")
    razao_col = headers.index("Razão Social")
    plano_col = headers.index("Plano de Adesão")
    dist_col = headers.index("Distribuidora")
    tipo_col = headers.index("Tipo de faturamento")
    obs_col = headers.index("Observações ClickUp")

    clickup_cols = {
        "status": status_col,
        "razao_social": razao_col,
        "plano": plano_col,
        "distribuidora": dist_col,
        "tipo_faturamento": tipo_col,
    }

    updates: dict[int, dict[int, str]] = {}

    for task in updated_tasks:
        tid = task.get("id", "")
        rows_for_task = task_rows.get(tid, [])
        if not rows_for_task:
            continue

        # Extrair valores atuais dos campos ClickUp
        values: dict[int, str] = {}
        for key, col_idx in clickup_cols.items():
            val = _extract_field_value(task, key)
            # Requisito: valores vazios também devem atualizar (limpar célula).
            values[col_idx] = val if val is not None else ""
        # Observações: campo computed, precisa de lógica própria
        obs_val = _build_observacoes(task)
        values[obs_col] = obs_val  # sempre atualizar (pode limpar)

        if values:
            for sheet_row in rows_for_task:
                updates[sheet_row] = values

    if updates:
        update_columns_in_place(ws, updates)
        logger.info("Delta ClickUp: %d linhas atualizadas", len(updates))

    del task_id_values, task_rows, updates
    force_free_memory()


def _merge_with_disappeared(
    new_rows: list[list[str]],
    ws,
    headers: list[str],
    uc_to_tasks: dict[str, list[dict]] | None = None,
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

    uc_idx = headers.index("UC")
    mes_idx = headers.index("Mês de Referencia")
    val_idx = headers.index("Validação")
    status_fat_idx = headers.index("Status de faturamento")
    valor_idx = headers.index("Valor do boleto")
    obs_idx = headers.index("Observações")
    val_final_idx = headers.index("Valor final")
    emiss_final_idx = headers.index("Data de emissão final")

    invoice_idx = headers.index("Invoice ID") if "Invoice ID" in headers else None
    provider_idx = headers.index("Provider") if "Provider" in headers else None
    issue_idx = headers.index("Data de Emissão da fatura") if "Data de Emissão da fatura" in headers else None
    parent_idx = headers.index("Parentesco agrupado") if "Parentesco agrupado" in headers else None

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
        parent = _row_value_local(row, parent_idx)
        issue = _row_value_local(row, issue_idx)
        provider = _row_value_local(row, provider_idx)
        status = _row_value_local(row, status_fat_idx)
        total = _money_token(_row_value_local(row, valor_idx))
        return "|".join([
            "SKD",
            uc,
            yyyymm,
            parent,
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

    def _has_protected_values(row: list[str]) -> bool:
        for idx in (obs_idx, val_final_idx, emiss_final_idx):
            if len(row) > idx and str(row[idx]).strip():
                return True
        return False

    existing = read_all_rows(ws)
    if not existing:
        return new_rows, {}, existing

    def _is_within_uc_period(uc: str, yyyymm: str) -> bool:
        if not uc_to_tasks:
            return True
        task_list = uc_to_tasks.get(uc)
        if not task_list or not yyyymm:
            return True

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

    for row in existing:
        uc = _row_value_local(row, uc_idx)
        mes = _row_value_local(row, mes_idx)
        if not uc or not mes:
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

        yyyymm = label_to_yyyymm(mes)
        has_protected = _has_protected_values(row)

        # Requisito crítico: se L/N/P já teve valor, a linha deve permanecer sempre.
        if (not has_protected) and yyyymm and (not _is_within_uc_period(uc, yyyymm)):
            continue

        preserved = [str(v) for v in row[:WRITE_COL_COUNT]]
        while len(preserved) < WRITE_COL_COUNT:
            preserved.append("")
        preserved[val_idx] = ""  # q_marks escreve o status de erro depois
        disappeared_by_uc.setdefault(uc, []).append(preserved)
        disappeared_row_ids.add(id(preserved))

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
    logger.info("Total tasks recebidas: %d", len(tasks))
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
    fallback_seen = 0
    for t in iter_team_tasks_with_uc(uc_cf_id):
        fallback_seen += 1
        if fallback_seen % 500 == 0:
            _refresh_distributed_lock_if_needed()
        # Só aceitar tasks cuja home list está nas listas permitidas
        task_list_id = t.get("list", {}).get("id", "")
        if task_list_id not in allowed_lists:
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
        rows_empty, q_marks, existing_rows = _merge_with_disappeared(rows_empty, ws, headers, uc_to_task)
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
    rows, q_marks, existing_rows = _merge_with_disappeared(rows, ws, headers, uc_to_task)

    del uc_invoices, uc_to_task
    force_free_memory()
    log_memory("Pós-build rows + gc")

    if len(rows) > rows_before:
        logger.info("Total linhas após merge com desaparecidos: %d (+%d preservadas)",
                     len(rows), len(rows) - rows_before)

    # Adicionar marcações Q para placeholders
    if placeholder_keys:
        val_idx = headers.index("Validação")
        uc_idx = headers.index("UC")
        mes_idx = headers.index("Mês de Referencia")
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

    ws = get_worksheet()
    ensure_headers(ws)
    headers = get_headers()

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




