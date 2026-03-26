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
import gc
import os
import signal
import sys
import time
import logging
from datetime import datetime

from config import (
    FULL_SYNC_INTERVAL_S,
    DELTA_SYNC_INTERVAL_S,
    POWERREV_BASE_URL,
)
from clickup_client import fetch_all_tasks, reset_session as reset_clickup_session
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
    write_all_rows,
    append_rows,
    update_columns_in_place,
    reset_client as reset_sheets_client,
)
from field_map import get_headers, COLUMN_ORDER, FIELD_MAP, COMPUTATION_FIELDS, RAZAO_SOCIAL_VENCTO_EXTRA
from stats import stats, log_memory, log_sync_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("faturamento_sync")

# ── Shutdown graceful via SIGTERM (Railway deploy) ───────
_shutdown_requested = False


def _handle_sigterm(signum, frame):
    global _shutdown_requested
    logger.info("Sinal SIGTERM recebido — shutdown graceful solicitado.")
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_sigterm)
# SIGINT (Ctrl+C) mantém comportamento padrão: levanta KeyboardInterrupt

# ── Constantes ───────────────────────────────────────────
_MAX_CONSECUTIVE_ERRORS = 5  # após N erros seguidos, reset total de sessions
_ERROR_BACKOFF_BASE = 30     # backoff base em segundos
_ERROR_BACKOFF_MAX = 300     # backoff máximo (5 min)
_ERRO_SISTEMA = "Erro no sistema"  # marca em Q para invoices que sumiram da PowerRev
_NAO_PROCESSADO = "Não processado"  # marca em Q para fatura do mês seguinte ainda não gerada

_known_task_ids: set[str] = set()
_FAR_FUTURE = datetime(9999, 1, 1)

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


def _build_uc_task_map(tasks: list[dict]) -> dict[str, dict]:
    """Constrói mapa UC → task. Se houver duplicatas, a última vence."""
    uc_map: dict[str, dict] = {}
    for task in tasks:
        uc = extract_task_uc(task)
        if uc:
            uc_map[uc] = task
    return uc_map


def _get_powerrev_date_range(tasks: list[dict]) -> tuple[str, str]:
    """Determina range de meses para consultar PowerRev baseado em inicio_operacao."""
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


def _fetch_invoices_grouped(
    start_ym: str, end_ym: str,
) -> dict[str, list[dict]]:
    """Busca invoices mês a mês, retorna agrupado por UC."""
    from powerrev_client import fetch_invoices_for_month, _load_consumer_units, reset_caches

    _load_consumer_units()

    uc_invoices: dict[str, list[dict]] = {}

    year = int(start_ym[:4])
    month = int(start_ym[4:6])
    end_year = int(end_ym[:4])
    end_month = int(end_ym[4:6])

    while (year < end_year) or (year == end_year and month <= end_month):
        ref = f"{year}{month:02d}"
        invoices = fetch_invoices_for_month(ref)
        for inv in invoices:
            uc = inv.get("uc", "").strip()
            if uc:
                uc_invoices.setdefault(uc, []).append(inv)
        del invoices
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
    uc_to_task: dict[str, dict],
) -> tuple[list[list[str]], set[tuple[str, str]]]:
    """
    Constrói todas as linhas + placeholders para mês seguinte.
    Retorna (rows, placeholder_keys) onde placeholder_keys = {(uc, mes_label), ...}
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
        task = uc_to_task.get(uc)
        if task:
            dt = get_inicio_operacao(task)
            return dt if dt is not None else _FAR_FUTURE
        return _FAR_FUTURE

    valid_ucs = [uc for uc in uc_invoices if uc in uc_to_task]
    sorted_ucs = sorted(valid_ucs, key=uc_sort_key)

    skipped = len(uc_invoices) - len(sorted_ucs)
    if skipped:
        logger.info("PowerRev: %d UCs ignoradas (sem task no ClickUp)", skipped)

    all_rows: list[list[str]] = []
    placeholder_keys: set[tuple[str, str]] = set()

    for uc in sorted_ucs:
        invoices = uc_invoices[uc]
        task = uc_to_task[uc]

        # Ignorar invoices após fim de operação
        fim = get_fim_operacao(task)
        if fim:
            fim_ym = f"{fim.year}{fim.month:02d}"
            before = len(invoices)
            invoices = [inv for inv in invoices if inv.get("referenceMonth", "") <= fim_ym]
            dropped = before - len(invoices)
            if dropped:
                logger.debug("UC %s: %d invoices ignorados (após fim_operacao %s)", uc, dropped, fim_ym)
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

        # Gerar linhas reais
        for inv in invoices:
            rm = inv.get("referenceMonth", "")
            mes_atendimento = unique_months.index(rm) + 1 if rm in unique_months else 0
            row = build_row(task, inv, mes_atendimento)
            all_rows.append(row)

        # Gerar placeholder se necessário
        if fim and (fim.year, fim.month) < (py, pm):
            continue  # cooperado encerrou antes do mês do placeholder

        if placeholder_ym in unique_months:
            continue  # já tem fatura real pra esse mês

        placeholder_invoice = {
            "referenceMonth": placeholder_ym,
            "status": "",
            "issueDate": "",
            "total": "R$0,00",
        }
        mes_atendimento = len(unique_months) + 1
        row = build_row(task, placeholder_invoice, mes_atendimento)
        all_rows.append(row)
        placeholder_keys.add((uc, placeholder_label))

    if placeholder_keys:
        logger.info("Placeholders: %d linhas '%s' geradas para %s",
                     len(placeholder_keys), _NAO_PROCESSADO, placeholder_label)

    return all_rows, placeholder_keys


def _delta_powerrev_check(ws, headers: list[str]) -> None:
    """
    Checa mês anterior, atual e próximo na PowerRev.
    Atualiza N, O, P em linhas existentes.
    Limpa Q se invoice chegou (resolve "Erro no sistema" e "Não processado").
    """
    from powerrev_client import fetch_invoices_for_month, _load_consumer_units

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

    existing_keys: dict[tuple[str, str], int] = {}
    # Rastrear linhas com Q marcado ("Erro no sistema" ou "Não processado")
    marked_rows: dict[tuple[str, str], int] = {}
    for i, row in enumerate(existing_rows):
        if len(row) > max(uc_col, mes_col):
            uc = str(row[uc_col]).strip()
            mes = str(row[mes_col]).strip()
            yyyymm = label_to_yyyymm(mes)
            if uc and yyyymm:
                existing_keys[(uc, yyyymm)] = i + 2
                val_q = str(row[val_col]).strip() if len(row) > val_col else ""
                if val_q in (_ERRO_SISTEMA, _NAO_PROCESSADO):
                    marked_rows[(uc, yyyymm)] = i + 2

    updates: dict[int, dict[int, str]] = {}
    new_count = 0

    for inv in all_invoices:
        uc = inv.get("uc", "").strip()
        ref = inv.get("referenceMonth", "").strip()
        if not uc or not ref:
            continue

        sheet_row = existing_keys.get((uc, ref))

        if sheet_row is not None:
            col_updates: dict[int, str] = {}
            if inv.get("status"):
                col_updates[status_fat_col] = inv["status"]
            if inv.get("issueDate"):
                col_updates[emissao_col] = inv["issueDate"]
            if inv.get("total"):
                col_updates[valor_col] = inv["total"]
            # Se linha tinha marcação Q e invoice chegou, limpar Q
            if (uc, ref) in marked_rows:
                col_updates[val_col] = ""
            if col_updates:
                updates[sheet_row] = col_updates
        else:
            new_count += 1

    if updates:
        update_columns_in_place(ws, updates)
        # Contar quantas tinham Q limpo
        q_cleared = sum(1 for sr, cols in updates.items() if val_col in cols and cols[val_col] == "")
        if q_cleared:
            logger.info("Delta PowerRev: %d linhas atualizadas (%d com Q limpo)", len(updates), q_cleared)
        else:
            logger.info("Delta PowerRev: %d linhas atualizadas", len(updates))

    if new_count:
        logger.info(
            "Delta PowerRev: %d faturas sem linha (incluídas no próximo full sync)",
            new_count,
        )

    del existing_rows, all_invoices


def _delta_clickup_update(ws, headers: list[str], updated_tasks: list[dict]) -> None:
    """
    Atualiza campos do ClickUp nas linhas existentes.
    Toca colunas ClickUp (B, D, I, J, K, L), preserva o resto.
    """
    if not updated_tasks:
        return

    existing_rows = read_all_rows(ws)
    task_id_col = headers.index("Task ID")

    # Mapear task_id → linhas na planilha
    task_rows: dict[str, list[int]] = {}
    for i, row in enumerate(existing_rows):
        if len(row) > task_id_col:
            tid = _extract_task_id_from_link(str(row[task_id_col]))
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
            if val:
                values[col_idx] = val
        # Observações: campo computed, precisa de lógica própria
        obs_val = _build_observacoes(task)
        values[obs_col] = obs_val  # sempre atualizar (pode limpar)

        if values:
            for sheet_row in rows_for_task:
                updates[sheet_row] = values

    if updates:
        update_columns_in_place(ws, updates)
        logger.info("Delta ClickUp: %d linhas atualizadas", len(updates))

    del existing_rows


def _merge_with_disappeared(
    new_rows: list[list[str]],
    ws,
    headers: list[str],
) -> tuple[list[list[str]], dict[int, dict[int, str]]]:
    """
    Preserva linhas cujo invoice sumiu da PowerRev.
    Q = "Erro no sistema" para desaparecidos.
    Q de reaparecidos já é "" (escrito por build_row/write_all_rows).

    Retorna (merged_rows, q_marks_desaparecidos).
    """
    from sheets_manager import WRITE_COL_COUNT

    uc_idx = headers.index("UC")
    mes_idx = headers.index("Mês de Referencia")
    val_idx = headers.index("Validação")

    existing = read_all_rows(ws)
    if not existing:
        return new_rows, {}

    # Chaves dos dados novos
    new_keys: set[tuple[str, str]] = set()
    for row in new_rows:
        uc = str(row[uc_idx]).strip() if len(row) > uc_idx else ""
        mes = str(row[mes_idx]).strip() if len(row) > mes_idx else ""
        if uc and mes:
            new_keys.add((uc, mes))

    # Detectar desaparecidos
    disappeared_by_uc: dict[str, list[list[str]]] = {}
    disappeared_keys: set[tuple[str, str]] = set()

    for row in existing:
        uc = str(row[uc_idx]).strip() if len(row) > uc_idx else ""
        mes = str(row[mes_idx]).strip() if len(row) > mes_idx else ""
        if not uc or not mes:
            continue
        if (uc, mes) not in new_keys:
            # Preservar A-Q, mas forçar Q="" (será marcado via q_marks)
            preserved = [str(v) for v in row[:WRITE_COL_COUNT]]
            while len(preserved) < WRITE_COL_COUNT:
                preserved.append("")
            preserved[val_idx] = ""  # q_marks cuida do Q depois
            disappeared_by_uc.setdefault(uc, []).append(preserved)
            disappeared_keys.add((uc, mes))

    if not disappeared_keys:
        return new_rows, {}

    logger.warning(
        "Integridade PowerRev: %d linhas sumiram — preservando com '%s' em Q.",
        len(disappeared_keys), _ERRO_SISTEMA,
    )

    # Inserir desaparecidos junto à mesma UC, ordenados por mês
    rows_by_uc: dict[str, list[list[str]]] = {}
    uc_order: list[str] = []
    for row in new_rows:
        uc = str(row[uc_idx]).strip() if len(row) > uc_idx else ""
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

    # q_marks: marcar Q = "Erro no sistema" apenas para desaparecidos
    q_marks: dict[int, dict[int, str]] = {}
    for i, row in enumerate(merged):
        uc = str(row[uc_idx]).strip() if len(row) > uc_idx else ""
        mes = str(row[mes_idx]).strip() if len(row) > mes_idx else ""
        if (uc, mes) in disappeared_keys:
            q_marks[i + 2] = {val_idx: _ERRO_SISTEMA}

    return merged, q_marks


def full_sync() -> None:
    global _known_task_ids
    stats.reset()

    logger.info("═══ FULL SYNC início ═══")
    log_memory("FULL SYNC início")
    t0 = time.time()

    # 1. Fetch ClickUp (já slim)
    tasks = fetch_all_tasks(include_closed=True, transform=slim_task)
    logger.info("Total tasks recebidas: %d", len(tasks))
    log_memory("Pós-fetch ClickUp (slim)")

    _known_task_ids = {t.get("id", "") for t in tasks if t.get("id")}

    # 2. Mapa UC → task
    uc_to_task = _build_uc_task_map(tasks)
    logger.info("UCs mapeadas do ClickUp: %d", len(uc_to_task))

    # 3. Determinar range de meses
    start_ym, end_ym = _get_powerrev_date_range(tasks)
    logger.info("PowerRev: período %s a %s", start_ym, end_ym)

    del tasks
    gc.collect()
    log_memory("Pós-build UC map + gc")

    # 4. Fetch PowerRev agrupado por UC
    if POWERREV_BASE_URL:
        uc_invoices = _fetch_invoices_grouped(start_ym, end_ym)
        log_memory("Pós-fetch PowerRev")
    else:
        logger.warning("PowerRev não configurada — nenhuma linha será gerada.")
        uc_invoices = {}

    if not uc_invoices:
        logger.warning("Nenhum invoice retornado pela PowerRev.")
        ws = get_worksheet()
        ensure_headers(ws)
        headers = get_headers()
        # Sem dados novos, mas preservar linhas existentes como "Erro no sistema"
        rows_empty: list[list[str]] = []
        rows_empty, q_marks = _merge_with_disappeared(rows_empty, ws, headers)
        if rows_empty:
            write_all_rows(ws, rows_empty)
            if q_marks:
                update_columns_in_place(ws, q_marks)
                logger.info("Integridade: %d marcações Q aplicadas.", len(q_marks))
        gc.collect()
        log_sync_stats("FULL SYNC (sem dados novos)")
        return

    # 5. Montar linhas (invoice = linha, enriquecido com ClickUp) + placeholders
    rows, placeholder_keys = _build_rows_from_invoices(uc_invoices, uc_to_task)
    logger.info("Total linhas geradas: %d (incl. %d placeholders)", len(rows), len(placeholder_keys))

    del uc_invoices, uc_to_task
    gc.collect()
    log_memory("Pós-build rows + gc")

    # 6. Escrever (com proteção de integridade)
    ws = get_worksheet()
    ensure_headers(ws)
    headers = get_headers()

    # Detectar invoices que sumiram e preservar suas linhas
    rows_before = len(rows)
    rows, q_marks = _merge_with_disappeared(rows, ws, headers)
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

    write_all_rows(ws, rows)

    # Aplicar marcações na coluna Q (Validação)
    if q_marks:
        update_columns_in_place(ws, q_marks)
        logger.info("Marcações Q aplicadas: %d", len(q_marks))

    elapsed = time.time() - t0
    logger.info(
        "═══ FULL SYNC concluído em %.1fs — %d linhas, %d tasks ═══",
        elapsed, len(rows), len(_known_task_ids),
    )

    del rows
    gc.collect()

    log_sync_stats("FULL SYNC")
    log_memory("Pós-gc final")


def delta_sync(last_updated_ts: int) -> int:
    global _known_task_ids
    stats.reset()
    now_ms = int(time.time() * 1000)

    tasks = fetch_all_tasks(
        include_closed=True,
        date_updated_gt=last_updated_ts,
        transform=slim_task,
    )

    ws = get_worksheet()
    ensure_headers(ws)
    headers = get_headers()

    # ── 1) Tasks atualizadas do ClickUp ───────────────────
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

    # ── 2) PowerRev: checar 3 meses ──────────────────────
    if POWERREV_BASE_URL:
        _delta_powerrev_check(ws, headers)

    log_sync_stats("DELTA SYNC")
    gc.collect()

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
    gc.collect()


def main() -> None:
    global _shutdown_requested

    logger.info("Faturamento Sync iniciando (PID %d)...", os.getpid())
    log_memory("Boot")

    # ── Full sync inicial ────────────────────────────────
    initial_ok = False
    for attempt in range(3):
        if _shutdown_requested:
            logger.info("Shutdown antes do full sync inicial.")
            return
        try:
            full_sync()
            initial_ok = True
            break
        except MemoryError:
            logger.critical("MemoryError no full sync inicial (tentativa %d/3)!", attempt + 1)
            gc.collect()
            _reset_all_sessions("MemoryError")
            time.sleep(30)
        except Exception:
            logger.exception("Erro no full sync inicial (tentativa %d/3).", attempt + 1)
            if attempt < 2:
                _reset_all_sessions("erro inicial")
            time.sleep(30)

    if not initial_ok:
        logger.error("Full sync inicial falhou 3x — entrando no loop mesmo assim.")

    last_full = time.time()
    last_delta_ts = int(time.time() * 1000)
    consecutive_errors = 0
    cycle_count = 0
    boot_time = time.time()

    while not _shutdown_requested:
        try:
            # ── Sleep primeiro (não roda delta logo após full) ──
            _interruptible_sleep(DELTA_SYNC_INTERVAL_S)
            if _shutdown_requested:
                break

            now = time.time()
            cycle_count += 1

            # ── Heartbeat a cada 10 ciclos ───────────────
            if cycle_count % 10 == 0:
                uptime_h = (now - boot_time) / 3600
                logger.info(
                    "♥ Heartbeat — ciclo %d, uptime %.1fh, RSS %.1f MB, "
                    "erros consecutivos: %d",
                    cycle_count, uptime_h, stats.get_memory_mb_safe(),
                    consecutive_errors,
                )

            # ── Sync ─────────────────────────────────────
            if now - last_full >= FULL_SYNC_INTERVAL_S:
                full_sync()
                last_full = time.time()
                last_delta_ts = int(time.time() * 1000)
            else:
                last_delta_ts = delta_sync(last_delta_ts)

            # Sucesso: reset contador de erros
            consecutive_errors = 0

        except KeyboardInterrupt:
            logger.info("Ctrl+C — encerrando.")
            break

        except MemoryError:
            consecutive_errors += 1
            logger.critical(
                "MemoryError no ciclo %d (erro consecutivo #%d)! "
                "Forçando gc + reset de caches.",
                cycle_count, consecutive_errors,
            )
            gc.collect()
            _reset_all_sessions("MemoryError")
            _interruptible_sleep(60)

        except Exception:
            consecutive_errors += 1
            backoff = min(
                _ERROR_BACKOFF_BASE * (2 ** (consecutive_errors - 1)),
                _ERROR_BACKOFF_MAX,
            )
            logger.exception(
                "Erro no ciclo %d (erro consecutivo #%d), retry em %ds...",
                cycle_count, consecutive_errors, backoff,
            )

            # Escalation: após N erros seguidos, reset total
            if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                logger.warning(
                    "Atingiu %d erros consecutivos — reset total de sessions.",
                    consecutive_errors,
                )
                _reset_all_sessions("escalation por erros consecutivos")
                consecutive_errors = 0  # Reset para não logar infinitamente

            _interruptible_sleep(backoff)

    # ── Shutdown graceful ────────────────────────────────
    logger.info(
        "Shutdown graceful — %d ciclos executados, uptime %.1fh",
        cycle_count, (time.time() - boot_time) / 3600,
    )
    log_memory("Shutdown")


def _interruptible_sleep(seconds: float) -> None:
    """Sleep que pode ser interrompido por SIGTERM."""
    end = time.time() + seconds
    while time.time() < end and not _shutdown_requested:
        time.sleep(min(1.0, end - time.time()))


if __name__ == "__main__":
    main()