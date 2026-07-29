"""Espelha dados calculados do faturamento na captura de faturas.

A planilha de faturamento e origem do espelho e recebe de volta somente o
status de captura calculado no destino. As colunas M e O do destino sao
preservadas pelo estado visivel da linha e por uma identidade estavel.
"""

from __future__ import annotations

import argparse
import logging
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Literal

import gspread
from gspread.utils import ValidationConditionType

from config import (
    INVOICE_CAPTURE_OBSERVATIONS_TAB_NAME,
    INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME,
    INVOICE_CAPTURE_SHEET_TAB_NAME,
    INVOICE_CAPTURE_SPREADSHEET_ID,
    SHEET_TAB_NAME,
    SPREADSHEET_ID,
)
from field_map import COLUMN_ORDER
from sheets_manager import _get_client, _retry


logger = logging.getLogger("invoice_capture_mirror")

MIRROR_INVOICE_ID_HEADER = "Invoice ID"
MIRROR_LEFT_SOURCE_COLUMN_COUNT = 8
MIRROR_SOURCE_COLUMN_COUNT = 9
MIRROR_DISTRIBUTOR_HEADER = "Distribuidora"
SOURCE_CAPTURE_STATUS_HEADER = "Captura de Faturas"
SOURCE_BILLING_STATUS_HEADER = "Status de faturamento"
MIRROR_BILLING_STATUS_HEADER = "Status de Fatura"
MIRROR_LOGIN_HEADER = "Login da Distribuidora"
MIRROR_PASSWORD_HEADER = "Senha da Distribuidora"
MIRROR_ISSUE_DAY_HEADER = "Data de Emissão"
MIRROR_IDEAL_SEND_DAY_HEADER = "Data de Envio Ideal"
MIRROR_SEND_MONTH_HEADER = "Mês de envio do boleto"
MIRROR_PROTECTED_HEADERS = [
    "Observações",
    "Concluido",
]
MIRROR_OBSERVATIONS_HEADER = MIRROR_PROTECTED_HEADERS[0]
MIRROR_REFERENCE_MONTH_INDEX = 6
MIRROR_ISSUE_DAY_INDEX = 9
MIRROR_IDEAL_SEND_DAY_INDEX = 10
MIRROR_SEND_MONTH_INDEX = 11
MIRROR_OBSERVATIONS_INDEX = 12
MIRROR_BILLING_STATUS_INDEX = 13
MIRROR_CHECKBOX_INDEX = 14
MIRROR_LOGIN_INDEX = 15
MIRROR_PASSWORD_INDEX = 16
MIRROR_INVOICE_ID_INDEX = 17
MIRROR_COLUMN_COUNT = 18
MIRROR_WRITE_CHUNK_SIZE = 1000
SOURCE_DISTRIBUTOR_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("distribuidora") + 1,
).replace("1", "")
SOURCE_BILLING_STATUS_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("status_faturamento") + 1,
).replace("1", "")
SOURCE_INVOICE_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("invoice_id") + 1,
).replace("1", "")
SOURCE_ISSUE_DATE_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("data_emissao_fatura") + 1,
).replace("1", "")
SOURCE_CAPTURE_STATUS_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("captura_faturas") + 1,
).replace("1", "")
SOURCE_LOGIN_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("login_distribuidora") + 1,
).replace("1", "")
SOURCE_PASSWORD_COLUMN = gspread.utils.rowcol_to_a1(
    1,
    COLUMN_ORDER.index("senha_distribuidora") + 1,
).replace("1", "")
ARCHIVE_HEADERS = [
    "Chave técnica",
    "Invoice ID",
    "Task ID",
    "UC",
    "Mês de Referencia",
    *MIRROR_PROTECTED_HEADERS,
]
PREVIOUS_EXTRA_MANUAL_ARCHIVE_HEADERS = [
    "Chave técnica",
    "Invoice ID",
    "Task ID",
    "UC",
    "Mês de Referencia",
    "Observações",
    "Campo manual",
    "Concluido",
]
PREVIOUS_CURRENT_ARCHIVE_HEADERS = ARCHIVE_HEADERS
PREVIOUS_L_TO_N_ARCHIVE_HEADERS = [
    "Chave técnica",
    "Invoice ID",
    "Task ID",
    "UC",
    "Mês de Referencia",
    "Mês de vencimento",
    "Observações",
    "Concluido",
]
PREVIOUS_J_TO_N_ARCHIVE_HEADERS = [
    "Chave técnica",
    "Invoice ID",
    "Task ID",
    "UC",
    "Mês de Referencia",
    "Data de Envio Ideal",
    "Mês de envio do boleto",
    "Mês de vencimento",
    "Observações",
    "Concluido",
]
LEGACY_I_TO_N_ARCHIVE_HEADERS = [
    "Chave técnica",
    "Invoice ID",
    "Task ID",
    "UC",
    "Mês de Referencia",
    "Data de Emissão",
    "Data de Envio Ideal",
    "Mês de envio do boleto",
    "Mês de vencimento",
    "Observações",
    "Concluido",
]
LEGACY_OBSERVATION_ARCHIVE_HEADERS = [
    "Chave técnica",
    "Invoice ID",
    "Task ID",
    "UC",
    "Mês de Referencia",
    "Observações",
]

_CLICKUP_ISSUE_DAY_BY_TASK_ID: dict[str, str] = {}
_CLICKUP_ISSUE_DAY_CACHE_COMPLETE = False
ARCHIVE_READ_WIDTH = max(
    len(ARCHIVE_HEADERS),
    len(PREVIOUS_EXTRA_MANUAL_ARCHIVE_HEADERS),
    len(PREVIOUS_CURRENT_ARCHIVE_HEADERS),
    len(PREVIOUS_L_TO_N_ARCHIVE_HEADERS),
    len(PREVIOUS_J_TO_N_ARCHIVE_HEADERS),
    len(LEGACY_I_TO_N_ARCHIVE_HEADERS),
)


@dataclass(frozen=True)
class MirrorResult:
    mode: str
    source_rows: int
    changed_rows: int
    structural_rewrite: bool
    preserved_observations: int
    archived_observations: int


@dataclass(frozen=True)
class ProtectedRecord:
    invoice_id: str
    task_id: str
    uc: str
    reference_month: str
    manual_values: tuple[str, ...]

    def as_archive_row(self, key: str) -> list[str]:
        return [
            key,
            self.invoice_id,
            self.task_id,
            self.uc,
            self.reference_month,
            *self.manual_values,
        ]


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _normalize_row(row: Iterable[object], width: int) -> list[str]:
    normalized = [_text(value) for value in list(row)[:width]]
    normalized.extend([""] * (width - len(normalized)))
    return normalized


def _task_id_from_link(value: object) -> str:
    text = _text(value).strip().rstrip("/")
    if not text:
        return ""
    return text.rsplit("/", 1)[-1]


def cache_clickup_invoice_issue_days(
    tasks: Iterable[dict],
    *,
    complete: bool = False,
) -> None:
    """Armazena o fallback ClickUp em memoria sem alterar o faturamento."""
    global _CLICKUP_ISSUE_DAY_CACHE_COMPLETE
    from row_expander import extract_task_invoice_issue_day

    for task in tasks:
        task_id = str(task.get("id", "") or "").strip()
        if task_id:
            _CLICKUP_ISSUE_DAY_BY_TASK_ID[task_id] = extract_task_invoice_issue_day(task)
    if complete:
        _CLICKUP_ISSUE_DAY_CACHE_COMPLETE = True


def _ensure_clickup_issue_day_cache(task_ids: set[str]) -> None:
    global _CLICKUP_ISSUE_DAY_CACHE_COMPLETE
    missing = {
        task_id for task_id in task_ids
        if task_id and task_id not in _CLICKUP_ISSUE_DAY_BY_TASK_ID
    }
    if not missing or _CLICKUP_ISSUE_DAY_CACHE_COMPLETE:
        return

    # No poll.py o cache completo e preenchido durante o fetch normal. Este
    # fallback existe para permitir executar invoice_capture_mirror.py sozinho.
    from clickup_client import fetch_all_tasks
    from row_expander import slim_task

    logger.info(
        "Espelho: carregando fallback de dia de emissao do ClickUp "
        "para execucao independente."
    )
    tasks = fetch_all_tasks(include_closed=True, transform=slim_task)
    cache_clickup_invoice_issue_days(tasks, complete=True)


def _month_token(value: object) -> int | None:
    text = _text(value).strip().lower()
    match = re.match(r"^([a-zç]{3})\.?/(\d{4})$", text)
    if not match:
        return None
    months = {
        "jan": 1, "fev": 2, "mar": 3, "abr": 4,
        "mai": 5, "jun": 6, "jul": 7, "ago": 8,
        "set": 9, "out": 10, "nov": 11, "dez": 12,
    }
    month = months.get(match.group(1))
    return (int(match.group(2)) * 100 + month) if month else None


def _issue_day(value: object) -> int | None:
    text = _text(value).strip()
    if not text:
        return None
    try:
        numeric = float(text.replace(",", "."))
        if numeric.is_integer() and 1 <= int(numeric) <= 31:
            return int(numeric)
    except ValueError:
        pass

    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).day
    except ValueError:
        pass
    for fmt in (
        "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt).day
        except ValueError:
            continue
    match = re.match(r"^\s*\d{4}[-/]\d{1,2}[-/](\d{1,2})", text)
    if match:
        day = int(match.group(1))
        return day if 1 <= day <= 31 else None
    return None


def _issue_date_rank(value: object) -> tuple[int, int, int, int, int, int]:
    """Ordena reemissoes do mesmo mes pela data completa, quando disponivel."""
    text = _text(value).strip()
    if not text:
        return (0, 0, 0, 0, 0, 0)
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return (
            parsed.year, parsed.month, parsed.day,
            parsed.hour, parsed.minute, parsed.second,
        )
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return (parsed.year, parsed.month, parsed.day, 0, 0, 0)
        except ValueError:
            continue
    return (0, 0, 0, 0, 0, 0)


def _normalize_uc_key(value: object) -> str:
    text = _text(value).strip()
    digits = re.sub(r"\D+", "", text)
    if digits:
        return digits
    return unicodedata.normalize("NFKC", text).strip().lower()


def _normal_invoice_id(invoice: dict) -> int:
    raw = _text(
        invoice.get("invoiceId") or invoice.get("idFaturaConsumo") or invoice.get("id")
    ).strip()
    try:
        return int(raw)
    except ValueError:
        digits = "".join(ch for ch in raw if ch.isdigit())
        return int(digits) if digits else 0


def _normal_invoice_rank(invoice: dict) -> tuple[tuple[int, int, int, int, int, int], int]:
    return _issue_date_rank(_normal_invoice_issue_date(invoice)), _normal_invoice_id(invoice)


def _normal_invoice_issue_date(invoice: dict) -> str:
    for field in ("dtEmissao", "dataEmissao"):
        issue_date = _text(invoice.get(field)).strip()
        if issue_date:
            return issue_date
    return ""


def _normal_invoice_items(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("content", "data", "items", "results", "responseList", "invoices"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _normal_invoice_uc(invoice: dict) -> str:
    for field in ("nuInstalacao", "cdInstalacao"):
        uc = _normalize_uc_key(invoice.get(field))
        if uc:
            return uc

    keys: list[str] = []
    for field in (
        "idUnidadeConsumo",
        "codUnidadeConsumo",
        "cdChaveExterna",
        "noRecurso",
    ):
        value = _text(invoice.get(field)).strip()
        if value:
            keys.append(value)

    consumer_units_raw = invoice.get("consumerUnits")
    if isinstance(consumer_units_raw, list):
        for value in consumer_units_raw:
            if not isinstance(value, dict):
                continue
            resource = value.get("recurso") if isinstance(value.get("recurso"), dict) else None
            if resource:
                for field in ("idUnidadeConsumo", "cdChaveExterna", "noRecurso"):
                    raw = _text(resource.get(field)).strip()
                    if raw:
                        keys.append(raw)
            for field in ("idUnidadeConsumo", "cdChaveExterna", "noRecurso"):
                raw = _text(value.get(field)).strip()
                if raw:
                    keys.append(raw)
    elif consumer_units_raw is not None:
        raw = _text(consumer_units_raw).strip()
        if raw:
            keys.append(raw)

    if not keys:
        return ""

    try:
        import powerrev_client

        powerrev_client._load_consumer_units()
        for key in dict.fromkeys(keys):
            normalized_key = _normalize_uc_key(key)
            uc = (
                powerrev_client._UC_BY_ID.get(key.strip())
                or powerrev_client._UC_BY_INSTALLATION.get(normalized_key)
                or powerrev_client._UC_BY_CODE.get(normalized_key)
            )
            if uc and uc.get("nuInstalacao"):
                return _normalize_uc_key(uc["nuInstalacao"])
    except Exception:
        logger.debug("Falha ao resolver UC da invoice normal.", exc_info=True)

    return ""


def _fetch_normal_invoice_issue_dates_for_month(
    month: int,
    target_ucs: set[str],
) -> dict[tuple[str, int], str]:
    if not target_ucs:
        return {}

    from powerrev_client import POWERREV_BASE_URL, _request

    response = _request(
        "GET",
        f"{POWERREV_BASE_URL}/invoice",
        params={"nuAnoMes": str(month)},
    )
    invoices = _normal_invoice_items(response.json())
    selected_by_uc: dict[str, dict] = {}
    for invoice in invoices:
        uc_key = _normal_invoice_uc(invoice)
        if not uc_key or uc_key not in target_ucs:
            continue
        issue_date = _normal_invoice_issue_date(invoice)
        if not issue_date:
            continue
        current = selected_by_uc.get(uc_key)
        if current is None or _normal_invoice_rank(invoice) > _normal_invoice_rank(current):
            selected_by_uc[uc_key] = invoice

    logger.info(
        "Espelho: /invoice?nuAnoMes=%s retornou %d invoices; dtEmissao para %d/%d UCs.",
        month,
        len(invoices),
        len(selected_by_uc),
        len(target_ucs),
    )
    return {
        (uc_key, month): _normal_invoice_issue_date(invoice)
        for uc_key, invoice in selected_by_uc.items()
    }


def _populate_distributor_issue_dates(rows: list[list[str]]) -> None:
    """Replica a coluna N do rateio: dtEmissao de /invoice?nuAnoMes por UC/mes."""
    ucs_by_month: dict[int, set[str]] = defaultdict(set)
    for row in rows:
        uc_key = _normalize_uc_key(row[2])
        month = _month_token(row[MIRROR_REFERENCE_MONTH_INDEX])
        if not uc_key or month is None:
            continue
        ucs_by_month[month].add(uc_key)

    issue_dates: dict[tuple[str, int], str] = {}
    for month, target_ucs in sorted(ucs_by_month.items()):
        try:
            issue_dates.update(_fetch_normal_invoice_issue_dates_for_month(month, target_ucs))
        except Exception as exc:
            logger.warning(
                "dtEmissao PowerRev via /invoice?nuAnoMes=%s indisponivel; mantendo fallback existente: %s",
                month,
                exc,
            )

    for row in rows:
        uc_key = _normalize_uc_key(row[2])
        month = _month_token(row[MIRROR_REFERENCE_MONTH_INDEX])
        if not uc_key or month is None:
            continue
        issue_date = issue_dates.get((uc_key, month))
        if issue_date:
            row[MIRROR_ISSUE_DAY_INDEX] = issue_date


def _populate_issue_days(rows: list[list[str]]) -> None:
    """Aplica PowerRev atual/anterior e, por ultimo, fallback do ClickUp."""
    by_uc: dict[str, dict[int, tuple[tuple[int, int, int, int, int, int], int]]] = (
        defaultdict(dict)
    )
    for row in rows:
        uc = _normalize_uc_key(row[2])
        month = _month_token(row[MIRROR_REFERENCE_MONTH_INDEX])
        day = _issue_day(row[MIRROR_ISSUE_DAY_INDEX])
        if uc and month is not None and day is not None:
            candidate = (_issue_date_rank(row[MIRROR_ISSUE_DAY_INDEX]), day)
            current = by_uc[uc].get(month)
            if current is None or candidate[0] >= current[0]:
                by_uc[uc][month] = candidate

    histories = {
        uc: sorted((month, value[1]) for month, value in month_days.items())
        for uc, month_days in by_uc.items()
    }
    needs_fallback: set[str] = set()
    resolved: list[int | None] = []
    for row in rows:
        uc = _normalize_uc_key(row[2])
        month = _month_token(row[MIRROR_REFERENCE_MONTH_INDEX])
        day = None
        if uc and month is not None:
            candidates = [
                candidate_day for candidate_month, candidate_day in histories.get(uc, [])
                if candidate_month <= month
            ]
            day = candidates[-1] if candidates else None
        resolved.append(day)
        if day is None:
            task_id = _task_id_from_link(row[0])
            if task_id:
                needs_fallback.add(task_id)

    _ensure_clickup_issue_day_cache(needs_fallback)
    for row, day in zip(rows, resolved):
        if day is None:
            fallback = _CLICKUP_ISSUE_DAY_BY_TASK_ID.get(_task_id_from_link(row[0]), "")
            day = _issue_day(fallback)
        row[MIRROR_ISSUE_DAY_INDEX] = str(day) if day is not None else ""
        if day is None:
            row[MIRROR_IDEAL_SEND_DAY_INDEX] = ""
            row[MIRROR_SEND_MONTH_INDEX] = ""
            continue

        if day < 2:
            ideal_send_day = 10
        elif day < 12:
            ideal_send_day = 20
        elif day < 22:
            ideal_send_day = 30
        else:
            ideal_send_day = 10

        row[MIRROR_IDEAL_SEND_DAY_INDEX] = str(ideal_send_day)
        row[MIRROR_SEND_MONTH_INDEX] = (
            "Mês Seguinte" if day > ideal_send_day else "Mês Atual"
        )


def _last_nonempty_row(*datasets: list[list[object]]) -> int:
    last = 0
    total = max((len(dataset) for dataset in datasets), default=0)
    for index in range(total):
        values: list[object] = []
        for dataset in datasets:
            values.extend(dataset[index] if index < len(dataset) else [])
        if any(_text(value).strip() for value in values):
            last = index + 1
    return last


def _read_source(source_ws) -> tuple[list[str], list[list[str]]]:
    """Le somente colunas necessarias da origem, sempre como valores visiveis."""
    distributor_range = f"{SOURCE_DISTRIBUTOR_COLUMN}:{SOURCE_DISTRIBUTOR_COLUMN}"
    billing_status_range = f"{SOURCE_BILLING_STATUS_COLUMN}:{SOURCE_BILLING_STATUS_COLUMN}"
    invoice_range = f"{SOURCE_INVOICE_COLUMN}:{SOURCE_INVOICE_COLUMN}"
    issue_date_range = f"{SOURCE_ISSUE_DATE_COLUMN}:{SOURCE_ISSUE_DATE_COLUMN}"
    login_range = f"{SOURCE_LOGIN_COLUMN}:{SOURCE_LOGIN_COLUMN}"
    password_range = f"{SOURCE_PASSWORD_COLUMN}:{SOURCE_PASSWORD_COLUMN}"
    ranges = _retry(
        source_ws.batch_get,
        [
            "A:H",
            distributor_range,
            issue_date_range,
            billing_status_range,
            login_range,
            password_range,
            invoice_range,
        ],
        value_render_option="FORMATTED_VALUE",
    )
    if len(ranges) != 7:
        raise RuntimeError(
            "Leitura da origem nao retornou os intervalos "
            f"A:H, {distributor_range}, {issue_date_range}, "
            f"{billing_status_range}, {login_range}, {password_range} "
            f"e {invoice_range}."
        )

    left = ranges[0] or []
    distributors = ranges[1] or []
    issue_dates = ranges[2] or []
    billing_statuses = ranges[3] or []
    logins = ranges[4] or []
    passwords = ranges[5] or []
    invoice_ids = ranges[6] or []
    if not left or len(left[0]) < MIRROR_LEFT_SOURCE_COLUMN_COUNT:
        raise RuntimeError("A planilha de faturamento nao possui os cabecalhos A:H esperados.")

    source_left_headers = _normalize_row(left[0], MIRROR_LEFT_SOURCE_COLUMN_COUNT)
    distributor_header = _text(
        distributors[0][0] if distributors and distributors[0] else ""
    ).strip()
    if distributor_header != MIRROR_DISTRIBUTOR_HEADER:
        raise RuntimeError(
            f"A coluna {SOURCE_DISTRIBUTOR_COLUMN} da planilha de faturamento nao e "
            "'Distribuidora'; espelhamento cancelado antes de qualquer escrita."
        )
    issue_date_header = _text(
        issue_dates[0][0] if issue_dates and issue_dates[0] else ""
    ).strip()
    if issue_date_header != "Data de Emissão da fatura":
        raise RuntimeError(
            f"A coluna {SOURCE_ISSUE_DATE_COLUMN} da planilha de faturamento nao e "
            "'Data de Emissão da fatura'; espelhamento cancelado antes de qualquer escrita."
        )
    billing_status_header = _text(
        billing_statuses[0][0] if billing_statuses and billing_statuses[0] else ""
    ).strip()
    if billing_status_header != SOURCE_BILLING_STATUS_HEADER:
        raise RuntimeError(
            f"A coluna {SOURCE_BILLING_STATUS_COLUMN} da planilha de faturamento nao e "
            "'Status de faturamento'; espelhamento cancelado antes de qualquer escrita."
        )
    login_header = _text(logins[0][0] if logins and logins[0] else "").strip()
    if login_header != MIRROR_LOGIN_HEADER:
        raise RuntimeError(
            f"A coluna {SOURCE_LOGIN_COLUMN} da planilha de faturamento nao e "
            "'Login da Distribuidora'; espelhamento cancelado antes de qualquer escrita."
        )
    password_header = _text(
        passwords[0][0] if passwords and passwords[0] else ""
    ).strip()
    if password_header != MIRROR_PASSWORD_HEADER:
        raise RuntimeError(
            f"A coluna {SOURCE_PASSWORD_COLUMN} da planilha de faturamento nao e "
            "'Senha da Distribuidora'; espelhamento cancelado antes de qualquer escrita."
        )
    invoice_header = _text(invoice_ids[0][0] if invoice_ids and invoice_ids[0] else "").strip()
    if invoice_header != MIRROR_INVOICE_ID_HEADER:
        raise RuntimeError(
            f"A coluna {SOURCE_INVOICE_COLUMN} da planilha de faturamento nao e "
            "'Invoice ID'; "
            "espelhamento cancelado antes de qualquer escrita."
        )

    data_left = left[1:]
    data_distributors = distributors[1:]
    data_issue_dates = issue_dates[1:]
    data_billing_statuses = billing_statuses[1:]
    data_logins = logins[1:]
    data_passwords = passwords[1:]
    data_invoice_ids = invoice_ids[1:]
    row_count = _last_nonempty_row(
        data_left,
        data_distributors,
        data_issue_dates,
        data_billing_statuses,
        data_logins,
        data_passwords,
        data_invoice_ids,
    )
    rows: list[list[str]] = []
    for index in range(row_count):
        left_row = data_left[index] if index < len(data_left) else []
        distributor_row = data_distributors[index] if index < len(data_distributors) else []
        issue_date_row = data_issue_dates[index] if index < len(data_issue_dates) else []
        billing_status_row = (
            data_billing_statuses[index] if index < len(data_billing_statuses) else []
        )
        login_row = data_logins[index] if index < len(data_logins) else []
        password_row = data_passwords[index] if index < len(data_passwords) else []
        invoice_row = data_invoice_ids[index] if index < len(data_invoice_ids) else []
        normalized_left = _normalize_row(left_row, MIRROR_LEFT_SOURCE_COLUMN_COUNT)
        distributor = _text(distributor_row[0] if distributor_row else "")
        issue_date = _text(issue_date_row[0] if issue_date_row else "")
        billing_status = _text(billing_status_row[0] if billing_status_row else "")
        login = _text(login_row[0] if login_row else "")
        password = _text(password_row[0] if password_row else "")
        invoice_id = _text(invoice_row[0] if invoice_row else "")
        rows.append(
            normalized_left[:4]
            + [distributor]
            + normalized_left[4:]
            + [issue_date]
            + ["", ""]
            + [""]
            + [billing_status]
            + ["FALSE"]
            + [login, password, invoice_id]
        )

    _populate_distributor_issue_dates(rows)
    _populate_issue_days(rows)
    headers = (
        source_left_headers[:4]
        + [MIRROR_DISTRIBUTOR_HEADER]
        + source_left_headers[4:]
        + [
            MIRROR_ISSUE_DAY_HEADER,
            MIRROR_IDEAL_SEND_DAY_HEADER,
            MIRROR_SEND_MONTH_HEADER,
            MIRROR_OBSERVATIONS_HEADER,
            MIRROR_BILLING_STATUS_HEADER,
            "Concluido",
            MIRROR_LOGIN_HEADER,
            MIRROR_PASSWORD_HEADER,
        ]
        + [MIRROR_INVOICE_ID_HEADER]
    )
    return headers, rows


def _read_grid(ws, range_name: str, width: int) -> list[list[str]]:
    values = _retry(
        ws.get,
        range_name,
        value_render_option="FORMATTED_VALUE",
    ) or []
    normalized = [_normalize_row(row, width) for row in values]
    while normalized and not any(cell.strip() for cell in normalized[-1]):
        normalized.pop()
    return normalized


def _write_single_column(ws, col_letter: str, start_row: int, values: list[object]) -> None:
    for offset in range(0, len(values), MIRROR_WRITE_CHUNK_SIZE):
        chunk = values[offset:offset + MIRROR_WRITE_CHUNK_SIZE]
        first = start_row + offset
        last = first + len(chunk) - 1
        _retry(
            ws.batch_update,
            [{
                "range": f"{col_letter}{first}:{col_letter}{last}",
                "values": [[value] for value in chunk],
            }],
            value_input_option="RAW",
            is_write=True,
        )


def _ensure_source_capture_checkbox(source_ws) -> None:
    _retry(
        source_ws.add_validation,
        f"{SOURCE_CAPTURE_STATUS_COLUMN}2:{SOURCE_CAPTURE_STATUS_COLUMN}",
        ValidationConditionType.boolean,
        [],
        strict=True,
        showCustomUi=True,
        is_write=True,
    )


def _sync_capture_status_to_source(source_ws, target_ws, source_row_count: int) -> int:
    """Copia Faturas!O para a coluna Captura de Faturas da origem, por ordem."""
    if source_row_count <= 0:
        return 0

    target_values = _retry(
        target_ws.get,
        "O:O",
        value_render_option="UNFORMATTED_VALUE",
    ) or []
    source_values = _retry(
        source_ws.get,
        f"{SOURCE_CAPTURE_STATUS_COLUMN}:{SOURCE_CAPTURE_STATUS_COLUMN}",
        value_render_option="FORMATTED_VALUE",
    ) or []

    target_header = _text(target_values[0][0] if target_values and target_values[0] else "").strip()
    if target_header != "Concluido":
        raise RuntimeError(
            "A coluna O do espelho de captura nao e 'Concluido'; "
            "copia para Captura de Faturas cancelada."
        )

    source_header = _text(source_values[0][0] if source_values and source_values[0] else "").strip()
    if source_header != SOURCE_CAPTURE_STATUS_HEADER:
        raise RuntimeError(
            f"A coluna {SOURCE_CAPTURE_STATUS_COLUMN} da planilha de faturamento nao e "
            "'Captura de Faturas'; copia cancelada antes de qualquer escrita."
        )

    data_rows = target_values[1:]
    output = [
        data_rows[index][0] if index < len(data_rows) and data_rows[index] else ""
        for index in range(source_row_count)
    ]
    _ensure_source_capture_checkbox(source_ws)
    _write_single_column(source_ws, SOURCE_CAPTURE_STATUS_COLUMN, 2, output)
    return len(output)


def _is_legacy_no_distributor_target_layout(header: list[str]) -> bool:
    normalized = _normalize_row(header, MIRROR_COLUMN_COUNT)
    return (
        normalized[0] == "Task ID"
        and normalized[1] == "Status Detalhado"
        and normalized[2] == "UC"
        and normalized[3] == "UC Aneel"
        and normalized[4] == "Razão Social"
        and normalized[13] == MIRROR_INVOICE_ID_HEADER
        and not normalized[14]
    )


def _is_previous_distributor_target_layout(header: list[str]) -> bool:
    normalized = _normalize_row(header, MIRROR_COLUMN_COUNT)
    return (
        normalized[0] == "Task ID"
        and normalized[1] == "Status Detalhado"
        and normalized[2] == "UC"
        and normalized[3] == "UC Aneel"
        and normalized[4] == MIRROR_DISTRIBUTOR_HEADER
        and normalized[5] == "Razão Social"
        and normalized[14] == MIRROR_INVOICE_ID_HEADER
        and not normalized[15]
    )


def _is_previous_status_without_extra_manual_layout(header: list[str]) -> bool:
    normalized = _normalize_row(header, MIRROR_COLUMN_COUNT)
    return (
        normalized[0] == "Task ID"
        and normalized[4] == MIRROR_DISTRIBUTOR_HEADER
        and normalized[10] in {SOURCE_BILLING_STATUS_HEADER, MIRROR_BILLING_STATUS_HEADER}
        and normalized[15] == MIRROR_INVOICE_ID_HEADER
    )


def _is_previous_status_with_extra_manual_layout(header: list[str]) -> bool:
    normalized = _normalize_row(header, 17)
    return (
        normalized[0] == "Task ID"
        and normalized[4] == MIRROR_DISTRIBUTOR_HEADER
        and normalized[10] in {SOURCE_BILLING_STATUS_HEADER, MIRROR_BILLING_STATUS_HEADER}
        and normalized[16] == MIRROR_INVOICE_ID_HEADER
    )


def _is_previous_current_target_layout(header: list[str]) -> bool:
    normalized = _normalize_row(header, 18)
    return (
        normalized[0] == "Task ID"
        and normalized[4] == MIRROR_DISTRIBUTOR_HEADER
        and normalized[MIRROR_BILLING_STATUS_INDEX] == MIRROR_BILLING_STATUS_HEADER
        and normalized[14] == "Concluido"
        and normalized[15] == MIRROR_INVOICE_ID_HEADER
        and not normalized[16]
        and not normalized[17]
    )


def _is_current_target_layout(header: list[str]) -> bool:
    normalized = _normalize_row(header, MIRROR_COLUMN_COUNT)
    return (
        normalized[0] == "Task ID"
        and normalized[4] == MIRROR_DISTRIBUTOR_HEADER
        and normalized[MIRROR_BILLING_STATUS_INDEX] == MIRROR_BILLING_STATUS_HEADER
        and normalized[MIRROR_LOGIN_INDEX] == MIRROR_LOGIN_HEADER
        and normalized[MIRROR_PASSWORD_INDEX] == MIRROR_PASSWORD_HEADER
        and normalized[MIRROR_INVOICE_ID_INDEX] == MIRROR_INVOICE_ID_HEADER
    )


def _upgrade_previous_current_row(row: list[str]) -> list[str]:
    normalized = _normalize_row(row, 16)
    return normalized[:15] + ["", ""] + [normalized[15]]


def _convert_previous_status_extra_row(row: list[str]) -> list[str]:
    normalized = _normalize_row(row, 17)
    previous_current = (
        normalized[:10]
        + [normalized[11], normalized[12], normalized[13]]
        + [normalized[10], normalized[15], normalized[16]]
    )
    return _upgrade_previous_current_row(previous_current)


def _normalize_target_grid(grid: list[list[str]]) -> list[list[str]]:
    """Converte layouts antigos para o layout atual A:R, se necessario."""
    if not grid:
        return grid
    is_current = _is_current_target_layout(grid[0])
    is_legacy_no_distributor = _is_legacy_no_distributor_target_layout(grid[0])
    is_previous_distributor = _is_previous_distributor_target_layout(grid[0])
    is_previous_status = _is_previous_status_without_extra_manual_layout(grid[0])
    is_previous_status_extra = _is_previous_status_with_extra_manual_layout(grid[0])
    is_previous_current = _is_previous_current_target_layout(grid[0])
    if is_current:
        return [_normalize_row(row, MIRROR_COLUMN_COUNT) for row in grid]

    if is_previous_current:
        converted = [_upgrade_previous_current_row(grid[0])]
        for row in grid[1:]:
            normalized17 = _normalize_row(row, 17)
            p_looks_like_old_checkbox = normalized17[15].strip().upper() in {
                "TRUE", "FALSE",
            }
            if p_looks_like_old_checkbox:
                converted.append(_convert_previous_status_extra_row(normalized17))
            else:
                converted.append(_upgrade_previous_current_row(row))
        return converted

    if not any((
        is_legacy_no_distributor,
        is_previous_distributor,
        is_previous_status,
        is_previous_status_extra,
    )):
        return grid

    converted: list[list[str]] = []
    for row in grid:
        if is_legacy_no_distributor:
            normalized = _normalize_row(row, 16)
            previous_current = (
                normalized[:4]
                + [""]
                + normalized[4:11]
                + [normalized[11], "", normalized[12], normalized[13]]
            )
            converted.append(_upgrade_previous_current_row(previous_current))
        elif is_previous_distributor:
            normalized = _normalize_row(row, 16)
            previous_current = (
                normalized[:12]
                + [normalized[12], "", normalized[13], normalized[14]]
            )
            converted.append(_upgrade_previous_current_row(previous_current))
        elif is_previous_status_extra:
            converted.append(_convert_previous_status_extra_row(row))
        else:
            normalized = _normalize_row(row, 16)
            previous_current = (
                normalized[:10]
                + [normalized[11], normalized[12], normalized[13]]
                + [normalized[10], normalized[14], normalized[15]]
            )
            converted.append(_upgrade_previous_current_row(previous_current))
    return converted


def _manual_label(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", _text(value).strip().lower())
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(normalized.replace("_", " ").replace("-", " ").split())


def _sanitize_manual_values(manual_values: Iterable[object]) -> tuple[str, str]:
    values = _normalize_row(manual_values, len(MIRROR_PROTECTED_HEADERS))
    observation = values[0]
    checkbox = values[1].strip().upper()
    if checkbox not in {"TRUE", "FALSE"}:
        checkbox = "FALSE"
    return observation, checkbox


def _has_manual_payload(manual_values: Iterable[object]) -> bool:
    observation, checkbox = _sanitize_manual_values(manual_values)
    return bool(observation.strip()) or checkbox.strip().upper() == "TRUE"


def _manual_values_from_row(row: list[str]) -> tuple[str, str]:
    normalized = _normalize_row(row, MIRROR_COLUMN_COUNT)
    return _sanitize_manual_values((
        normalized[MIRROR_OBSERVATIONS_INDEX],
        normalized[MIRROR_CHECKBOX_INDEX],
    ))


def _apply_manual_values(row: list[str], manual_values: tuple[str, ...]) -> None:
    values = _sanitize_manual_values(manual_values)
    row[MIRROR_OBSERVATIONS_INDEX] = values[0]
    row[MIRROR_CHECKBOX_INDEX] = values[1]


def _computed_values(row: list[str]) -> list[str]:
    normalized = _normalize_row(row, MIRROR_COLUMN_COUNT)
    return (
        normalized[:MIRROR_OBSERVATIONS_INDEX]
        + [normalized[MIRROR_BILLING_STATUS_INDEX]]
        + normalized[MIRROR_LOGIN_INDEX:MIRROR_INVOICE_ID_INDEX + 1]
    )


def _make_row_keys(rows: list[list[str]]) -> list[str]:
    """Gera chaves estaveis e diferencia duplicatas por ocorrencia."""
    occurrences: dict[str, int] = defaultdict(int)
    keys: list[str] = []
    for row in rows:
        normalized = _normalize_row(row, MIRROR_COLUMN_COUNT)
        invoice_id = normalized[MIRROR_INVOICE_ID_INDEX].strip()
        if invoice_id and not _is_synthetic_invoice_id(invoice_id):
            base = f"INV:{invoice_id}"
        else:
            identity = _row_stable_manual_identity(normalized)
            if all(identity):
                base = "ROW:{}|{}|{}".format(*identity)
            elif invoice_id:
                base = f"SYN:{invoice_id}"
            else:
                base = "ROW:{}|{}|{}".format(
                    normalized[0].strip(),
                    normalized[2].strip(),
                    normalized[MIRROR_REFERENCE_MONTH_INDEX].strip(),
                )
        occurrences[base] += 1
        keys.append(f"{base}#{occurrences[base]}")
    return keys


def _is_synthetic_invoice_id(value: object) -> bool:
    return _text(value).strip().startswith("SYN|")


def _reference_month_key(value: object) -> str:
    month = _month_token(value)
    if month is not None:
        return str(month)
    return _manual_label(value)


def _stable_manual_identity(
    task_id: object,
    uc: object,
    reference_month: object,
) -> tuple[str, str, str]:
    task_key = _task_id_from_link(task_id)
    uc_key = _normalize_uc_key(uc)
    month_key = _reference_month_key(reference_month)
    if not task_key or not uc_key or not month_key:
        return ("", "", "")
    return task_key, uc_key, month_key


def _row_stable_manual_identity(row: list[str]) -> tuple[str, str, str]:
    normalized = _normalize_row(row, MIRROR_COLUMN_COUNT)
    return _stable_manual_identity(
        normalized[0],
        normalized[2],
        normalized[MIRROR_REFERENCE_MONTH_INDEX],
    )


def _record_stable_manual_identity(record: ProtectedRecord) -> tuple[str, str, str]:
    return _stable_manual_identity(
        record.task_id,
        record.uc,
        record.reference_month,
    )


def _row_identity_counts(rows: list[list[str]]) -> dict[tuple[str, str, str], int]:
    counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in rows:
        identity = _row_stable_manual_identity(row)
        if all(identity):
            counts[identity] += 1
    return counts


def _unique_records_by_identity(
    records: dict[str, ProtectedRecord],
) -> dict[tuple[str, str, str], ProtectedRecord]:
    buckets = _record_buckets_by_identity(records)

    unique: dict[tuple[str, str, str], ProtectedRecord] = {}
    for identity, items in buckets.items():
        manual_values = {item.manual_values for item in items}
        if len(manual_values) == 1:
            unique[identity] = items[-1]
    return unique


def _record_buckets_by_identity(
    records: dict[str, ProtectedRecord],
) -> dict[tuple[str, str, str], list[ProtectedRecord]]:
    buckets: dict[tuple[str, str, str], list[ProtectedRecord]] = defaultdict(list)
    for record in records.values():
        identity = _record_stable_manual_identity(record)
        if all(identity):
            buckets[identity].append(record)
    return buckets


def _record_keys_by_identity(
    records: dict[str, ProtectedRecord],
) -> dict[tuple[str, str, str], set[str]]:
    keys_by_identity: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for key, record in records.items():
        identity = _record_stable_manual_identity(record)
        if all(identity):
            keys_by_identity[identity].add(key)
    return keys_by_identity


def _discard_record(
    records: dict[str, ProtectedRecord],
    keys_by_identity: dict[tuple[str, str, str], set[str]],
    key: str,
) -> None:
    record = records.pop(key, None)
    if record is None:
        return
    identity = _record_stable_manual_identity(record)
    identity_keys = keys_by_identity.get(identity)
    if identity_keys is None:
        return
    identity_keys.discard(key)
    if not identity_keys:
        keys_by_identity.pop(identity, None)


def _store_record(
    records: dict[str, ProtectedRecord],
    keys_by_identity: dict[tuple[str, str, str], set[str]],
    key: str,
    record: ProtectedRecord,
) -> None:
    _discard_record(records, keys_by_identity, key)
    records[key] = record
    identity = _record_stable_manual_identity(record)
    if all(identity):
        keys_by_identity.setdefault(identity, set()).add(key)


def _remove_records_by_identity(
    records: dict[str, ProtectedRecord],
    keys_by_identity: dict[tuple[str, str, str], set[str]],
    identity: tuple[str, str, str],
    *,
    except_key: str = "",
) -> None:
    if not all(identity):
        return
    for existing_key in tuple(keys_by_identity.get(identity, ())):
        if existing_key == except_key:
            continue
        _discard_record(records, keys_by_identity, existing_key)


def _record_from_target_row(row: list[str]) -> ProtectedRecord:
    normalized = _normalize_row(row, MIRROR_COLUMN_COUNT)
    return ProtectedRecord(
        invoice_id=normalized[MIRROR_INVOICE_ID_INDEX],
        task_id=normalized[0],
        uc=normalized[2],
        reference_month=normalized[MIRROR_REFERENCE_MONTH_INDEX],
        manual_values=_manual_values_from_row(normalized),
    )


def _records_from_target_rows(
    target_rows: list[list[str]],
) -> dict[str, ProtectedRecord]:
    return {
        key: _record_from_target_row(row)
        for key, row in zip(_make_row_keys(target_rows), target_rows)
    }


def _read_archive(archive_ws) -> dict[str, ProtectedRecord]:
    grid = _read_grid(archive_ws, "A:K", ARCHIVE_READ_WIDTH)
    if not grid:
        return {}
    header = grid[0]
    is_current = header[:len(ARCHIVE_HEADERS)] == ARCHIVE_HEADERS
    is_previous_extra_manual = (
        header[:len(PREVIOUS_EXTRA_MANUAL_ARCHIVE_HEADERS)]
        == PREVIOUS_EXTRA_MANUAL_ARCHIVE_HEADERS
    )
    is_previous_current = (
        header[:len(PREVIOUS_CURRENT_ARCHIVE_HEADERS)]
        == PREVIOUS_CURRENT_ARCHIVE_HEADERS
    )
    is_previous_l_to_n = (
        header[:len(PREVIOUS_L_TO_N_ARCHIVE_HEADERS)]
        == PREVIOUS_L_TO_N_ARCHIVE_HEADERS
    )
    is_previous_j_to_n = (
        header[:len(PREVIOUS_J_TO_N_ARCHIVE_HEADERS)]
        == PREVIOUS_J_TO_N_ARCHIVE_HEADERS
    )
    is_legacy_i_to_n = (
        header[:len(LEGACY_I_TO_N_ARCHIVE_HEADERS)]
        == LEGACY_I_TO_N_ARCHIVE_HEADERS
    )
    is_legacy_observation = (
        header[:len(LEGACY_OBSERVATION_ARCHIVE_HEADERS)]
        == LEGACY_OBSERVATION_ARCHIVE_HEADERS
    )
    if not any((
        is_current,
        is_previous_extra_manual,
        is_previous_current,
        is_previous_l_to_n,
        is_previous_j_to_n,
        is_legacy_i_to_n,
        is_legacy_observation,
    )):
        raise RuntimeError(
            "A aba tecnica de campos protegidos possui cabecalho inesperado; "
            "nenhuma escrita foi realizada."
        )

    records: dict[str, ProtectedRecord] = {}
    for row in grid[1:]:
        key = row[0].strip()
        if not key:
            continue
        if is_previous_extra_manual:
            manual_values = [row[5], row[7]]
        elif is_previous_current:
            manual_values = [row[5], row[6]]
        elif is_previous_l_to_n:
            manual_values = [row[6], row[7]]
        elif is_previous_j_to_n:
            manual_values = [row[8], row[9]]
        elif is_legacy_i_to_n:
            manual_values = [row[9], row[10]]
        elif is_legacy_observation:
            manual_values = [row[5], ""]
        else:
            manual_values = row[5:7]
        sanitized_manual_values = _sanitize_manual_values(manual_values)
        records[key] = ProtectedRecord(
            invoice_id=row[1],
            task_id=row[2],
            uc=row[3],
            reference_month=row[4],
            manual_values=sanitized_manual_values,
        )
    return records


def _merge_current_observations(
    records: dict[str, ProtectedRecord],
    target_rows: list[list[str]],
) -> dict[str, ProtectedRecord]:
    """Atualiza o arquivo tecnico com o estado manual atualmente visivel."""
    merged = dict(records)
    merged_identity_keys = _record_keys_by_identity(merged)
    archived_identity_buckets = _record_buckets_by_identity(records)
    target_identity_counts = _row_identity_counts(target_rows)
    for key, row in zip(_make_row_keys(target_rows), target_rows):
        record = _record_from_target_row(row)
        identity = _record_stable_manual_identity(record)
        unique_visible_identity = bool(
            all(identity) and target_identity_counts.get(identity) == 1
        )
        had_archived_state = bool(
            key in records
            or (all(identity) and archived_identity_buckets.get(identity))
        )
        if unique_visible_identity:
            _remove_records_by_identity(
                merged,
                merged_identity_keys,
                identity,
                except_key=key,
            )

        if _has_manual_payload(record.manual_values) or had_archived_state:
            _store_record(merged, merged_identity_keys, key, record)
        else:
            _discard_record(merged, merged_identity_keys, key)
    return merged


def _project_observations(
    source_rows: list[list[str]],
    records: dict[str, ProtectedRecord],
    visible_records: dict[str, ProtectedRecord],
) -> tuple[list[list[str]], int, dict[str, ProtectedRecord]]:
    projected: list[list[str]] = []
    updated_records = dict(records)
    updated_identity_keys = _record_keys_by_identity(updated_records)
    source_identity_counts = _row_identity_counts(source_rows)
    visible_identity_buckets = _record_buckets_by_identity(visible_records)
    visible_by_identity = _unique_records_by_identity(visible_records)
    archive_identity_buckets = _record_buckets_by_identity(records)
    records_by_identity = _unique_records_by_identity(records)
    preserved = 0
    for key, row in zip(_make_row_keys(source_rows), source_rows):
        row_out = _normalize_row(row, MIRROR_COLUMN_COUNT)
        identity = _row_stable_manual_identity(row_out)
        source_identity_is_unique = bool(
            all(identity) and source_identity_counts.get(identity) == 1
        )

        # A aba visivel e sempre a fonte da verdade para M/O. O arquivo
        # tecnico so entra quando nao existe uma linha visivel correspondente.
        record = visible_records.get(key)
        if record is None and all(identity):
            visible_candidates = visible_identity_buckets.get(identity, [])
            if source_identity_is_unique:
                record = visible_by_identity.get(identity)
            if record is None and any(
                _has_manual_payload(item.manual_values)
                for item in visible_candidates
            ):
                raise RuntimeError(
                    "Associacao ambigua de campos manuais na aba Faturas; "
                    "sincronizacao cancelada antes da reescrita."
                )

        archived_state_exists = bool(
            key in records
            or (all(identity) and archive_identity_buckets.get(identity))
        )
        if record is None:
            record = records.get(key)
            if record is None and all(identity):
                archive_candidates = archive_identity_buckets.get(identity, [])
                if source_identity_is_unique:
                    record = records_by_identity.get(identity)
                if record is None and any(
                    _has_manual_payload(item.manual_values)
                    for item in archive_candidates
                ):
                    raise RuntimeError(
                        "Associacao ambigua no arquivo de campos manuais; "
                        "sincronizacao cancelada antes da reescrita."
                    )

        if record is not None:
            _apply_manual_values(row_out, record.manual_values)
            if _has_manual_payload(record.manual_values):
                preserved += 1

        if record is not None and (
            _has_manual_payload(record.manual_values) or archived_state_exists
        ):
            if source_identity_is_unique:
                _remove_records_by_identity(
                    updated_records,
                    updated_identity_keys,
                    identity,
                    except_key=key,
                )
            _store_record(
                updated_records,
                updated_identity_keys,
                key,
                _record_from_target_row(row_out),
            )
        projected.append(row_out)
    return projected, preserved, updated_records


def _ensure_rows(ws, required_rows: int) -> None:
    if ws.row_count < required_rows:
        _retry(ws.resize, rows=required_rows, is_write=True)


def _write_matrix(ws, start_row: int, rows: list[list[object]], end_col: str) -> None:
    for offset in range(0, len(rows), MIRROR_WRITE_CHUNK_SIZE):
        chunk = rows[offset:offset + MIRROR_WRITE_CHUNK_SIZE]
        first = start_row + offset
        last = first + len(chunk) - 1
        _retry(
            ws.batch_update,
            [{"range": f"A{first}:{end_col}{last}", "values": chunk}],
            value_input_option="RAW",
            is_write=True,
        )


def _checkbox_value(value: object) -> object:
    if isinstance(value, bool):
        return value
    text = _text(value).strip().upper()
    if text == "TRUE":
        return True
    if text == "FALSE":
        return False
    return "" if not text else value


def _ensure_concluido_checkbox(target_ws) -> None:
    _retry(
        target_ws.add_validation,
        "O2:O",
        ValidationConditionType.boolean,
        [],
        strict=True,
        showCustomUi=True,
        is_write=True,
    )


def _write_full_target(
    target_ws,
    headers: list[str],
    rows: list[list[str]],
    previous_row_count: int,
) -> None:
    output: list[list[object]] = [headers]
    for row in rows:
        row_out: list[object] = list(row)
        row_out[MIRROR_CHECKBOX_INDEX] = _checkbox_value(
            row_out[MIRROR_CHECKBOX_INDEX]
        )
        output.append(row_out)
    _ensure_rows(target_ws, max(len(output), 1))
    _write_matrix(target_ws, 1, output, "R")
    _ensure_concluido_checkbox(target_ws)

    if previous_row_count > len(rows):
        _retry(
            target_ws.batch_clear,
            [f"A{len(rows) + 2}:R{previous_row_count + 1}"],
            is_write=True,
        )


def _write_delta_target(
    target_ws,
    current_rows: list[list[str]],
    projected_rows: list[list[str]],
) -> int:
    changed: list[tuple[int, list[str]]] = []
    for index, (current, expected) in enumerate(zip(current_rows, projected_rows), start=2):
        current_normalized = _normalize_row(current, MIRROR_COLUMN_COUNT)
        # M e O sao manuais e nunca entram numa escrita de delta.
        current_computed = _computed_values(current_normalized)
        expected_computed = _computed_values(expected)
        if current_computed != expected_computed:
            changed.append((index, expected))

    for offset in range(0, len(changed), MIRROR_WRITE_CHUNK_SIZE):
        chunk = changed[offset:offset + MIRROR_WRITE_CHUNK_SIZE]
        updates: list[dict[str, object]] = []
        for sheet_row, row in chunk:
            updates.append({
                "range": f"A{sheet_row}:L{sheet_row}",
                "values": [row[:MIRROR_OBSERVATIONS_INDEX]],
            })
            updates.append({
                "range": f"N{sheet_row}",
                "values": [[row[MIRROR_BILLING_STATUS_INDEX]]],
            })
            updates.append({
                "range": f"P{sheet_row}:R{sheet_row}",
                "values": [[
                    row[MIRROR_LOGIN_INDEX],
                    row[MIRROR_PASSWORD_INDEX],
                    row[MIRROR_INVOICE_ID_INDEX],
                ]],
            })
        _retry(
            target_ws.batch_update,
            updates,
            value_input_option="RAW",
            is_write=True,
        )
    return len(changed)


def _archive_rows(records: dict[str, ProtectedRecord]) -> list[list[str]]:
    return [records[key].as_archive_row(key) for key in sorted(records)]


def _write_archive_if_changed(
    archive_ws,
    records: dict[str, ProtectedRecord],
    existing_grid: list[list[str]],
) -> None:
    expected = [ARCHIVE_HEADERS, *_archive_rows(records)]
    normalized_existing = [
        _normalize_row(row, ARCHIVE_READ_WIDTH) for row in existing_grid
    ]
    normalized_expected = [
        _normalize_row(row, ARCHIVE_READ_WIDTH) for row in expected
    ]
    if normalized_existing == normalized_expected:
        return

    previous_data_count = max(len(normalized_existing) - 1, 0)
    _ensure_rows(archive_ws, max(len(expected), 1))
    _write_matrix(archive_ws, 1, expected, "G")
    if previous_data_count > len(records):
        _retry(
            archive_ws.batch_clear,
            [f"A{len(records) + 2}:G{previous_data_count + 1}"],
            is_write=True,
        )


def sync_worksheets(
    source_ws,
    target_ws,
    archive_ws,
    *,
    mode: Literal["full", "delta"],
) -> MirrorResult:
    """Sincroniza worksheets ja resolvidas; util para testes sem rede."""
    headers, source_rows = _read_source(source_ws)
    target_raw_grid = _read_grid(target_ws, "A:R", MIRROR_COLUMN_COUNT)
    target_grid = _normalize_target_grid(target_raw_grid)
    target_headers = target_grid[0] if target_grid else []
    target_rows = target_grid[1:] if target_grid else []

    archive_grid = _read_grid(archive_ws, "A:K", ARCHIVE_READ_WIDTH)
    archive_records = _read_archive(archive_ws) if archive_grid else {}
    visible_records = _records_from_target_rows(target_rows)
    archive_records = _merge_current_observations(archive_records, target_rows)
    projected_rows, preserved, archive_records = _project_observations(
        source_rows,
        archive_records,
        visible_records,
    )

    source_keys = _make_row_keys(projected_rows)
    target_keys = _make_row_keys(target_rows)
    protected_values_need_rewrite = any(
        _manual_values_from_row(current) != _manual_values_from_row(expected)
        for current, expected in zip(target_rows, projected_rows)
    )
    needs_rewrite = (
        target_headers != headers
        or source_keys != target_keys
        or len(source_rows) != len(target_rows)
        or protected_values_need_rewrite
        or any(
            _computed_values(current) != _computed_values(expected)
            for current, expected in zip(target_rows, projected_rows)
        )
    )
    # O destino Faturas e espelho da origem Faturamento. Mesmo no ciclo delta,
    # quando houver qualquer diferenca, a aba inteira e reconstruida com
    # preservacao das colunas manuais M/O por chave.
    structural_rewrite = mode == "full" or needs_rewrite

    # Uma origem vazia inesperada nunca apaga um espelho ja populado.
    if not source_rows and target_rows:
        raise RuntimeError(
            "A origem retornou zero linhas enquanto o destino possui dados; "
            "sincronizacao cancelada para proteger as colunas M e O."
        )

    # Arquiva primeiro. Assim, uma falha durante a reescrita estrutural nunca
    # remove do ultimo armazenamento persistente a observacao de uma linha orfa.
    _write_archive_if_changed(archive_ws, archive_records, archive_grid)

    if structural_rewrite:
        _write_full_target(target_ws, headers, projected_rows, len(target_rows))
        changed_rows = len(projected_rows)
    else:
        changed_rows = _write_delta_target(target_ws, target_rows, projected_rows)

    return MirrorResult(
        mode=mode,
        source_rows=len(source_rows),
        changed_rows=changed_rows,
        structural_rewrite=structural_rewrite,
        preserved_observations=preserved,
        archived_observations=len(archive_records),
    )


def _get_archive_worksheet(target_spreadsheet):
    try:
        return _retry(
            target_spreadsheet.worksheet,
            INVOICE_CAPTURE_OBSERVATIONS_TAB_NAME,
        )
    except gspread.exceptions.WorksheetNotFound:
        archive_ws = _retry(
            target_spreadsheet.add_worksheet,
            title=INVOICE_CAPTURE_OBSERVATIONS_TAB_NAME,
            rows=1000,
            cols=len(ARCHIVE_HEADERS),
            is_write=True,
        )
        try:
            _retry(archive_ws.hide, is_write=True)
        except Exception:
            logger.warning("Nao foi possivel ocultar a aba tecnica de observacoes.", exc_info=True)
        return archive_ws


def sync_invoice_capture_mirror(
    mode: Literal["full", "delta"],
) -> MirrorResult:
    if mode not in {"full", "delta"}:
        raise ValueError("mode deve ser 'full' ou 'delta'.")
    if not INVOICE_CAPTURE_SPREADSHEET_ID:
        raise RuntimeError("INVOICE_CAPTURE_SPREADSHEET_ID nao configurado.")
    if SHEET_TAB_NAME != INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME:
        raise RuntimeError(
            "Espelhamento bloqueado: o ciclo ativo usa a aba "
            f"'{SHEET_TAB_NAME}', mas a unica origem permitida e "
            f"'{INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME}'."
        )
    if SPREADSHEET_ID == INVOICE_CAPTURE_SPREADSHEET_ID:
        raise RuntimeError(
            "A planilha de origem e destino possuem o mesmo ID; "
            "espelhamento bloqueado para proteger o faturamento."
        )

    client = _get_client()
    source_spreadsheet = _retry(client.open_by_key, SPREADSHEET_ID)
    target_spreadsheet = _retry(client.open_by_key, INVOICE_CAPTURE_SPREADSHEET_ID)

    # A origem e lida para montar o espelho. Depois do espelho pronto, somente
    # a coluna Captura de Faturas recebe o status calculado em Faturas!O.
    source_ws = _retry(
        source_spreadsheet.worksheet,
        INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME,
    )
    target_ws = _retry(target_spreadsheet.worksheet, INVOICE_CAPTURE_SHEET_TAB_NAME)
    archive_ws = _get_archive_worksheet(target_spreadsheet)

    result = sync_worksheets(
        source_ws,
        target_ws,
        archive_ws,
        mode=mode,
    )
    synced_capture_status = _sync_capture_status_to_source(
        source_ws,
        target_ws,
        result.source_rows,
    )
    logger.info(
        "Espelho %s concluido: origem=%d, alteradas=%d, rewrite=%s, "
        "observacoes_preservadas=%d, observacoes_arquivadas=%d, "
        "captura_faturas_sincronizadas=%d.",
        mode,
        result.source_rows,
        result.changed_rows,
        result.structural_rewrite,
        result.preserved_observations,
        result.archived_observations,
        synced_capture_status,
    )
    return result


def validate_invoice_capture_mirror(limit: int | None = None) -> dict[str, object]:
    """Valida origem/destino sem criar abas e sem executar qualquer escrita."""
    if SPREADSHEET_ID == INVOICE_CAPTURE_SPREADSHEET_ID:
        raise RuntimeError("Origem e destino possuem o mesmo ID.")
    if SHEET_TAB_NAME != INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME:
        raise RuntimeError(
            f"A única origem permitida é '{INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME}'."
        )
    if limit is not None and limit <= 0:
        raise ValueError("limit deve ser maior que zero.")

    client = _get_client()
    source_spreadsheet = _retry(client.open_by_key, SPREADSHEET_ID)
    target_spreadsheet = _retry(client.open_by_key, INVOICE_CAPTURE_SPREADSHEET_ID)
    source_ws = _retry(source_spreadsheet.worksheet, INVOICE_CAPTURE_SOURCE_SHEET_TAB_NAME)
    target_ws = _retry(target_spreadsheet.worksheet, INVOICE_CAPTURE_SHEET_TAB_NAME)

    headers, source_rows = _read_source(source_ws)
    target_grid = _normalize_target_grid(
        _read_grid(target_ws, "A:R", MIRROR_COLUMN_COUNT)
    )
    target_headers = target_grid[0] if target_grid else []
    target_rows = target_grid[1:] if target_grid else []

    archive_status = "ausente"
    try:
        archive_ws = _retry(
            target_spreadsheet.worksheet,
            INVOICE_CAPTURE_OBSERVATIONS_TAB_NAME,
        )
        archive_grid = _read_grid(archive_ws, "A:K", ARCHIVE_READ_WIDTH)
        archive_header = archive_grid[0] if archive_grid else []
        known_header = (
            archive_header[:len(ARCHIVE_HEADERS)] == ARCHIVE_HEADERS
            or archive_header[:len(PREVIOUS_CURRENT_ARCHIVE_HEADERS)]
            == PREVIOUS_CURRENT_ARCHIVE_HEADERS
            or archive_header[:len(PREVIOUS_EXTRA_MANUAL_ARCHIVE_HEADERS)]
            == PREVIOUS_EXTRA_MANUAL_ARCHIVE_HEADERS
            or archive_header[:len(PREVIOUS_L_TO_N_ARCHIVE_HEADERS)]
            == PREVIOUS_L_TO_N_ARCHIVE_HEADERS
            or archive_header[:len(PREVIOUS_J_TO_N_ARCHIVE_HEADERS)]
            == PREVIOUS_J_TO_N_ARCHIVE_HEADERS
            or archive_header[:len(LEGACY_I_TO_N_ARCHIVE_HEADERS)]
            == LEGACY_I_TO_N_ARCHIVE_HEADERS
            or archive_header[:len(LEGACY_OBSERVATION_ARCHIVE_HEADERS)]
            == LEGACY_OBSERVATION_ARCHIVE_HEADERS
        )
        if archive_grid and not known_header:
            raise RuntimeError("A aba técnica de campos protegidos possui cabeçalho inesperado.")
        archive_status = "válida" if archive_grid else "vazia"
    except gspread.exceptions.WorksheetNotFound:
        pass

    sample_rows = source_rows[:limit] if limit is not None else source_rows
    result = {
        "source_rows": len(source_rows),
        "validated_rows": len(sample_rows),
        "target_rows": len(target_rows),
        "headers_match": bool(target_headers) and target_headers == headers,
        "archive_status": archive_status,
    }
    logger.info(
        "Validação do espelho: origem=%d, validadas=%d, destino=%d, "
        "headers=%s, arquivo_observações=%s. Nenhuma escrita executada.",
        result["source_rows"],
        result["validated_rows"],
        result["target_rows"],
        "OK" if result["headers_match"] else "DIVERGENTE",
        result["archive_status"],
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Espelha dados calculados do faturamento na aba Faturas."
    )
    parser.add_argument("--mode", choices=("full", "delta"), default="full")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Valida a configuração e os dados sem escrever na planilha.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limita as linhas verificadas no modo --validate-only.",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if args.limit is not None and not args.validate_only:
        parser.error("--limit só pode ser usado com --validate-only")
    if args.validate_only:
        validate_invoice_capture_mirror(args.limit)
    else:
        sync_invoice_capture_mirror(args.mode)


if __name__ == "__main__":
    main()
