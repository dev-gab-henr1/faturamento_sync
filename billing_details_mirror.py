"""Espelho limitado de detalhes de faturamento para testes.

A origem e sempre a aba Faturamento do sync principal, tratada como somente
leitura. O destino e uma planilha separada para validar novas colunas sem
alterar o faturamento em producao.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from clickup_client import _get_session as _get_clickup_session
from clickup_client import get_custom_field_options
from config import (
    BILLING_DETAILS_MIRROR_CACHE_TAB_NAME,
    BILLING_DETAILS_MIRROR_SHEET_TAB_NAME,
    BILLING_DETAILS_MIRROR_SPREADSHEET_ID,
    CLICKUP_BASE_URL,
    SHEET_TAB_NAME,
    SPREADSHEET_ID,
)
from field_map import COLUMN_ORDER
from powerrev_client import fetch_invoice_detail
from sheets_manager import _get_client, _retry
from stats import stats


logger = logging.getLogger("billing_details_mirror")

TARGET_SPREADSHEET_ID = os.getenv(
    "BILLING_DETAILS_MIRROR_SPREADSHEET_ID",
    BILLING_DETAILS_MIRROR_SPREADSHEET_ID,
).strip()
TARGET_SHEET_TAB_NAME = os.getenv(
    "BILLING_DETAILS_MIRROR_SHEET_TAB_NAME",
    BILLING_DETAILS_MIRROR_SHEET_TAB_NAME,
).strip() or "Teste"
CACHE_SHEET_TAB_NAME = os.getenv(
    "BILLING_DETAILS_MIRROR_CACHE_TAB_NAME",
    BILLING_DETAILS_MIRROR_CACHE_TAB_NAME,
).strip() or "__Billing_Details_Cache"
PRODUCT_FIELD_ID = "62193781-2249-49c1-a95d-80df43d66971"

HEADERS = [
    "Status Detalhado",
    "UC",
    "UC Aneel",
    "Razão Social",
    "Mês de Referencia",
    "Envio do boleto",
    "Data de Vencimento",
    "Mês de atandimento",
    "Plano de Adesão",
    "Distribuidora",
    "Valor do boleto",
    "Data de Emissão da fatura",
    "Provider",
    "Status de faturamento",
    "Produto",
    "Data de Pagamento",
    "Data de vencimento Fatura Sion",
    "Consumo Total",
    "Energia compensada",
    "R$ Compensados",
]

SOURCE_KEYS = [
    "task_id",
    "status",
    "uc",
    "uc_aneel",
    "razao_social",
    "mes_referencia",
    "envio_boleto",
    "data_vencimento",
    "mes_atendimento",
    "plano",
    "distribuidora",
    "valor_boleto",
    "data_emissao_fatura",
    "provider_name",
    "status_faturamento",
    "invoice_id",
]

SOURCE_INDEX = {key: COLUMN_ORDER.index(key) for key in SOURCE_KEYS}
SOURCE_READ_LAST_COL = "AA"  # esquema atual do Faturamento: A:AA
TARGET_LAST_COL = "T"
WRITE_CHUNK_SIZE = 500
CACHE_HEADERS = [
    "Invoice ID",
    "Data de vencimento Fatura Sion",
    "Consumo Total",
    "Energia compensada",
    "R$ Compensados",
]
CACHE_LAST_COL = "E"


def _env_float(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


POWERREV_DETAIL_REQUEST_INTERVAL_SECONDS = _env_float(
    "BILLING_DETAILS_POWERREV_DETAIL_INTERVAL_SECONDS",
    5.0,
)
POWERREV_DETAIL_BATCH_SIZE = _env_int(
    "BILLING_DETAILS_POWERREV_DETAIL_BATCH_SIZE",
    100,
)
POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS = _env_float(
    "BILLING_DETAILS_POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS",
    60.0,
)

CONSUMPTION_HFP_CODES = ("QT_ENERGIA_CONSUMIDA_HFP_KWH", "QT_ENERGIA_CONSUMIDA_HFP")
CONSUMPTION_HP_CODES = ("QT_ENERGIA_CONSUMIDA_HP_KWH", "QT_CONSUMIDA_HP_KWH")
COMPENSATED_HFP_CODES = ("QT_ENERGIA_COMPENSADA_HFP_KWH", "QT_ENERGIA_COMPENSADA_HFP")
COMPENSATED_HP_CODES = ("QT_ENERGIA_COMPENSADA_HP_KWH", "QT_COMPENSADA_HP_KWH")
COMPENSATED_VALUE_HFP_CODES = ("VL_ENERGIA_COMPENSADA_HFP_RS", "VL_COMPENSADA_HFP_RS")
COMPENSATED_VALUE_HP_CODES = ("VL_ENERGIA_COMPENSADA_HP_RS", "VL_COMPENSADA_HP_RS")

_PRODUCT_BY_TASK_ID: dict[str, str] = {}
_PRODUCT_CACHE_COMPLETE = False


@dataclass(frozen=True)
class SourceRow:
    sheet_row: int
    task_link: str
    task_id: str
    status: str
    uc: str
    uc_aneel: str
    razao_social: str
    mes_referencia: str
    envio_boleto: str
    data_vencimento: str
    mes_atendimento: str
    plano: str
    distribuidora: str
    valor_boleto: str
    data_emissao_fatura: str
    provider: str
    status_faturamento: str
    invoice_id: str
    month_token: int


@dataclass(frozen=True)
class PowerRevMetrics:
    due_date: str = ""
    consumo_total: str = ""
    energia_compensada: str = ""
    reais_compensados: str = ""


def _cell(row: list[Any], index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    value = row[index]
    return "" if value is None else str(value).strip()


def _task_id_from_link(value: str) -> str:
    text = str(value or "").strip().rstrip("/")
    if not text:
        return ""
    text = text.split("?", 1)[0].rstrip("/")
    return text.rsplit("/", 1)[-1]


def _normalize_text(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _parse_month_token(value: str) -> int | None:
    text = _normalize_text(value)
    match = re.search(r"\b(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\.?\s*/\s*(\d{4})\b", text)
    if not match:
        return None
    month_map = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    month = month_map[match.group(1)]
    year = int(match.group(2))
    return year * 100 + month


def _is_real_invoice_id(value: str) -> bool:
    return bool(str(value or "").strip().isdigit())


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "nan"}:
        return None
    text = text.replace("R$", "").replace("%", "").strip()
    text = re.sub(r"\s+", "", text)

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def _format_decimal_pt(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{value:,.2f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def _format_kwh_pt(value: float | None) -> str:
    formatted = _format_decimal_pt(value)
    return f"{formatted} kWh" if formatted else ""


def _format_currency_pt(value: float | None) -> str:
    if value is None:
        return ""
    return f"R$ {_format_decimal_pt(abs(value))}"


def _format_date_pt(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        year, month, day = match.groups()
        return f"{day}/{month}/{year}"
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return text


def _read_source_rows(limit_scan: int | None = None) -> list[SourceRow]:
    client = _get_client()
    source_ws = _retry(
        client.open_by_key(SPREADSHEET_ID).worksheet,
        SHEET_TAB_NAME,
    )
    values = _retry(
        source_ws.get,
        f"A:{SOURCE_READ_LAST_COL}",
        value_render_option="FORMATTED_VALUE",
    )
    rows = values[1:] if values else []
    if limit_scan:
        rows = rows[:limit_scan]

    out: list[SourceRow] = []
    for offset, row in enumerate(rows, start=2):
        if not any(str(value or "").strip() for value in row):
            continue
        month_token = _parse_month_token(_cell(row, SOURCE_INDEX["mes_referencia"]))
        if month_token is None:
            continue
        task_link = _cell(row, SOURCE_INDEX["task_id"])
        out.append(
            SourceRow(
                sheet_row=offset,
                task_link=task_link,
                task_id=_task_id_from_link(task_link),
                status=_cell(row, SOURCE_INDEX["status"]),
                uc=_cell(row, SOURCE_INDEX["uc"]),
                uc_aneel=_cell(row, SOURCE_INDEX["uc_aneel"]),
                razao_social=_cell(row, SOURCE_INDEX["razao_social"]),
                mes_referencia=_cell(row, SOURCE_INDEX["mes_referencia"]),
                envio_boleto=_cell(row, SOURCE_INDEX["envio_boleto"]),
                data_vencimento=_cell(row, SOURCE_INDEX["data_vencimento"]),
                mes_atendimento=_cell(row, SOURCE_INDEX["mes_atendimento"]),
                plano=_cell(row, SOURCE_INDEX["plano"]),
                distribuidora=_cell(row, SOURCE_INDEX["distribuidora"]),
                valor_boleto=_cell(row, SOURCE_INDEX["valor_boleto"]),
                data_emissao_fatura=_cell(row, SOURCE_INDEX["data_emissao_fatura"]),
                provider=_cell(row, SOURCE_INDEX["provider_name"]),
                status_faturamento=_cell(row, SOURCE_INDEX["status_faturamento"]),
                invoice_id=_cell(row, SOURCE_INDEX["invoice_id"]),
                month_token=month_token,
            )
        )
    return out


def _round_robin_by_month(rows: Iterable[SourceRow], used_ucs: set[str], limit: int) -> list[SourceRow]:
    grouped: dict[int, list[SourceRow]] = defaultdict(list)
    for row in rows:
        grouped[row.month_token].append(row)
    for group in grouped.values():
        group.sort(key=lambda item: item.sheet_row)

    selected: list[SourceRow] = []
    months = sorted(grouped.keys(), reverse=True)
    while len(selected) < limit:
        progressed = False
        for month in months:
            bucket = grouped[month]
            while bucket:
                candidate = bucket.pop(0)
                uc_key = candidate.uc.strip()
                if not uc_key or uc_key in used_ucs:
                    continue
                selected.append(candidate)
                used_ucs.add(uc_key)
                progressed = True
                break
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected


def select_recent_2026_rows(rows: Iterable[SourceRow], limit: int = 25) -> list[SourceRow]:
    valid = [
        row for row in rows
        if 202601 <= row.month_token <= 202612 and row.uc.strip()
    ]
    valid.sort(key=lambda item: (-item.month_token, item.sheet_row))

    used_ucs: set[str] = set()
    real_invoice_rows = [row for row in valid if _is_real_invoice_id(row.invoice_id)]
    selected = _round_robin_by_month(real_invoice_rows, used_ucs, limit)

    if len(selected) < limit:
        remaining = [row for row in valid if row not in selected]
        selected.extend(_round_robin_by_month(remaining, used_ucs, limit - len(selected)))

    selected.sort(key=lambda item: (-item.month_token, item.sheet_row))
    return selected[:limit]


def select_mirror_rows(rows: Iterable[SourceRow], limit: int | None = None) -> list[SourceRow]:
    """Retorna todas as linhas do espelho ou uma amostra limitada para testes."""
    source_rows = [row for row in rows if row.uc.strip()]
    if limit and limit > 0:
        return select_recent_2026_rows(source_rows, limit=limit)
    return source_rows


def _extract_dropdown_label(raw_value: Any, options: list[dict]) -> str:
    if raw_value is None or raw_value == "":
        return ""
    if isinstance(raw_value, dict):
        for key in ("name", "label", "value"):
            if raw_value.get(key):
                return str(raw_value[key]).strip()
        raw_value = raw_value.get("id") or raw_value.get("orderindex")

    raw_text = str(raw_value).strip()
    for option in options:
        option_id = str(option.get("id", "")).strip()
        option_name = str(option.get("name", "")).strip()
        option_order = str(option.get("orderindex", "")).strip()
        if raw_text in {option_id, option_name, option_order}:
            return option_name or raw_text

    return raw_text


def _resolve_product_from_task(task: dict) -> str:
    for cf in task.get("custom_fields", []):
        if cf.get("id") != PRODUCT_FIELD_ID:
            continue
        options = cf.get("type_config", {}).get("options", []) or []
        if not options:
            options = get_custom_field_options(PRODUCT_FIELD_ID)
        return _extract_dropdown_label(cf.get("value"), options)
    return ""


def cache_clickup_products(
    tasks: Iterable[dict],
    *,
    complete: bool = False,
) -> None:
    """Armazena Produto por task id para o espelho usar sem chamadas extras."""
    global _PRODUCT_CACHE_COMPLETE
    for task in tasks:
        task_id = str(task.get("id", "") or "").strip()
        if task_id:
            _PRODUCT_BY_TASK_ID[task_id] = _resolve_product_from_task(task)
    if complete:
        _PRODUCT_CACHE_COMPLETE = True


def fetch_products_by_task_id(task_ids: Iterable[str]) -> dict[str, str]:
    session = _get_clickup_session()
    resolved: dict[str, str] = {}
    unique_task_ids = [task_id for task_id in dict.fromkeys(task_ids) if task_id]

    for task_id in unique_task_ids:
        if task_id in _PRODUCT_BY_TASK_ID:
            resolved[task_id] = _PRODUCT_BY_TASK_ID[task_id]

    if _PRODUCT_CACHE_COMPLETE:
        for task_id in unique_task_ids:
            resolved.setdefault(task_id, "")
        return resolved

    for task_id in unique_task_ids:
        if task_id in resolved:
            continue
        url = f"{CLICKUP_BASE_URL}/task/{task_id}"
        try:
            resp = session.get(
                url,
                params={"custom_task_ids": "false", "include_subtasks": "false"},
                timeout=30,
            )
            stats.clickup_requests += 1
            resp.raise_for_status()
            resolved[task_id] = _resolve_product_from_task(resp.json())
            _PRODUCT_BY_TASK_ID[task_id] = resolved[task_id]
        except Exception as exc:
            logger.warning("Produto ClickUp nao recuperado para task %s: %s", task_id, exc)
            resolved[task_id] = ""
        time.sleep(0.1)

    return resolved


def _set_metric(metrics: dict[str, float], code: Any, value: Any) -> None:
    code_text = str(code or "").strip().upper()
    if not code_text or not (code_text.startswith("QT_") or code_text.startswith("VL_")):
        return
    number = _to_number(value)
    if number is None:
        return
    metrics.setdefault(code_text, number)


def _extract_measurements_from_formula(formula_payload: dict[str, Any], metrics: dict[str, float]) -> None:
    measurements = formula_payload.get("medicoes")
    if isinstance(measurements, list):
        for item in measurements:
            if not isinstance(item, dict):
                continue
            for code, payload in item.items():
                if isinstance(payload, dict):
                    value = (
                        payload.get("vl_medicao")
                        if "vl_medicao" in payload
                        else payload.get("value")
                    )
                else:
                    value = payload
                _set_metric(metrics, code, value)

    formula_code = formula_payload.get("formula")
    if isinstance(formula_code, str) and re.fullmatch(r"[A-Z0-9_]+", formula_code.strip()):
        value = formula_payload.get("resultado")
        if value is None:
            value = formula_payload.get("valor")
        _set_metric(metrics, formula_code, value)


def _walk_metric_values(node: Any, metrics: dict[str, float]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            key_text = str(key or "").strip().upper()
            if key_text.startswith(("QT_", "VL_")):
                if isinstance(value, dict):
                    candidate = (
                        value.get("vl_medicao")
                        if "vl_medicao" in value
                        else value.get("value")
                    )
                    if candidate is None:
                        candidate = value.get("resultado")
                else:
                    candidate = value
                _set_metric(metrics, key_text, candidate)
            _walk_metric_values(value, metrics)
    elif isinstance(node, list):
        for item in node:
            _walk_metric_values(item, metrics)


def collect_powerrev_metrics(detail: dict[str, Any]) -> dict[str, float]:
    metrics: dict[str, float] = {}
    formulas = detail.get("formulasCobranca")
    if isinstance(formulas, list):
        for entry in formulas:
            if not isinstance(entry, dict):
                continue
            formula_payload = entry.get("formulas")
            if isinstance(formula_payload, dict):
                _extract_measurements_from_formula(formula_payload, metrics)

    _walk_metric_values(detail, metrics)
    return metrics


def _first_metric(metrics: dict[str, float], codes: Iterable[str]) -> float | None:
    for code in codes:
        value = metrics.get(code)
        if value is not None:
            return value
    return None


def _sum_present(values: Iterable[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(abs(value) for value in present)


def extract_powerrev_detail_fields(detail: dict[str, Any]) -> PowerRevMetrics:
    metrics = collect_powerrev_metrics(detail)

    consumo_total = _sum_present([
        _first_metric(metrics, CONSUMPTION_HFP_CODES),
        _first_metric(metrics, CONSUMPTION_HP_CODES),
    ])
    energia_compensada = _sum_present([
        _first_metric(metrics, COMPENSATED_HFP_CODES),
        _first_metric(metrics, COMPENSATED_HP_CODES),
    ])

    compensated_value_parts: list[float | None] = [
        _first_metric(metrics, COMPENSATED_VALUE_HFP_CODES),
        _first_metric(metrics, COMPENSATED_VALUE_HP_CODES),
    ]
    compensated_value_parts.extend(
        value for code, value in metrics.items()
        if code.startswith("VL_COMPENSADA_BAND_")
    )
    reais_compensados = _sum_present(compensated_value_parts)

    return PowerRevMetrics(
        due_date=_format_date_pt(detail.get("dueDate")),
        consumo_total=_format_kwh_pt(consumo_total),
        energia_compensada=_format_kwh_pt(energia_compensada),
        reais_compensados=_format_currency_pt(reais_compensados),
    )


def _metrics_from_cache_row(row: list[Any]) -> tuple[str, PowerRevMetrics] | None:
    values = ["" if value is None else str(value).strip() for value in row]
    if not values or not values[0]:
        return None
    values.extend([""] * (len(CACHE_HEADERS) - len(values)))
    return values[0], PowerRevMetrics(
        due_date=values[1],
        consumo_total=values[2],
        energia_compensada=values[3],
        reais_compensados=values[4],
    )


def _open_cache_worksheet(spreadsheet_id: str, *, create: bool):
    client = _get_client()
    spreadsheet = _retry(client.open_by_key, spreadsheet_id)
    try:
        return _retry(spreadsheet.worksheet, CACHE_SHEET_TAB_NAME)
    except Exception:
        if not create:
            return None
        return _retry(
            spreadsheet.add_worksheet,
            title=CACHE_SHEET_TAB_NAME,
            rows=1000,
            cols=len(CACHE_HEADERS),
            is_write=True,
        )


def _read_detail_cache(spreadsheet_id: str, *, create: bool = False) -> dict[str, PowerRevMetrics]:
    try:
        cache_ws = _open_cache_worksheet(spreadsheet_id, create=create)
        if cache_ws is None:
            return {}
        values = _retry(
            cache_ws.get,
            f"A:{CACHE_LAST_COL}",
            value_render_option="FORMATTED_VALUE",
        )
    except Exception:
        logger.warning("Cache de detalhes PowerRev indisponivel; seguindo sem cache.", exc_info=True)
        return {}

    rows = values[1:] if values else []
    cache: dict[str, PowerRevMetrics] = {}
    for row in rows:
        parsed = _metrics_from_cache_row(row)
        if parsed:
            invoice_id, metrics = parsed
            cache[invoice_id] = metrics
    return cache


def _write_detail_cache(
    spreadsheet_id: str,
    cache: dict[str, PowerRevMetrics],
) -> None:
    cache_ws = _open_cache_worksheet(spreadsheet_id, create=True)
    rows = [
        [
            invoice_id,
            metrics.due_date,
            metrics.consumo_total,
            metrics.energia_compensada,
            metrics.reais_compensados,
        ]
        for invoice_id, metrics in sorted(cache.items(), key=lambda item: int(item[0]))
    ]
    output = [CACHE_HEADERS] + rows
    for offset in range(0, len(output), WRITE_CHUNK_SIZE):
        chunk = output[offset:offset + WRITE_CHUNK_SIZE]
        first = 1 + offset
        last = first + len(chunk) - 1
        _retry(
            cache_ws.batch_update,
            [{"range": f"A{first}:{CACHE_LAST_COL}{last}", "values": chunk}],
            value_input_option="RAW",
            is_write=True,
        )


def fetch_powerrev_details_by_invoice_id(invoice_ids: Iterable[str]) -> dict[str, PowerRevMetrics]:
    resolved: dict[str, PowerRevMetrics] = {}
    unique_invoice_ids = [value for value in dict.fromkeys(invoice_ids) if _is_real_invoice_id(value)]
    total = len(unique_invoice_ids)
    if total:
        logger.info(
            "Detalhes PowerRev: buscando %d invoices com intervalo de %.1fs e cooldown de %.1fs a cada %d requests.",
            total,
            POWERREV_DETAIL_REQUEST_INTERVAL_SECONDS,
            POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS,
            POWERREV_DETAIL_BATCH_SIZE,
        )

    last_request_started_at = 0.0
    for index, invoice_id in enumerate(unique_invoice_ids, start=1):
        if last_request_started_at:
            elapsed = time.monotonic() - last_request_started_at
            wait_seconds = POWERREV_DETAIL_REQUEST_INTERVAL_SECONDS - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)

        last_request_started_at = time.monotonic()
        try:
            detail = fetch_invoice_detail(invoice_id)
            resolved[invoice_id] = extract_powerrev_detail_fields(detail)
        except Exception as exc:
            logger.warning("Detalhe PowerRev nao recuperado para invoice %s: %s", invoice_id, exc)
            resolved[invoice_id] = PowerRevMetrics()

        if index % 100 == 0 or index == total:
            logger.info("Detalhes PowerRev: %d/%d invoices processadas.", index, total)

        if (
            POWERREV_DETAIL_BATCH_SIZE > 0
            and index < total
            and index % POWERREV_DETAIL_BATCH_SIZE == 0
            and POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS > 0
        ):
            logger.info(
                "Detalhes PowerRev: cooldown de %.1fs apos %d requests.",
                POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS,
                index,
            )
            time.sleep(POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS)
            last_request_started_at = 0.0
    return resolved


def fetch_powerrev_details_with_cache(
    invoice_ids: Iterable[str],
    *,
    spreadsheet_id: str,
    persist_cache: bool,
) -> dict[str, PowerRevMetrics]:
    unique_invoice_ids = [
        value for value in dict.fromkeys(invoice_ids)
        if _is_real_invoice_id(value)
    ]
    cache = _read_detail_cache(spreadsheet_id, create=persist_cache)
    resolved = {
        invoice_id: cache[invoice_id]
        for invoice_id in unique_invoice_ids
        if invoice_id in cache
    }
    missing = [
        invoice_id for invoice_id in unique_invoice_ids
        if invoice_id not in resolved
    ]

    logger.info(
        "Detalhes PowerRev: %d em cache, %d pendentes.",
        len(resolved),
        len(missing),
    )
    fetched = fetch_powerrev_details_by_invoice_id(missing)
    resolved.update(fetched)

    if persist_cache and fetched:
        for invoice_id, metrics in fetched.items():
            if any([
                metrics.due_date,
                metrics.consumo_total,
                metrics.energia_compensada,
                metrics.reais_compensados,
            ]):
                cache[invoice_id] = metrics
        _write_detail_cache(spreadsheet_id, cache)
        logger.info("Cache de detalhes PowerRev atualizado: %d invoices.", len(cache))

    return resolved


def build_output_rows(
    selected: Iterable[SourceRow],
    products_by_task_id: dict[str, str],
    details_by_invoice_id: dict[str, PowerRevMetrics],
) -> list[list[str]]:
    rows: list[list[str]] = []
    for item in selected:
        detail = details_by_invoice_id.get(item.invoice_id, PowerRevMetrics())
        rows.append([
            item.status,
            item.uc,
            item.uc_aneel,
            item.razao_social,
            item.mes_referencia,
            item.envio_boleto,
            item.data_vencimento,
            item.mes_atendimento,
            item.plano,
            item.distribuidora,
            item.valor_boleto,
            item.data_emissao_fatura,
            item.provider,
            item.status_faturamento,
            products_by_task_id.get(item.task_id, ""),
            "",  # Data de Pagamento ainda sem origem confiavel na API.
            detail.due_date,
            detail.consumo_total,
            detail.energia_compensada,
            detail.reais_compensados,
        ])
    return rows


def _open_target_worksheet(spreadsheet_id: str, tab_name: str):
    client = _get_client()
    return _retry(
        client.open_by_key(spreadsheet_id).worksheet,
        tab_name,
    )


def write_target_rows(spreadsheet_id: str, tab_name: str, rows: list[list[str]]) -> None:
    target_ws = _open_target_worksheet(spreadsheet_id, tab_name)
    previous_values = _retry(
        target_ws.get,
        f"A:{TARGET_LAST_COL}",
        value_render_option="FORMATTED_VALUE",
    )
    previous_row_count = max(len(previous_values), 1)
    output = [HEADERS] + rows

    for offset in range(0, len(output), WRITE_CHUNK_SIZE):
        chunk = output[offset:offset + WRITE_CHUNK_SIZE]
        first = 1 + offset
        last = first + len(chunk) - 1
        _retry(
            target_ws.batch_update,
            [{"range": f"A{first}:{TARGET_LAST_COL}{last}", "values": chunk}],
            value_input_option="RAW",
            is_write=True,
        )

    if previous_row_count > len(output):
        _retry(
            target_ws.batch_clear,
            [f"A{len(output) + 1}:{TARGET_LAST_COL}{previous_row_count}"],
            is_write=True,
        )


def sync_billing_details_mirror(
    *,
    limit: int | None = None,
    apply: bool = False,
    spreadsheet_id: str = TARGET_SPREADSHEET_ID,
    tab_name: str = TARGET_SHEET_TAB_NAME,
    limit_scan: int | None = None,
) -> list[list[str]]:
    source_rows = _read_source_rows(limit_scan=limit_scan)
    selected = select_mirror_rows(source_rows, limit=limit)
    logger.info(
        "Linhas selecionadas para espelho de detalhes: %d de %d lidas.",
        len(selected),
        len(source_rows),
    )

    by_month: dict[int, int] = defaultdict(int)
    for row in selected:
        by_month[row.month_token] += 1
    logger.info(
        "Distribuicao por mes: %s",
        ", ".join(f"{month}={count}" for month, count in sorted(by_month.items(), reverse=True)),
    )

    products = fetch_products_by_task_id(row.task_id for row in selected)
    details = fetch_powerrev_details_with_cache(
        (row.invoice_id for row in selected),
        spreadsheet_id=spreadsheet_id,
        persist_cache=apply,
    )
    output_rows = build_output_rows(selected, products, details)

    if apply:
        write_target_rows(spreadsheet_id, tab_name, output_rows)
        logger.info("Espelho de detalhes atualizado: %d linhas em %s/%s.", len(output_rows), spreadsheet_id, tab_name)
    else:
        logger.info("[DRY-RUN] Nenhuma escrita realizada em %s/%s.", spreadsheet_id, tab_name)
        for item, output in zip(selected[:10], output_rows[:10]):
            logger.info(
                "[DRY-RUN] linha=%s UC=%s mes=%s invoice=%s produto=%s",
                item.sheet_row,
                item.uc,
                item.mes_referencia,
                item.invoice_id,
                output[14],
            )

    return output_rows


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Espelha detalhes de faturamento para a planilha de detalhes.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Quantidade maxima de cooperados; 0 espelha tudo.",
    )
    parser.add_argument("--apply", action="store_true", help="Grava na planilha destino.")
    parser.add_argument("--spreadsheet-id", default=TARGET_SPREADSHEET_ID)
    parser.add_argument("--tab", default=TARGET_SHEET_TAB_NAME)
    parser.add_argument(
        "--limit-scan",
        type=int,
        default=None,
        help="Limita quantas linhas da origem serao analisadas; util para debug local.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args()
    limit = args.limit if args.limit and args.limit > 0 else None
    sync_billing_details_mirror(
        limit=limit,
        apply=args.apply,
        spreadsheet_id=args.spreadsheet_id,
        tab_name=args.tab,
        limit_scan=args.limit_scan,
    )


if __name__ == "__main__":
    main()
