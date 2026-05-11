"""
Cliente PowerRev API para o projeto Faturamento Sync.
Busca billing/invoices e resolve UCs para matching com a planilha.
"""
import time
import logging
import re
from collections import OrderedDict
from typing import Any

import requests

from config import (
    POWERREV_BASE_URL,
    POWERREV_AUTH_URL,
    POWERREV_ACCOUNT_ID,
    POWERREV_API_KEY,
    POWERREV_TIMEOUT,
    POWERREV_DELAY,
    POWERREV_MAX_RETRIES,
    POWERREV_PAGE_LIMIT,
)
from stats import stats

logger = logging.getLogger(__name__)

_SESSION: requests.Session | None = None
_TOKEN: str | None = None

_UC_BY_ID: dict[str, dict] = {}
_UC_BY_INSTALLATION: dict[str, dict] = {}
_UC_BY_CODE: dict[str, dict] = {}
_ACCOUNT_CACHE: OrderedDict[str, dict] = OrderedDict()
_ACCOUNT_CACHE_MAX = 1500

_STATUS_TRANSLATION = {
    "CANCELED": "Cancelada",
    "PAID": "Paga",
    "ISSUED": "Emitida",
    "CALCULATED": "Calculada",
    "MISSING_CALCULATION": "Cálculo Pendente",
    "MISSING_RULES": "Regras Ausentes",
    "MISSING_MAP_STATUS": "Mapeamento Ausente",
    "CALCULATING": "Calculando",
    "ISSUING": "Emitindo",
    "REISSUED": "Reemitida",
    "ERROR_IN_CALCULATING": "Erro no Cálculo",
    "ERROR_IN_EMISSION": "Erro na Emissão",
    "DEMONSTRATIVE_ONLY": "Apenas Demonstrativo",
    "MISSING_DISTRIBUTOR_INVOICE": "Sem Fatura Distribuidora",
    "WAITING_REGISTER": "Aguardando Cadastro",
    "OVERDUE": "Vencida",
    "EXPIRED": "Expirada",
    "EXTERNALLY_PAID": "Paga Externamente",
    "NEGOTIATED": "Negociada",
    "READY_TO_REISSUE": "Reemissão Calculada",
}


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_token(value: Any) -> str:
    """Token seguro para composição de chaves técnicas."""
    return _safe_str(value).replace("|", "/")


def _synthetic_invoice_id(
    *,
    kind: str,
    uc: str,
    parent_grouped_uc: str,
    issue_date: str,
    provider_name: str,
) -> str:
    """
    Gera ID técnico determinístico para faturas sem id definitivo no PowerRev.
    Mantém estabilidade entre syncs para proteger colunas manuais por linha.
    """
    parts = [
        "SYN",
        kind,
        _safe_token(uc),
        _safe_token(parent_grouped_uc),
        _safe_token(issue_date),
        _safe_token(provider_name),
    ]
    return "|".join(parts)


def _row_key(
    *,
    kind: str,
    uc: str,
    reference_month: str,
    parent_grouped_uc: str,
    status: str,
    issue_date: str,
    total: float | str,
    provider_name: str,
) -> str:
    """
    Chave tecnica secundaria (deterministica) para matching de linhas sem ID definitivo.
    Nao substitui Invoice ID; serve como protecao extra contra colisoes no fallback UC|Mes.
    """
    parts = [
        "RK",
        _safe_token(kind),
        _safe_token(uc),
        _safe_token(reference_month),
        _safe_token(parent_grouped_uc),
        _safe_token(status),
        _safe_token(issue_date),
        _safe_token(total),
        _safe_token(provider_name),
    ]
    return "|".join(parts)


def _to_currency_number(value) -> float | str:
    """Converte valor monetario para numero (float) quando possivel.

    Mantem string apenas como fallback em casos nao parseaveis.
    """
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return ""

    # Remove simbolos de moeda/espacos e preserva apenas digitos/sinais/separadores.
    normalized = re.sub(r"[^0-9,.\-]", "", text)
    if not normalized:
        return ""

    try:
        if "," in normalized and "." in normalized:
            # Decide separador decimal pela ultima ocorrencia.
            if normalized.rfind(",") > normalized.rfind("."):
                normalized = normalized.replace(".", "").replace(",", ".")
            else:
                normalized = normalized.replace(",", "")
        elif "," in normalized:
            normalized = normalized.replace(".", "").replace(",", ".")
        return float(normalized)
    except (ValueError, TypeError):
        return text


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.timeout = POWERREV_TIMEOUT
    return _SESSION


def reset_session() -> None:
    """Fecha session HTTP e limpa token. Próxima chamada re-autentica."""
    global _SESSION, _TOKEN
    if _SESSION is not None:
        try:
            _SESSION.close()
        except Exception:
            pass
        _SESSION = None
    _TOKEN = None
    logger.info("PowerRev session resetada.")


def _authenticate() -> str:
    global _TOKEN
    session = _get_session()

    for attempt in range(POWERREV_MAX_RETRIES):
        try:
            resp = session.post(
                f"{POWERREV_AUTH_URL}/sign",
                json={"accountId": POWERREV_ACCOUNT_ID, "apiKey": POWERREV_API_KEY},
                headers={"Content-Type": "application/json"},
            )
            stats.powerrev_requests += 1
            resp.raise_for_status()
            data = resp.json()
            token = data.get("token") or data.get("accessToken")
            if token:
                _TOKEN = token
                logger.info("PowerRev: autenticação OK.")
                return token
            raise RuntimeError("Token não retornado pela API PowerRev.")
        except requests.RequestException as exc:
            logger.warning("PowerRev auth tentativa %d/%d: %s", attempt + 1, POWERREV_MAX_RETRIES, exc)
            if attempt < POWERREV_MAX_RETRIES - 1:
                time.sleep(POWERREV_DELAY * (attempt + 1))
            else:
                raise
    raise RuntimeError("Falha na autenticação PowerRev.")


def _get_headers() -> dict[str, str]:
    global _TOKEN
    if _TOKEN is None:
        _authenticate()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {_TOKEN}",
    }


def _request(method: str, url: str, **kwargs) -> requests.Response:
    global _TOKEN
    session = _get_session()

    attempt = 0
    while attempt < POWERREV_MAX_RETRIES:
        try:
            kwargs["headers"] = _get_headers()
            resp = session.request(method, url, **kwargs)
            stats.powerrev_requests += 1

            if resp.status_code == 401:
                logger.warning("PowerRev 401, re-autenticando...")
                _TOKEN = None
                _authenticate()
                continue  # não incrementa attempt

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "30"))
                logger.warning("PowerRev rate limit 429, aguardando %ds", retry_after)
                time.sleep(retry_after)
                continue  # não incrementa attempt

            if resp.status_code in (500, 502, 503):
                attempt += 1
                wait = 2 ** attempt
                logger.warning("PowerRev %s (tentativa %d/%d), retry em %ds",
                               resp.status_code, attempt, POWERREV_MAX_RETRIES, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            attempt += 1
            logger.warning("PowerRev tentativa %d/%d: %s", attempt, POWERREV_MAX_RETRIES, exc)
            if attempt < POWERREV_MAX_RETRIES:
                time.sleep(POWERREV_DELAY * attempt)
            else:
                raise
    raise RuntimeError("Falha na requisição PowerRev.")


def _normalize_items(payload: Any) -> tuple[list[dict], int | None, int | None, int | None]:
    if isinstance(payload, list):
        return payload, None, None, None
    if isinstance(payload, dict):
        for key in ("content", "data", "items", "results", "responseList"):
            if key in payload and isinstance(payload[key], list):
                return (
                    payload[key],
                    payload.get("page"),
                    payload.get("total"),
                    payload.get("quantityPerPage"),
                )
    return [], None, None, None


def _load_consumer_units() -> None:
    global _UC_BY_ID, _UC_BY_INSTALLATION, _UC_BY_CODE
    if _UC_BY_ID:
        return

    resp = _request("GET", f"{POWERREV_BASE_URL}/consumer-unit")
    payload = resp.json()
    units = payload if isinstance(payload, list) else []

    _UC_BY_ID = {str(u["idUnidadeConsumo"]): u for u in units if u.get("idUnidadeConsumo") is not None}
    _UC_BY_INSTALLATION = {str(u["nuInstalacao"]): u for u in units if u.get("nuInstalacao")}
    _UC_BY_CODE = {str(u.get("codUnidadeConsumo")): u for u in units if u.get("codUnidadeConsumo")}

    logger.info("PowerRev: %d UCs carregadas.", len(units))


def _resolve_uc_installation(item: dict) -> str | None:
    consumer_units_raw = item.get("consumerUnits")
    keys: list[str] = []

    if isinstance(consumer_units_raw, list):
        for v in consumer_units_raw:
            if isinstance(v, dict):
                recurso = v.get("recurso") if isinstance(v.get("recurso"), dict) else None
                if recurso:
                    for field in ("idUnidadeConsumo", "cdChaveExterna", "noRecurso"):
                        val = recurso.get(field)
                        if val is not None and str(val).strip():
                            keys.append(str(val).strip())
                for field in ("idUnidadeConsumo", "cdChaveExterna", "noRecurso"):
                    val = v.get(field)
                    if val is not None and str(val).strip():
                        keys.append(str(val).strip())
    elif consumer_units_raw is not None:
        raw = str(consumer_units_raw).strip()
        if raw:
            keys.append(raw)

    if not keys:
        account_id = item.get("accountId")
        if account_id is not None:
            cache_key = str(account_id)
            account = _ACCOUNT_CACHE.get(cache_key)
            if account is not None:
                _ACCOUNT_CACHE.move_to_end(cache_key)
            if account is None:
                try:
                    resp = _request("GET", f"{POWERREV_BASE_URL}/billing/account/{account_id}")
                    account = resp.json() if isinstance(resp.json(), dict) else {}
                except requests.RequestException:
                    account = {}
                _ACCOUNT_CACHE[cache_key] = account
                if len(_ACCOUNT_CACHE) > _ACCOUNT_CACHE_MAX:
                    _ACCOUNT_CACHE.popitem(last=False)
            cu_raw = account.get("consumerUnits")
            if isinstance(cu_raw, list):
                for v in cu_raw:
                    if isinstance(v, dict):
                        recurso = v.get("recurso") if isinstance(v.get("recurso"), dict) else None
                        if recurso:
                            for field in ("idUnidadeConsumo", "cdChaveExterna", "noRecurso"):
                                val = recurso.get(field)
                                if val is not None and str(val).strip():
                                    keys.append(str(val).strip())

    for key in keys:
        uc = _UC_BY_ID.get(key) or _UC_BY_INSTALLATION.get(key) or _UC_BY_CODE.get(key)
        if uc and uc.get("nuInstalacao"):
            return str(uc["nuInstalacao"])

    return None


def _format_date(value: Any) -> str:
    if value is None or not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    try:
        from datetime import datetime
        candidate = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(candidate)
        return dt.strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        pass
    return text


def _extract_grouped_consumer_unit(item: dict) -> str:
    """Extrai UC de payload grouped-invoice (mãe/filha)."""
    raw = item.get("consumerUnits")
    if raw is None:
        return ""

    if isinstance(raw, str):
        return raw.strip()

    if isinstance(raw, list):
        for val in raw:
            if isinstance(val, str) and val.strip():
                return val.strip()
            if isinstance(val, dict):
                for field in ("consumerUnitCode", "cdChaveExterna", "nuInstalacao", "code"):
                    candidate = _safe_str(val.get(field))
                    if candidate:
                        return candidate
        return ""

    if isinstance(raw, dict):
        for field in ("consumerUnitCode", "cdChaveExterna", "nuInstalacao", "code"):
            candidate = _safe_str(raw.get(field))
            if candidate:
                return candidate
        return ""

    return _safe_str(raw)


def _invoice_completeness_score(invoice: dict) -> int:
    """Score simples para desempate em colisões de dedupe."""
    score = 0
    for key in (
        "invoiceId",
        "uc",
        "statusRaw",
        "status",
        "issueDate",
        "total",
        "providerName",
        "parentGroupedUc",
    ):
        if _safe_str(invoice.get(key)):
            score += 1
    return score


def _fetch_simple_invoice_items(reference_month: str) -> list[dict]:
    """Busca invoices no endpoint simples (v2 com fallback legado)."""
    items: list[dict] = []
    page = 1
    limit = min(POWERREV_PAGE_LIMIT, 250)

    while True:
        try:
            resp = _request(
                "GET",
                f"{POWERREV_BASE_URL}/billing/invoice/v2",
                params={
                    "referenceMonth": int(reference_month),
                    "page": page,
                    "limit": limit,
                    "countTotal": "false",
                },
            )
            data = resp.json()
            result_items = data.get("content", [])
            pg = data.get("page")
            total = data.get("total")
            qty_per_page = data.get("quantityPerPage", limit)
        except requests.RequestException:
            resp = _request(
                "GET",
                f"{POWERREV_BASE_URL}/billing/invoice",
                params={"referenceMonth": reference_month},
            )
            result_items, _, _, _ = _normalize_items(resp.json())
            items.extend(result_items)
            break

        if not result_items:
            break

        items.extend(result_items)

        if qty_per_page and total is not None:
            total_pages = (total + qty_per_page - 1) // qty_per_page
            if pg is not None and pg >= total_pages:
                break
        elif len(result_items) < qty_per_page:
            break

        page += 1
        time.sleep(POWERREV_DELAY)

    return items


def _fetch_grouped_invoice_rows(reference_month: str) -> list[tuple[str, dict, str]]:
    """Busca cobranças agrupadas e faz flatten mãe + filhas.

    Retorna lista [(kind, item)] onde kind é:
      - "M" (mãe)
      - "C" (filha / innerAccounts)
    """
    rows: list[tuple[str, dict, str]] = []
    page = 1
    limit = min(POWERREV_PAGE_LIMIT, 100)  # endpoint grouped limita em 100

    while True:
        resp = _request(
            "GET",
            f"{POWERREV_BASE_URL}/billing/grouped-invoice/v2",
            params={
                "referenceMonth": int(reference_month),
                "page": page,
                "limit": limit,
            },
        )
        data = resp.json()
        content = data.get("content", []) if isinstance(data, dict) else []
        if not content:
            break

        for mother in content:
            if not isinstance(mother, dict):
                continue
            mother_uc = _extract_grouped_consumer_unit(mother)
            if not mother_uc:
                mother_uc = _resolve_uc_installation(mother) or ""

            rows.append(("M", mother, ""))
            inner = mother.get("innerAccounts", [])
            if isinstance(inner, list):
                for child in inner:
                    if isinstance(child, dict):
                        rows.append(("C", child, mother_uc))

        if len(content) < limit:
            break

        page += 1
        time.sleep(POWERREV_DELAY)

    return rows


def fetch_invoices_for_month(reference_month: str) -> list[dict]:
    _load_consumer_units()
    simple_items = _fetch_simple_invoice_items(reference_month)

    try:
        grouped_rows = _fetch_grouped_invoice_rows(reference_month)
    except Exception as exc:
        # Mantém comportamento atual mesmo se grouped estiver indisponível
        logger.warning("PowerRev grouped-invoice indisponível para %s: %s", reference_month, exc)
        grouped_rows = []

    # Dedupe por chave:
    # 1) id quando disponível (mais forte)
    # 2) chave composta quando id = null (inclui kind para evitar colisão mãe/filha)
    deduped: dict[tuple, dict] = {}

    # Normalizar dados simples
    for item in simple_items:
        uc = _resolve_uc_installation(item) or ""
        raw_status = _safe_str(item.get("status"))
        raw_id = _safe_str(item.get("id"))
        translated_status = _STATUS_TRANSLATION.get(raw_status, raw_status)
        formatted_issue = _format_date(item.get("issueDate"))
        formatted_total = _to_currency_number(item.get("total"))
        invoice_id = raw_id or _synthetic_invoice_id(
            kind="S",
            uc=uc,
            parent_grouped_uc="",
            issue_date=formatted_issue,
            provider_name=item.get("providerName", ""),
        )
        invoice = {
            "invoiceId": invoice_id,
            "uc": uc,
            "parentGroupedUc": "",
            "referenceMonth": reference_month,
            "providerName": item.get("providerName", ""),
            "statusRaw": raw_status,
            "status": translated_status,
            "issueDate": formatted_issue,
            "total": formatted_total,
            "rowKey": _row_key(
                kind="S",
                uc=uc,
                reference_month=reference_month,
                parent_grouped_uc="",
                status=translated_status,
                issue_date=formatted_issue,
                total=formatted_total,
                provider_name=item.get("providerName", ""),
            ),
        }

        if raw_id:
            key = ("ID", raw_id)
        else:
            key = (
                "SNULL",
                reference_month,
                _safe_str(item.get("accountId")),
                _safe_str(item.get("consumerUnits")),
                raw_status,
                _safe_str(item.get("issueDate")),
                _safe_str(item.get("dueDate")),
                _safe_str(item.get("total")),
            )

        prev = deduped.get(key)
        if prev is None or _invoice_completeness_score(invoice) > _invoice_completeness_score(prev):
            deduped[key] = invoice

    # Normalizar dados agrupados (mãe + filhas)
    for kind, item, parent_uc in grouped_rows:
        uc = _extract_grouped_consumer_unit(item)
        if not uc:
            # fallback robusto para compatibilidade com formatos heterogêneos
            uc = _resolve_uc_installation(item) or ""

        raw_status = _safe_str(item.get("status"))
        ref = _safe_str(item.get("referenceMonth")) or reference_month
        raw_id = _safe_str(item.get("id"))
        parent_grouped_uc = f"UC Mãe: {uc}" if kind == "M" and uc else (parent_uc if kind == "C" else "")
        translated_status = _STATUS_TRANSLATION.get(raw_status, raw_status)
        formatted_issue = _format_date(item.get("issueDate"))
        formatted_total = _to_currency_number(item.get("total"))
        invoice_id = raw_id or _synthetic_invoice_id(
            kind=("M" if kind == "M" else "C"),
            uc=uc,
            parent_grouped_uc=parent_grouped_uc,
            issue_date=formatted_issue,
            provider_name=item.get("providerName", ""),
        )
        invoice = {
            "invoiceId": invoice_id,
            "uc": uc,
            "parentGroupedUc": parent_grouped_uc,
            "referenceMonth": ref,
            "providerName": item.get("providerName", ""),
            "statusRaw": raw_status,
            "status": translated_status,
            "issueDate": formatted_issue,
            "total": formatted_total,
            "rowKey": _row_key(
                kind=("M" if kind == "M" else "C"),
                uc=uc,
                reference_month=ref,
                parent_grouped_uc=parent_grouped_uc,
                status=translated_status,
                issue_date=formatted_issue,
                total=formatted_total,
                provider_name=item.get("providerName", ""),
            ),
        }

        if raw_id:
            key = ("ID", raw_id)
        else:
            key = (
                "GNULL",
                kind,  # evita colidir mãe x filha quando ambos têm id null
                ref,
                _safe_str(item.get("accountId")),
                uc,
                parent_uc if kind == "C" else "",
                raw_status,
                _safe_str(item.get("issueDate")),
                _safe_str(item.get("dueDate")),
                _safe_str(item.get("total")),
                _safe_str(item.get("providerName")),
            )

        prev = deduped.get(key)
        if prev is None or _invoice_completeness_score(invoice) > _invoice_completeness_score(prev):
            deduped[key] = invoice

    resolved = list(deduped.values())

    stats.powerrev_invoices_fetched += len(resolved)
    if resolved:
        logger.info(
            "  → %d invoices (simples=%d, agrupadas_flat=%d, dedup=%d)",
            len(resolved),
            len(simple_items),
            len(grouped_rows),
            len(simple_items) + len(grouped_rows) - len(resolved),
        )
    else:
        logger.info("  → sem dados")
    return resolved


def reset_caches() -> None:
    global _UC_BY_ID, _UC_BY_INSTALLATION, _UC_BY_CODE, _ACCOUNT_CACHE
    _UC_BY_ID = {}
    _UC_BY_INSTALLATION = {}
    _UC_BY_CODE = {}
    _ACCOUNT_CACHE = OrderedDict()
