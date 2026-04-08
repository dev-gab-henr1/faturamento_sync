"""
Cliente PowerRev API para o projeto Faturamento Sync.
Busca billing/invoices e resolve UCs para matching com a planilha.
"""
import time
import logging
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
_ACCOUNT_CACHE: dict[str, dict] = {}

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


def _format_currency(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
        formatted = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {formatted}"
    except (ValueError, TypeError):
        return str(value)


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
            if account is None:
                try:
                    resp = _request("GET", f"{POWERREV_BASE_URL}/billing/account/{account_id}")
                    account = resp.json() if isinstance(resp.json(), dict) else {}
                except requests.RequestException:
                    account = {}
                _ACCOUNT_CACHE[cache_key] = account
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


def fetch_invoices_for_month(reference_month: str) -> list[dict]:
    _load_consumer_units()

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
            try:
                resp = _request(
                    "GET",
                    f"{POWERREV_BASE_URL}/billing/invoice",
                    params={"referenceMonth": reference_month},
                )
                result_items, _, _, _ = _normalize_items(resp.json())
            except requests.RequestException:
                break
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

    resolved: list[dict] = []
    for item in items:
        uc = _resolve_uc_installation(item)
        raw_status = str(item.get("status", ""))
        resolved.append({
            "uc": uc or "",
            "referenceMonth": reference_month,
            "providerName": item.get("providerName", ""),
            "statusRaw": raw_status,
            "status": _STATUS_TRANSLATION.get(raw_status, raw_status),
            "issueDate": _format_date(item.get("issueDate")),
            "total": _format_currency(item.get("total")),
        })

    stats.powerrev_invoices_fetched += len(resolved)
    if resolved:
        logger.info("  → %d invoices", len(resolved))
    else:
        logger.info("  → sem dados")
    return resolved


def reset_caches() -> None:
    global _UC_BY_ID, _UC_BY_INSTALLATION, _UC_BY_CODE, _ACCOUNT_CACHE
    _UC_BY_ID = {}
    _UC_BY_INSTALLATION = {}
    _UC_BY_CODE = {}
    _ACCOUNT_CACHE = {}
