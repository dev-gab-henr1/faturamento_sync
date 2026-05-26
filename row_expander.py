"""
Constrói linhas a partir de invoices da PowerRev + dados do ClickUp.
PowerRev define quais linhas existem (uma por invoice).
ClickUp enriquece com dados do cooperado.
"""
from datetime import datetime
import re
import unicodedata
from field_map import (
    FIELD_MAP, DATE_FIELDS, COLUMN_ORDER,
    COMPUTATION_FIELDS, DROPDOWN_OPTIONS,
    OBS_FIELDS,
)
from transformers import TRANSFORMERS, clean_description

_MONTH_ABBR_PT = {
    1: "jan.", 2: "fev.", 3: "mar.", 4: "abr.",
    5: "mai.", 6: "jun.", 7: "jul.", 8: "ago.",
    9: "set.", 10: "out.", 11: "nov.", 12: "dez.",
}

_MONTH_NUM_PT = {v: k for k, v in _MONTH_ABBR_PT.items()}

# Set de todos os cf_ids necessários (para slim)
_NEEDED_CF_IDS: set[str] = set()
for spec in FIELD_MAP.values():
    if spec.get("cf_id"):
        _NEEDED_CF_IDS.add(spec["cf_id"])
for spec in DATE_FIELDS.values():
    _NEEDED_CF_IDS.add(spec["cf_id"])
for spec in COMPUTATION_FIELDS.values():
    _NEEDED_CF_IDS.add(spec["cf_id"])
for obs in OBS_FIELDS:
    _NEEDED_CF_IDS.add(obs["cf_id"])

# cf_ids de dropdowns que NÃO estão no mapa estático DROPDOWN_OPTIONS.
# Apenas estes precisam de type_config para fallback no resolve_dropdown.
# Os demais (Status, Distribuidora, Tipo Faturamento, Mês Envio) já estão
# cobertos pelo mapa estático e não precisam carregar options da API.
_NEEDS_TYPE_CONFIG: set[str] = set()
_DROPDOWN_CF_IDS: set[str] = set()
for _spec in FIELD_MAP.values():
    if _spec.get("transform") == "resolve_dropdown" and _spec.get("cf_id"):
        _DROPDOWN_CF_IDS.add(_spec["cf_id"])
        if _spec["cf_id"] not in DROPDOWN_OPTIONS:
            _NEEDS_TYPE_CONFIG.add(_spec["cf_id"])
_DROPDOWN_CF_IDS.add(COMPUTATION_FIELDS["mes_envio_boleto"]["cf_id"])
_DROPDOWN_CF_IDS.add(COMPUTATION_FIELDS["mes_vencimento_boleto"]["cf_id"])


def slim_task(task: dict) -> dict:
    """Extrai só os campos necessários de uma task crua do ClickUp."""
    slim_cfs = []
    status_cf_id = FIELD_MAP["status"]["cf_id"]
    for cf in task.get("custom_fields", []):
        cf_id = cf.get("id")
        if cf_id in _NEEDED_CF_IDS:
            slim_cf = {"id": cf_id, "value": cf.get("value")}
            # Para dropdowns, guardar options para resolver quando vier orderindex
            # Inclui também Status Detalhado para detectar "Encerrado - Troca de Plano"
            if cf_id in _DROPDOWN_CF_IDS or cf_id == status_cf_id:
                tc = cf.get("type_config")
                if tc and "options" in tc:
                    slim_cf["type_config"] = {"options": tc["options"]}
            slim_cfs.append(slim_cf)
    return {
        "id": task.get("id", ""),
        "list_id": str(task.get("list", {}).get("id", "") or ""),
        "custom_fields": slim_cfs,
    }


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        ts = int(value)
        if ts > 1e12:
            ts = ts / 1000
        return datetime.fromtimestamp(ts)
    except (ValueError, OSError):
        return None


def _add_months(month: int, year: int, n: int) -> tuple[int, int]:
    month += n
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return month, year


def _get_cf_value(task: dict, cf_id: str) -> dict | None:
    for cf in task.get("custom_fields", []):
        if cf.get("id") == cf_id:
            return cf
    return None


def _get_cf_raw(task: dict, cf_id: str) -> str | None:
    cf = _get_cf_value(task, cf_id)
    if cf is None:
        return None
    val = cf.get("value")
    return str(val) if val is not None else None


def _resolve_dropdown_value(cf_id: str, raw_value: str) -> str:
    opts = DROPDOWN_OPTIONS.get(cf_id, {})
    return opts.get(raw_value, raw_value)


def _normalize_dropdown_label(text: str) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    raw = raw.replace("mÃªs", "mês").replace("mãªs", "mês")
    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())


def _parse_month_offset(label: str) -> int | None:
    normalized = _normalize_dropdown_label(label)
    if not normalized:
        return None

    # Regras tolerantes a variações de encoding/acento.
    if "mesmo" in normalized and "emiss" in normalized:
        return 0
    if "dois mes" in normalized and "emiss" in normalized:
        return 2
    if "um mes" in normalized and "emiss" in normalized:
        return 1

    # Novos labels
    if "mesmo mes da emissao" in normalized:
        return 0
    if "um mes apos a emissao" in normalized:
        return 1
    if "dois meses apos a emissao" in normalized:
        return 2

    # Retrocompatibilidade
    if "mes atual" in normalized:
        return 0
    if "mes seguinte" in normalized or "seguinte" in normalized:
        return 1

    # Fallback numerico
    if normalized.isdigit():
        off = int(normalized)
        if 0 <= off <= 2:
            return off

    m = re.search(r"\b([0-2])\b", normalized)
    if m:
        return int(m.group(1))

    return None


def _get_month_offset_by_cf(task: dict, cf_id: str, default: int = 0) -> int:
    cf = _get_cf_value(task, cf_id)
    if cf is None:
        return default

    resolved = TRANSFORMERS["resolve_dropdown"](cf)
    parsed = _parse_month_offset(resolved)
    if parsed is not None:
        return parsed

    raw_value = cf.get("value")
    if raw_value is not None:
        parsed = _parse_month_offset(str(raw_value))
        if parsed is not None:
            return parsed

        try:
            off = int(float(raw_value))
            if 0 <= off <= 2:
                return off
        except (ValueError, TypeError):
            pass

    return default


def _extract_field_value(task: dict, key: str) -> str:
    spec = FIELD_MAP[key]
    source = spec["source"]

    if source in ("computed", "placeholder"):
        return ""

    if source == "task_field":
        raw = str(task.get(spec.get("task_key", ""), ""))
        transform_name = spec.get("transform")
        if transform_name and transform_name in TRANSFORMERS:
            return TRANSFORMERS[transform_name](raw)
        return raw

    if source == "custom_field":
        cf = _get_cf_value(task, spec["cf_id"])
        if cf is None:
            return ""
        transform_name = spec.get("transform")
        if transform_name and transform_name in TRANSFORMERS:
            return TRANSFORMERS[transform_name](cf)
        val = cf.get("value")
        if val is None:
            return ""
        return str(val)

    return ""


def _compute_envio_boleto(
    dia_envio: int, ref_month: int, ref_year: int, month_offset: int,
) -> str:
    month, year = ref_month, ref_year
    if month_offset:
        month, year = _add_months(month, year, month_offset)
    return f"{dia_envio:02d}/{month:02d}/{year}"


def _compute_data_vencimento(
    dia_vencto: int,
    dia_envio: int,
    ref_month: int,
    ref_year: int,
    month_offset: int,
) -> str:
    month, year = ref_month, ref_year
    if month_offset:
        month, year = _add_months(month, year, month_offset)
    extra = dia_vencto < dia_envio
    if extra:
        month, year = _add_months(month, year, 1)
    return f"{dia_vencto:02d}/{month:02d}/{year}"


def get_inicio_operacao(task: dict) -> datetime | None:
    cf = _get_cf_value(task, DATE_FIELDS["inicio_operacao"]["cf_id"])
    if cf is None:
        return None
    return _parse_date(cf.get("value"))


def get_fim_operacao(task: dict) -> datetime | None:
    cf = _get_cf_value(task, DATE_FIELDS["fim_operacao"]["cf_id"])
    if cf is None:
        return None
    return _parse_date(cf.get("value"))


def get_fim_operacao_display(task: dict | None) -> str:
    """Retorna texto para coluna de fim de operação."""
    if not task:
        return "Sem data de fim definida."
    dt = get_fim_operacao(task)
    if dt is None:
        return "Sem data de fim definida."
    return dt.strftime("%d/%m/%Y")


def compute_data_vencimento_for_task(task: dict | None, mes_label: str) -> str:
    """Calcula Data de Vencimento (coluna G) para um mês de referência da linha."""
    if not task or not mes_label:
        return ""

    ref_ym = label_to_yyyymm(str(mes_label).strip())
    if len(ref_ym) < 6:
        return ""

    ref_month = int(ref_ym[4:6])
    ref_year = int(ref_ym[:4])

    dia_envio_raw = _get_cf_raw(task, COMPUTATION_FIELDS["dia_envio_boleto"]["cf_id"])
    dia_vencto_raw = _get_cf_raw(task, COMPUTATION_FIELDS["dia_vencto_boleto"]["cf_id"])

    try:
        dia_envio = int(float(dia_envio_raw)) if dia_envio_raw else 0
    except (ValueError, TypeError):
        dia_envio = 0
    try:
        dia_vencto = int(float(dia_vencto_raw)) if dia_vencto_raw else 0
    except (ValueError, TypeError):
        dia_vencto = 0

    if not dia_vencto:
        return ""

    month_offset_envio = _get_month_offset_by_cf(
        task,
        COMPUTATION_FIELDS["mes_envio_boleto"]["cf_id"],
        default=0,
    )
    month_offset_venc = _get_month_offset_by_cf(
        task,
        COMPUTATION_FIELDS["mes_vencimento_boleto"]["cf_id"],
        default=month_offset_envio,
    )

    return _compute_data_vencimento(
        dia_vencto=dia_vencto,
        dia_envio=dia_envio,
        ref_month=ref_month,
        ref_year=ref_year,
        month_offset=month_offset_venc,
    )


def compute_envio_boleto_for_task(task: dict | None, mes_label: str) -> str:
    """Calcula Envio do boleto (coluna F) para um mes de referencia da linha."""
    if not task or not mes_label:
        return ""

    ref_ym = label_to_yyyymm(str(mes_label).strip())
    if len(ref_ym) < 6:
        return ""

    ref_month = int(ref_ym[4:6])
    ref_year = int(ref_ym[:4])

    dia_envio_raw = _get_cf_raw(task, COMPUTATION_FIELDS["dia_envio_boleto"]["cf_id"])
    try:
        dia_envio = int(float(dia_envio_raw)) if dia_envio_raw else 0
    except (ValueError, TypeError):
        dia_envio = 0

    if not dia_envio:
        return ""

    month_offset_envio = _get_month_offset_by_cf(
        task,
        COMPUTATION_FIELDS["mes_envio_boleto"]["cf_id"],
        default=0,
    )

    return _compute_envio_boleto(
        dia_envio=dia_envio,
        ref_month=ref_month,
        ref_year=ref_year,
        month_offset=month_offset_envio,
    )


def extract_task_uc(task: dict) -> str:
    """Extrai o valor de UC de um task slim."""
    cf = _get_cf_value(task, FIELD_MAP["uc"]["cf_id"])
    if cf is None:
        return ""
    val = cf.get("value")
    return str(val).strip() if val else ""


def yyyymm_to_label(yyyymm: str) -> str:
    """'202503' → 'mar./2025'"""
    if len(yyyymm) < 6:
        return ""
    month = int(yyyymm[4:6])
    year = yyyymm[:4]
    abbr = _MONTH_ABBR_PT.get(month, "")
    return f"{abbr}/{year}" if abbr else ""


def label_to_yyyymm(label: str) -> str:
    """'mar./2025' → '202503'"""
    try:
        abbr, year = label.split("/")
        month_num = _MONTH_NUM_PT.get(abbr, 0)
        return f"{year}{month_num:02d}"
    except (ValueError, IndexError):
        return ""


def _build_observacoes(task: dict) -> str:
    """Concatena os 3 campos de observações do ClickUp, só os populados."""
    parts = []
    for obs in OBS_FIELDS:
        raw = _get_cf_raw(task, obs["cf_id"])
        text = clean_description(raw) if raw else ""
        if text:
            parts.append(f"{obs['label']}: {text}")
    return "\n".join(parts)


def build_row(
    task: dict | None,
    invoice: dict,
    mes_atendimento: int,
) -> list[str]:
    """
    Constrói uma linha a partir de um invoice PowerRev + task ClickUp.
    task pode ser None se não houver task correspondente.
    """
    ref_ym = str(invoice.get("referenceMonth", ""))
    ref_month = int(ref_ym[4:6]) if len(ref_ym) >= 6 else 0
    ref_year = int(ref_ym[:4]) if len(ref_ym) >= 4 else 0
    mes_label = yyyymm_to_label(ref_ym)

    # Extrair campos ClickUp
    if task:
        base_values: dict[str, str] = {}
        for key in COLUMN_ORDER:
            base_values[key] = _extract_field_value(task, key)
        base_values["observacoes_clickup"] = _build_observacoes(task)

        dia_envio_raw = _get_cf_raw(task, COMPUTATION_FIELDS["dia_envio_boleto"]["cf_id"])
        dia_vencto_raw = _get_cf_raw(task, COMPUTATION_FIELDS["dia_vencto_boleto"]["cf_id"])

        try:
            dia_envio = int(float(dia_envio_raw)) if dia_envio_raw else 0
        except (ValueError, TypeError):
            dia_envio = 0
        try:
            dia_vencto = int(float(dia_vencto_raw)) if dia_vencto_raw else 0
        except (ValueError, TypeError):
            dia_vencto = 0

        month_offset_envio = _get_month_offset_by_cf(
            task,
            COMPUTATION_FIELDS["mes_envio_boleto"]["cf_id"],
            default=0,
        )
        month_offset_venc = _get_month_offset_by_cf(
            task,
            COMPUTATION_FIELDS["mes_vencimento_boleto"]["cf_id"],
            default=month_offset_envio,
        )
    else:
        base_values = {k: "" for k in COLUMN_ORDER}
        dia_envio = 0
        dia_vencto = 0
        month_offset_envio = 0
        month_offset_venc = 0

    # Montar linha
    row: list[str] = []
    for key in COLUMN_ORDER:
        if key == "mes_referencia":
            row.append(mes_label)
        elif key == "envio_boleto":
            if dia_envio and ref_month:
                row.append(_compute_envio_boleto(
                    dia_envio, ref_month, ref_year, month_offset_envio,
                ))
            else:
                row.append("")
        elif key == "data_vencimento":
            if dia_vencto and ref_month:
                row.append(_compute_data_vencimento(
                    dia_vencto, dia_envio, ref_month, ref_year,
                    month_offset_venc,
                ))
            else:
                row.append("")
        elif key == "mes_atendimento":
            row.append(str(mes_atendimento))
        elif key == "status_faturamento":
            row.append(invoice.get("status", ""))
        elif key == "provider_name":
            row.append(invoice.get("providerName", ""))
        elif key == "data_emissao_fatura":
            row.append(invoice.get("issueDate", ""))
        elif key == "valor_boleto":
            row.append(invoice.get("total", ""))
        elif key == "parentesco_agrupado":
            row.append(get_fim_operacao_display(task))
        elif key == "invoice_id":
            row.append(invoice.get("invoiceId", ""))
        else:
            row.append(base_values.get(key, ""))

    return row
