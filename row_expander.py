"""
Constrói linhas a partir de invoices da PowerRev + dados do ClickUp.
PowerRev define quais linhas existem (uma por invoice).
ClickUp enriquece com dados do cooperado.
"""
from datetime import datetime
from field_map import (
    FIELD_MAP, DATE_FIELDS, COLUMN_ORDER,
    COMPUTATION_FIELDS, DROPDOWN_OPTIONS, RAZAO_SOCIAL_VENCTO_EXTRA,
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


def slim_task(task: dict) -> dict:
    """Extrai só os campos necessários de uma task crua do ClickUp."""
    slim_cfs = []
    for cf in task.get("custom_fields", []):
        cf_id = cf.get("id")
        if cf_id in _NEEDED_CF_IDS:
            slim_cf = {"id": cf_id, "value": cf.get("value")}
            tc = cf.get("type_config")
            if tc and "options" in tc:
                slim_cf["type_config"] = {"options": tc["options"]}
            slim_cfs.append(slim_cf)
    return {
        "id": task.get("id", ""),
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
    dia_envio: int, ref_month: int, ref_year: int, mes_seguinte: bool,
) -> str:
    month, year = ref_month, ref_year
    if mes_seguinte:
        month, year = _add_months(month, year, 1)
    return f"{dia_envio:02d}/{month:02d}/{year}"


def _compute_data_vencimento(
    dia_vencto: int,
    dia_envio: int,
    ref_month: int,
    ref_year: int,
    mes_seguinte: bool,
    razao_social: str,
) -> str:
    month, year = ref_month, ref_year
    if mes_seguinte:
        month, year = _add_months(month, year, 1)
    extra = (dia_vencto < dia_envio) or (razao_social.strip() in RAZAO_SOCIAL_VENCTO_EXTRA)
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
        mes_envio_raw = _get_cf_raw(task, COMPUTATION_FIELDS["mes_envio_boleto"]["cf_id"])

        try:
            dia_envio = int(float(dia_envio_raw)) if dia_envio_raw else 0
        except (ValueError, TypeError):
            dia_envio = 0
        try:
            dia_vencto = int(float(dia_vencto_raw)) if dia_vencto_raw else 0
        except (ValueError, TypeError):
            dia_vencto = 0

        mes_envio_label = ""
        if mes_envio_raw:
            cf_id = COMPUTATION_FIELDS["mes_envio_boleto"]["cf_id"]
            mes_envio_label = _resolve_dropdown_value(cf_id, mes_envio_raw)
        mes_seguinte = mes_envio_label == "Mês Seguinte"
        razao_social = base_values.get("razao_social", "")
    else:
        base_values = {k: "" for k in COLUMN_ORDER}
        dia_envio = 0
        dia_vencto = 0
        mes_seguinte = False
        razao_social = ""

    # Montar linha
    row: list[str] = []
    for key in COLUMN_ORDER:
        if key == "mes_referencia":
            row.append(mes_label)
        elif key == "envio_boleto":
            if dia_envio and ref_month:
                row.append(_compute_envio_boleto(
                    dia_envio, ref_month, ref_year, mes_seguinte,
                ))
            else:
                row.append("")
        elif key == "data_vencimento":
            if dia_vencto and ref_month:
                row.append(_compute_data_vencimento(
                    dia_vencto, dia_envio, ref_month, ref_year,
                    mes_seguinte, razao_social,
                ))
            else:
                row.append("")
        elif key == "mes_atendimento":
            row.append(str(mes_atendimento))
        elif key == "status_faturamento":
            row.append(invoice.get("status", ""))
        elif key == "data_emissao_fatura":
            row.append(invoice.get("issueDate", ""))
        elif key == "valor_boleto":
            row.append(invoice.get("total", ""))
        else:
            row.append(base_values.get(key, ""))

    return row