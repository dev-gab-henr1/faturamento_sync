"""
Funções de transformação de valores de custom fields do ClickUp.
Usa mapa estático DROPDOWN_OPTIONS para resolver UUIDs → nomes.
"""
from field_map import DROPDOWN_OPTIONS


def resolve_dropdown(field: dict) -> str:
    """
    Resolve dropdown: busca o UUID do value no mapa estático.
    Fallback: tenta type_config.options da API, depois retorna raw.
    """
    value = field.get("value")
    if value is None:
        return ""

    cf_id = field.get("id", "")
    value_str = str(value)

    # 1) Mapa estático (preferencial)
    if cf_id in DROPDOWN_OPTIONS:
        name = DROPDOWN_OPTIONS[cf_id].get(value_str)
        if name:
            return name

    # 2) Fallback: type_config.options da API (por orderindex ou id)
    options = field.get("type_config", {}).get("options", [])
    if isinstance(value, int) and 0 <= value < len(options):
        return options[value].get("name", "")
    for opt in options:
        if str(opt.get("orderindex")) == value_str or opt.get("id") == value_str:
            return opt.get("name", "")

    return value_str


def task_id_to_link(task_id: str) -> str:
    """Converte task ID em link completo do ClickUp."""
    if not task_id:
        return ""
    return f"https://app.clickup.com/t/{task_id}"


def clean_description(raw: str) -> str:
    """
    Extrai texto puro da description do ClickUp (formato Quill Delta JSON).
    Retorna "" se vazio, None, ou só linhas em branco.
    """
    if not raw or raw == "None":
        return ""
    import json
    text_parts = []
    decoder = json.JSONDecoder()
    pos = 0
    parsed = False
    while pos < len(raw):
        s = raw[pos:].lstrip()
        if not s:
            break
        try:
            obj, end = decoder.raw_decode(s)
            parsed = True
            for op in obj.get("ops", []):
                insert = op.get("insert", "")
                if isinstance(insert, str):
                    text_parts.append(insert)
            pos += (len(raw[pos:]) - len(s)) + end
        except (json.JSONDecodeError, AttributeError):
            break
    if not parsed:
        # Não é JSON — retornar como texto direto
        return raw.strip()
    return "".join(text_parts).strip()


# Registro de transformers por nome (usado em field_map)
TRANSFORMERS = {
    "resolve_dropdown": resolve_dropdown,
    "task_id_to_link": task_id_to_link,
    "clean_description": clean_description,
}