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


# Registro de transformers por nome (usado em field_map)
TRANSFORMERS = {
    "resolve_dropdown": resolve_dropdown,
    "task_id_to_link": task_id_to_link,
}