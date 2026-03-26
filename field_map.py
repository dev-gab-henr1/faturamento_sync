"""
Mapeamento de campos ClickUp → colunas da planilha Faturamento.

Cada entrada:
  key          – identificador interno
  header       – nome da coluna no Google Sheets
  source       – "custom_field" | "task_field" | "computed" | "placeholder"
  cf_id        – UUID do custom field (quando source == "custom_field")
  transform    – nome de função em transformers.py (opcional)
"""

# ── Campos que vêm do ClickUp ────────────────────────────
FIELD_MAP = {
    "task_id": {
        "header": "Task ID",
        "source": "task_field",
        "task_key": "id",
        "transform": "task_id_to_link",
    },
    "status": {
        "header": "Status Detalhado",
        "source": "custom_field",
        "cf_id": "1a5118f7-b9a0-466f-889d-37edd76bd304",
        "transform": "resolve_dropdown",
    },
    "uc": {
        "header": "UC",
        "source": "custom_field",
        "cf_id": "abb7e1e9-3c99-4044-b20c-5eb19575a6d5",
    },
    "razao_social": {
        "header": "Razão Social",
        "source": "custom_field",
        "cf_id": "dfb0de9b-121a-4bf6-977f-dfb5eec523cb",
    },
    "mes_referencia": {
        "header": "Mês de Referencia",
        "source": "computed",
    },
    "envio_boleto": {
        "header": "Envio do boleto",
        "source": "computed",  # dia_envio + mes_ref + lógica Mês Atual/Seguinte
    },
    "data_vencimento": {
        "header": "Data de Vencimento",
        "source": "computed",  # dia_vencto + mes_ref + lógica dia/razão social
    },
    "mes_atendimento": {
        "header": "Mês de atandimento",
        "source": "computed",  # contador sequencial por task
    },
    "plano": {
        "header": "Plano de Adesão",
        "source": "custom_field",
        "cf_id": "0e009719-1e94-482a-825a-c359e268727e",
        "transform": "resolve_dropdown",
    },
    "distribuidora": {
        "header": "Distribuidora",
        "source": "custom_field",
        "cf_id": "84bd83df-2e9f-485f-ae77-0d5c4e02ddf9",
        "transform": "resolve_dropdown",
    },
    "tipo_faturamento": {
        "header": "Tipo de faturamento",
        "source": "custom_field",
        "cf_id": "1b7083f4-36b2-4be8-bddd-c80001630359",
        "transform": "resolve_dropdown",
    },
    "observacoes_clickup": {
        "header": "Observações ClickUp",
        "source": "computed",
    },
    "status_faturamento": {
        "header": "Status de faturamento",
        "source": "placeholder",
    },
    "data_emissao_fatura": {
        "header": "Data de Emissão da fatura",
        "source": "placeholder",
    },
    "valor_boleto": {
        "header": "Valor do boleto",
        "source": "placeholder",
    },
    "validacao": {
        "header": "Validação",
        "source": "placeholder",
    },
    "observacoes": {
        "header": "Observações",
        "source": "placeholder",
    },
}

# ── Campos de data usados para expandir meses ────────────
DATE_FIELDS = {
    "inicio_operacao": {
        "cf_id": "ebd051a1-d5b6-4cb1-861b-574a1f968663",
    },
    "fim_operacao": {
        "cf_id": "f0afcaaf-ccca-4bf3-9d42-2cb0fa3d1296",
    },
}

# ── Campos auxiliares para cálculo de Envio/Vencimento ───
COMPUTATION_FIELDS = {
    "mes_envio_boleto": {
        "cf_id": "ed8813c6-d508-4e47-9298-926a7fcd928a",
    },
    "dia_envio_boleto": {
        "cf_id": "838a8088-ce85-4564-90ee-ce243b229a29",
    },
    "dia_vencto_boleto": {
        "cf_id": "cafdd69f-fdb7-41c0-9328-940a26be6b3b",
    },
}

# ── Campos de observações (3 CFs concatenados na coluna L) ─
OBS_FIELDS = [
    {"label": "Plano", "cf_id": "48b2d7f1-d2e0-45b4-a58a-3f83686ea980"},
    {"label": "Gerais", "cf_id": "94ef8acf-865b-4bcc-b763-02ce2aa184a7"},
    {"label": "Contrato", "cf_id": "1271d921-116c-431e-9e82-0f75ba6f28cb"},
]

# ── Razões sociais que sempre recebem +1 mês no vencimento
RAZAO_SOCIAL_VENCTO_EXTRA = {
    "DROGARIAS PACHECO S.A.",
    "DROGARIAS SÃO PAULO",
    "W V BEZERRA RESTAURANTE LTDA",
}

# ── Mapa estático de opções dos dropdowns (id → nome) ────
DROPDOWN_OPTIONS = {
    # Status Detalhado
    "1a5118f7-b9a0-466f-889d-37edd76bd304": {
        "12a08c0a-9e2b-4ed0-b40e-7313791840eb": "Ativo",
        "d322386d-2b63-43cb-8036-cae3cf94531f": "Retirado da Usina - Saldo",
        "d8831b76-8f4d-4744-938b-82efef419437": "Retirado da Usina - Inadimplência",
        "ae80bc03-d28f-4bc3-ae2f-653accd64e0b": "Aguardando Cadastro - Usina",
        "92cb3240-3915-43ac-a9d9-517a8903b448": "A Retirar da Usina - Demissão",
        "a74997a7-e393-4bfc-9241-ed76a0a05569": "Encerrado - Financeiro",
        "25a28dc4-16ff-4ecf-b94f-a7b3a6eef42c": "Encerrado - Troca de Plano",
        "b39e4722-25c1-4bbb-980d-dc5d43789dc3": "Aguardando saída de concorrente",
        "15f1bd8a-215f-4869-9386-fb725a7b8adb": "Cadastro em andamento",
        "265047c8-7ca1-44ec-a627-c598aab081ba": "Baixo Consumo",
        "5afbfb3f-8c96-455d-8d87-164ed477ae52": "Retirado da Usina - CR",
        "1c4aabb2-3fb0-4e2d-8a67-03025ac2654d": "Aguardando Cadastro - em Contingência",
        "c4876bc8-67fd-4db1-8d3e-60a8995ee839": "Ativo - em Contingência",
        "6460b3b7-e6c7-484c-ac90-6a1f9d2d0ca0": "A Retirar da Usina - CR",
        "32706ab8-e1c8-4052-ab94-3261c52acc72": "Retirado da Usina - Demissão",
        "2e7e31aa-13c8-4a78-a550-3d8d8ea6bd5a": "A Retirar da Usina - Inadimplência",
        "2ff02b08-cd28-48b0-8ab4-b516ed8be73d": "Eliminado",
        "a2ff017f-77d1-403e-ba35-c375057144d0": "Excluido",
        "a858ffec-5fe1-44ac-84aa-da5ead59ce7b": "Demitido",
        "3d472363-6bfb-4b0f-a7b8-d8f8e850a79e": "Aguardando Troca de Titularidade",
        "29e28b58-2922-49c9-a8d0-f2a83d398d0a": "Planejamento - Black",
        "633b62b9-1c73-4de6-bab3-c78410ac80c5": "A Retirar da Usina - Black",
        "9d26bcc2-174b-487a-b7bb-46708b3ebf58": "Retirado da Usina - Black",
        "c5807601-bde8-4a50-8af7-4f5453dbfc74": "A Retirar da Usina - Saldo",
    },
    # Tipo de Faturamento
    "1b7083f4-36b2-4be8-bddd-c80001630359": {
        "89e77d9d-7688-4d4a-b158-11ad40e479ef": "Simples",
        "8f57c2a5-b2d8-4f82-933c-a8b1f40e61ec": "Agrupado",
    },
    # Distribuidora
    "84bd83df-2e9f-485f-ae77-0d5c4e02ddf9": {
        "12954f6f-86be-48f8-81b6-8df5b118733f": "COPEL",
        "d5d26875-9beb-4c62-85e7-a95d90fb8920": "Energisa MS",
        "d4e00593-30b8-423c-b3b6-c7a498d7d435": "CELESC",
        "c19855c6-d4a7-446a-92c9-9e00f213c143": "AmE",
    },
    # Mês de envio do boleto
    "ed8813c6-d508-4e47-9298-926a7fcd928a": {
        "1c745be3-0763-4396-b508-0de2d1189de3": "Mês Atual",
        "16a8ada3-c5a1-484b-acd1-97f34e97f576": "Mês Seguinte",
    },
}

# Ordem das colunas na planilha (define a posição de cada campo)
COLUMN_ORDER = [
    "task_id",
    "status",
    "uc",
    "razao_social",
    "mes_referencia",
    "envio_boleto",
    "data_vencimento",
    "mes_atendimento",
    "plano",
    "distribuidora",
    "tipo_faturamento",
    "observacoes_clickup",
    "status_faturamento",
    "data_emissao_fatura",
    "valor_boleto",
    "validacao",
    "observacoes",
]


def get_headers() -> list[str]:
    """Retorna lista de headers na ordem correta."""
    return [FIELD_MAP[k]["header"] for k in COLUMN_ORDER]