"""
Mapeamento de campos ClickUp â†’ colunas da planilha Faturamento.

Cada entrada:
  key          â€“ identificador interno
  header       â€“ nome da coluna no Google Sheets
  source       â€“ "custom_field" | "task_field" | "computed" | "placeholder"
  cf_id        â€“ UUID do custom field (quando source == "custom_field")
  transform    â€“ nome de funÃ§Ã£o em transformers.py (opcional)
"""

# â”€â”€ Campos que vÃªm do ClickUp â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
        "source": "computed",  # dia_envio + mes_ref + lÃ³gica Mês Atual/Seguinte
    },
    "data_vencimento": {
        "header": "Data de Vencimento",
        "source": "computed",  # dia_vencto + mes_ref + lÃ³gica dia/razÃ£o social
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
        "header": "Observações Financeiras",
        "source": "computed",
    },
    "data_faturamento": {
        "header": "Data de Faturamento",
        "source": "placeholder",
    },
    "login_distribuidora": {
        "header": "Login da Distribuidora",
        "source": "custom_field",
        "cf_id": "d6305b8c-e448-4009-83ef-6afb1047c566",
        "missing_value": "Sem login na ClickUp",
    },
    "senha_distribuidora": {
        "header": "Senha da Distribuidora",
        "source": "custom_field",
        "cf_id": "117b58c3-a31b-4967-9cb2-42706f1cf812",
        "missing_value": "Sem senha na ClickUp",
    },
    "status_faturamento": {
        "header": "Status de faturamento",
        "source": "placeholder",
    },
    "provider_name": {
        "header": "Provider",
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
    "valor_final": {
        "header": "Valor final",
        "source": "placeholder",
    },
    "data_emissao_final": {
        "header": "Data de emissão final",
        "source": "placeholder",
    },
    "validacao": {
        "header": "Validação",
        "source": "placeholder",
    },
    "inicio_operacao_display": {
        "header": "Início de Operação",
        "source": "placeholder",
    },
    "observacoes": {
        "header": "Observações",
        "source": "placeholder",
    },
    "parentesco_agrupado": {
        "header": "Fim de Operação",
        "source": "placeholder",
    },
    "invoice_id": {
        "header": "Invoice ID",
        "source": "placeholder",
    },
}

# â”€â”€ Campos de data usados para expandir meses â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
DATE_FIELDS = {
    "inicio_operacao": {
        "cf_id": "ebd051a1-d5b6-4cb1-861b-574a1f968663",
    },
    "fim_operacao": {
        "cf_id": "f0afcaaf-ccca-4bf3-9d42-2cb0fa3d1296",
    },
}

# â”€â”€ Campos auxiliares para cÃ¡lculo de Envio/Vencimento â”€â”€â”€
COMPUTATION_FIELDS = {
    "mes_envio_boleto": {
        "cf_id": "ed8813c6-d508-4e47-9298-926a7fcd928a",
    },
    "mes_vencimento_boleto": {
        "cf_id": "dcaaa5d9-8d1a-435d-bf2b-50fa5488d480",
    },
    "dia_envio_boleto": {
        "cf_id": "838a8088-ce85-4564-90ee-ce243b229a29",
    },
    "dia_vencto_boleto": {
        "cf_id": "cafdd69f-fdb7-41c0-9328-940a26be6b3b",
    },
}

# â”€â”€ Campo de observações financeiras do ClickUp â”€
OBS_FIELDS = [
    {"label": "Financeiras", "cf_id": "dead0bd3-64a3-482d-99b0-994abc8440b1"},
]

# â”€â”€ Mapa estÃ¡tico de opÃ§Ãµes dos dropdowns (id â†’ nome) â”€â”€â”€â”€
DROPDOWN_OPTIONS = {
    # Status Detalhado
    "1a5118f7-b9a0-466f-889d-37edd76bd304": {
        "12a08c0a-9e2b-4ed0-b40e-7313791840eb": "Ativo",
        "d322386d-2b63-43cb-8036-cae3cf94531f": "Retirado da Usina - Saldo",
        "d8831b76-8f4d-4744-938b-82efef419437": "Retirado da Usina - InadimplÃªncia",
        "ae80bc03-d28f-4bc3-ae2f-653accd64e0b": "Aguardando Cadastro - Usina",
        "92cb3240-3915-43ac-a9d9-517a8903b448": "A Retirar da Usina - DemissÃ£o",
        "a74997a7-e393-4bfc-9241-ed76a0a05569": "Encerrado - Financeiro",
        "25a28dc4-16ff-4ecf-b94f-a7b3a6eef42c": "Encerrado - Troca de Plano",
        "b39e4722-25c1-4bbb-980d-dc5d43789dc3": "Aguardando saÃ­da de concorrente",
        "15f1bd8a-215f-4869-9386-fb725a7b8adb": "Cadastro em andamento",
        "265047c8-7ca1-44ec-a627-c598aab081ba": "Baixo Consumo",
        "5afbfb3f-8c96-455d-8d87-164ed477ae52": "Retirado da Usina - CR",
        "1c4aabb2-3fb0-4e2d-8a67-03025ac2654d": "Aguardando Cadastro - em ContingÃªncia",
        "c4876bc8-67fd-4db1-8d3e-60a8995ee839": "Ativo - em ContingÃªncia",
        "6460b3b7-e6c7-484c-ac90-6a1f9d2d0ca0": "A Retirar da Usina - CR",
        "32706ab8-e1c8-4052-ab94-3261c52acc72": "Retirado da Usina - DemissÃ£o",
        "2e7e31aa-13c8-4a78-a550-3d8d8ea6bd5a": "A Retirar da Usina - InadimplÃªncia",
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
    # MÃªs de envio do boleto
    "ed8813c6-d508-4e47-9298-926a7fcd928a": {
        "1c745be3-0763-4396-b508-0de2d1189de3": "Mesmo mês da emissão",
        "16a8ada3-c5a1-484b-acd1-97f34e97f576": "Um mês após a emissão",
    },
}

# Ordem das colunas na planilha (define a posiÃ§Ã£o de cada campo)
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
    "observacoes",          # coluna manual protegida
    "valor_boleto",         # valor PowerRev
    "data_emissao_fatura",
    "data_faturamento",     # coluna manual protegida
    "login_distribuidora",
    "senha_distribuidora",
    "status_faturamento",
    "provider_name",
    "observacoes_clickup",
    "validacao",
    "inicio_operacao_display",
    "parentesco_agrupado",
    "invoice_id",               # coluna tecnica
]


def get_headers() -> list[str]:
    """Retorna lista de headers na ordem correta."""
    return [FIELD_MAP[k]["header"] for k in COLUMN_ORDER]
