# Faturamento Sync

Sincroniza dados de tasks do ClickUp com uma planilha Google Sheets de faturamento.

## Lógica principal

Cada task que possui os campos **Início de Operação** e **Fim de Operação** é expandida em N linhas — **uma por mês** dentro do intervalo.

Exemplo: Início `01/11/2023`, Fim `01/05/2024` → 7 linhas (11/2023 a 05/2024).

## Colunas da planilha

| Coluna | Fonte | Status |
|---|---|---|
| Status | ClickUp (Status Detalhado) | ✅ |
| UC | ClickUp (UC antiga) | ✅ |
| Razão Social (Matriz) /Pessoa | ClickUp | ✅ |
| Mês Referência | Computado (início→fim operação) | ✅ |
| Envio | A definir | ⏳ |
| Data Vencimento Boleto | A definir | ⏳ |
| Mês de Atendimento | A definir | ⏳ |
| Plano | ClickUp (dropdown) | ✅ |
| Distribuidora | ClickUp | ✅ |
| Pagamento para Grupo Econômico | ClickUp (Tipo Faturamento) | ✅ |
| Observação | A definir | ⏳ |
| Aquisição de Faturas | PowerRev (futuro) | ⏳ |
| Status de Faturamento | PowerRev (futuro) | ⏳ |
| Data de Emissão da Fatura | PowerRev (futuro) | ⏳ |
| Valor do Boleto | PowerRev (futuro) | ⏳ |
| Task ID | ClickUp (controle) | ✅ |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Editar .env com suas credenciais
python poll.py
```

## Deploy (Railway)

Variáveis de ambiente:
- `CLICKUP_TOKEN`
- `GOOGLE_CREDENTIALS_JSON` (JSON da service account)
- `SPREADSHEET_ID`

## Arquitetura

```
poll.py              ← Loop principal (full + delta sync)
clickup_client.py    ← Cliente ClickUp com paginação e retry
row_expander.py      ← Expansão task → N linhas mensais
sheets_manager.py    ← Leitura/escrita Google Sheets (RAW, chunks)
field_map.py         ← Mapeamento de campos e ordem das colunas
transformers.py      ← Transformações de valores (dropdown, etc.)
config.py            ← Configurações e env vars
```
