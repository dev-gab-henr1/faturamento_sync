# Faturamento Sync

Sincroniza o faturamento mensal de cooperados entre ClickUp, PowerRev e Google
Sheets. O processo mantém a aba principal `Faturamento`, o espelho operacional de
captura de faturas e o espelho semanal de detalhes.

A referência técnica vigente está em
[`docs/funcionamento_planilhas_faturamento.md`](docs/funcionamento_planilhas_faturamento.md).
Ela documenta colunas, fontes, campos textuais, métricas, filtros, preservação de
valores manuais, full sync, delta sync e os dois espelhos.

## Execução

```bash
pip install -r requirements.txt
python poll.py
```

O serviço executa um full sync no início, agenda um full diário, executa deltas
periódicos e atualiza o espelho de captura após cada full ou delta. O espelho de
detalhes é um ciclo semanal separado e não roda no primeiro full.

## Arquivos principais

- `poll.py`: orquestração, filtros, full, delta e agendamentos.
- `field_map.py`: schema e ordem das colunas da aba `Faturamento`.
- `row_expander.py`: cálculo e montagem das linhas mensais.
- `powerrev_client.py`: invoices simples e agrupadas da PowerRev.
- `sheets_manager.py`: escrita e preservação das colunas manuais.
- `invoice_capture_mirror.py`: espelho de captura de faturas.
- `billing_details_mirror.py`: espelho semanal de detalhes.
- `config.py`: listas, planilhas, flags e variáveis de ambiente.

## Segurança operacional

Não execute full, delta, espelhos ou scripts utilitários contra planilhas ativas
apenas para validar uma mudança. Escritas em Google Sheets, ClickUp ou PowerRev
exigem autorização explícita.
