# Funcionamento das planilhas de faturamento

Data de referência desta documentação: **24/08/2026**.

Este documento descreve o comportamento atualmente implementado no projeto
`faturamento_sync`. Em caso de divergência com anotações antigas, prevalecem o
schema de `field_map.py` e as regras efetivas dos módulos citados em cada seção.

Regra de segurança operacional: não executar full sync, delta sync, espelhos,
scripts utilitários ou qualquer escrita em Google Sheets, ClickUp ou PowerRev
apenas para validar esta documentação. Validações em dados ativos devem começar
em modo somente leitura e qualquer escrita exige autorização explícita.

## Visão geral

O serviço mantém três saídas:

1. **Faturamento**: aba principal com uma ou mais linhas por UC e competência.
2. **Faturas**: espelho operacional para captura de faturas, com duas colunas
   manuais protegidas.
3. **Teste**: espelho semanal de detalhes financeiros e energéticos de invoices.

O ciclo normal é:

1. full sync inicial;
2. espelho de captura após o full inicial;
3. full diário, por padrão às `00:10` em `America/Sao_Paulo`;
4. delta periódico, atualmente a cada 600 segundos;
5. espelho de captura após cada full ou delta;
6. espelho de detalhes aos sábados às `04:00`, em ciclo próprio.

O espelho de detalhes **não roda no primeiro full**. Quando sua janela semanal é
atingida, o loop executa o detalhamento e encerra aquele ciclo sem executar delta
nem espelho de captura no mesmo ciclo; esses processos voltam no ciclo seguinte.

## Planilhas principais

| Papel | Planilha padrão | Aba | Escrita realizada pelo projeto |
|---|---|---|---|
| Base principal | `1ea2_iw2_GCK1_p2qrIxL-y_EdCY78BreFi_HzuPkjsg` | `Faturamento` | Full, delta e retorno de `Faturas!O` para `Faturamento!T`. |
| Captura de faturas | `1Ehe5ef63fbnlWWA8iVT-Rf5ri1qJn3ERvS0AKKXf7Dc` | `Faturas` | Reconstrução ou atualização do espelho, preservando campos manuais. |
| Arquivo técnico da captura | mesma planilha de captura | `__Faturas_Observacoes` | Persistência técnica dos campos manuais; a aba é criada e ocultada quando necessário. |
| Detalhes de faturamento | `1OULXOPS-QvToCFizB2pjPS-ZyC6BRRRZ6TfU88nD12E` | `Teste` | Reconstrução semanal do espelho de detalhes. |
| Cache de detalhes | mesma planilha de detalhes | `__Billing_Details_Cache` | Cache por Invoice ID dos detalhes já recuperados. |

Os IDs e nomes das abas de destino podem ser alterados por variáveis de ambiente.
A origem permitida para o espelho de captura continua sendo exclusivamente a aba
`Faturamento`.

## Fontes de dados

### ClickUp

O projeto consulta as listas:

- `901322296001`;
- `901321549851`.

O full também percorre tasks do workspace que possuam UC, mas só aceita tasks cuja
home list pertença a essa lista permitida. O delta consulta tasks atualizadas com
uma janela de sobreposição configurada por `CLICKUP_DELTA_LOOKBACK_SECONDS`.

Campos principais:

| Informação | Campo ClickUp |
|---|---|
| Status Detalhado | `1a5118f7-b9a0-466f-889d-37edd76bd304` |
| UC | `abb7e1e9-3c99-4044-b20c-5eb19575a6d5` |
| UC Aneel | `cd8687a7-0393-45b9-8292-f9b878b31512` |
| Razão Social | `dfb0de9b-121a-4bf6-977f-dfb5eec523cb` |
| Plano de Adesão | `0e009719-1e94-482a-825a-c359e268727e` |
| Distribuidora | `84bd83df-2e9f-485f-ae77-0d5c4e02ddf9` |
| Tipo de faturamento | `1b7083f4-36b2-4be8-bddd-c80001630359` |
| Início de Operação | `ebd051a1-d5b6-4cb1-861b-574a1f968663` |
| Fim de Operação | `f0afcaaf-ccca-4bf3-9d42-2cb0fa3d1296` |
| Dia de emissão, fallback da captura | `c4f18991-f556-4019-af84-157c55aada63` |
| Mês de envio do boleto | `ed8813c6-d508-4e47-9298-926a7fcd928a` |
| Mês de vencimento do boleto | `dcaaa5d9-8d1a-435d-bf2b-50fa5488d480` |
| Dia de envio do boleto | `838a8088-ce85-4564-90ee-ce243b229a29` |
| Dia de vencimento do boleto | `cafdd69f-fdb7-41c0-9328-940a26be6b3b` |
| Login da Distribuidora | `d6305b8c-e448-4009-83ef-6afb1047c566` |
| Senha da Distribuidora | `117b58c3-a31b-4967-9cb2-42706f1cf812` |
| Observações Financeiras | `dead0bd3-64a3-482d-99b0-994abc8440b1` |
| Observações de Cobrança | `daae1721-fdd9-4a47-8fb9-39ecbeceb178` |
| Produto, usado no espelho de detalhes | `62193781-2249-49c1-a95d-80df43d66971` |

### PowerRev

O faturamento principal combina:

- `GET /billing/invoice/v2`, paginado, com fallback para
  `GET /billing/invoice`;
- `GET /billing/grouped-invoice/v2`, paginado, usando somente as faturas filhas;
- `GET /consumer-unit`, para resolver IDs internos em UCs;
- `GET /invoice?nuAnoMes=YYYYMM`, no espelho de captura, para obter `dtEmissao`
  por UC e competência;
- `GET /billing/invoice/{invoice_id}`, somente no espelho de detalhes.

Invoices simples e agrupadas com IDs reais diferentes representam linhas distintas.
O dedupe elimina apenas repetições da mesma identidade técnica retornada pela API:
ID real igual ou, quando não existe ID, a chave composta do próprio payload.

Para `MISSING_DISTRIBUTOR_INVOICE`:

1. se houver outra invoice para a mesma UC e competência, a linha de ausência é
   removida;
2. se só houver linhas de ausência, no máximo uma é mantida para a UC/competência.

## Aba Faturamento

### Granularidade e janela mensal

A granularidade é **uma invoice por linha**. Uma UC pode ter mais de uma linha na
mesma competência quando a PowerRev possui invoices reais diferentes.

O full considera a vigência mensal das tasks e limita a expansão ao mês atual + 1.
Início e fim são inclusivos. Quando não há início, a primeira competência de invoice
da UC pode servir de início; sem início e sem invoice, nenhuma linha é gerada. Um fim
vazio mantém a vigência aberta até o limite do full.

Em cada competência coberta por uma task:

- cada invoice gera uma linha própria;
- sem invoice, é criada uma linha placeholder com `Validação = Não processado`;
- entre cards da mesma UC, o card vigente na competência é selecionado;
- `Mês de atandimento` é a posição cronológica da competência dentro da UC, e
  invoices distintas do mesmo mês recebem o mesmo número.

### Colunas A:AA

| Coluna | Nome | Tipo semântico | Origem ou regra |
|---|---|---|---|
| A | Task ID | Identificador textual | Link da task no ClickUp. |
| B | Status Detalhado | Categoria textual | Label do dropdown do ClickUp. |
| C | UC | Identificador textual | UC antiga do ClickUp e chave principal de associação. |
| D | UC Aneel | Identificador textual | UC Aneel do ClickUp; não substitui a UC antiga. |
| E | Razão Social | Texto descritivo | Campo da task. |
| F | Mês de Referencia | Competência mensal | `referenceMonth`, exibido como `jan./2026`. |
| G | Envio do boleto | Data calculada | Dia e deslocamento mensal configurados no ClickUp. |
| H | Data de Vencimento | Data calculada | Dia e mês de vencimento configurados no ClickUp. |
| I | Mês de atandimento | Métrica ordinal | Ranking cronológico das competências da UC. |
| J | Plano de Adesão | Categoria textual | Label do dropdown do ClickUp. |
| K | Distribuidora | Categoria textual | Label do dropdown do ClickUp. |
| L | Tipo de faturamento | Categoria textual | `Simples`, `Agrupado` ou outra label resolvida. |
| M | Observações | Texto manual protegido | Editado na planilha; não é calculado pelo sync. |
| N | Valor do boleto | Métrica monetária | `total` da billing invoice PowerRev. |
| O | Data de Emissão da fatura | Data PowerRev com política de status | `issueDate` da billing invoice, sujeito à regra de exibição. |
| P | Data de Faturamento | Data manual protegida | Editada na planilha. |
| Q | Login da Distribuidora | Credencial textual | Campo ClickUp; fallback `Sem login na ClickUp`. |
| R | Senha da Distribuidora | Credencial textual | Campo ClickUp; fallback `Sem senha na ClickUp`. |
| S | Status de faturamento | Categoria textual | Status PowerRev traduzido para português. |
| T | Captura de Faturas | Booleano protegido | Copiado de `Faturas!O` após o espelho de captura. |
| U | Provider | Categoria textual | `providerName` da PowerRev. |
| V | Observações Financeiras | Texto ClickUp | Campo `dead0...`, convertido de rich text quando necessário. |
| W | Observações de Cobrança | Texto ClickUp por competência | Trecho do campo `daae...` correspondente ao mês da linha. |
| X | Validação | Estado técnico textual | Vazio, `Não processado` ou `Erro no sistema`. |
| Y | Início de Operação | Data ou marcador textual | Data ClickUp ou `Sem data de inicio definida`. |
| Z | Fim de Operação | Data ou marcador textual | Data ClickUp ou `Sem data de fim definida.` |
| AA | Invoice ID | Identificador técnico | ID real PowerRev, ID sintético `SYN|...` ou vazio em placeholder. |

### Contrato semântico de texto e métricas

Os campos da planilha se dividem em identificadores, dimensões textuais, textos
descritivos, métricas numéricas, booleanos e datas. Essa classificação deve orientar
validações, filtros e novas integrações.

#### Identificadores e categorias textuais

- `Task ID`, `UC`, `UC Aneel` e `Invoice ID` são identificadores, não métricas.
  Devem permanecer como texto e nunca ser somados ou convertidos para número.
- A normalização de UC é usada somente para match. Ela não autoriza alterar o valor
  exibido nem remover zeros à esquerda.
- Quando a PowerRev identifica uma invoice pela `UC Aneel` e a planilha usa outra
  `UC` na coluna C, o sync associa as duas somente se a UC Aneel aponta para uma
  única UC da planilha e não é também uma UC direta. A coluna C conserva seu valor.
- Status, plano, distribuidora, tipo de faturamento, provider e produto são dimensões
  categóricas. IDs internos de dropdown devem ser resolvidos para labels humanas.
- IDs sintéticos iniciados por `SYN|` são identidades determinísticas de linhas sem
  ID definitivo. Eles não representam uma invoice real da PowerRev.

#### Textos descritivos

- Texto vazio significa ausência de conteúdo; não equivale a `0`, `FALSE` ou a um
  marcador como `Sem data...`.
- `Observações` é manual e tem prioridade sobre qualquer reconstrução.
- `Observações Financeiras` acompanha integralmente o valor atual do campo ClickUp.
  O delta também pode limpar a célula quando o campo for apagado na task.
- Rich text do ClickUp no formato Quill Delta é convertido em texto puro. Se o valor
  não for JSON reconhecido, o texto original é mantido, inclusive quando começa com
  palavras como `pagamentos lançados`.
- Marcadores como `Sem login na ClickUp`, `Sem senha na ClickUp`,
  `Sem data de inicio definida` e `Sem data de fim definida.` são estados textuais
  explícitos. Eles não devem entrar em cálculos.

#### Observações de Cobrança

O campo de cobrança contém blocos mensais. Exemplos aceitos de marcador incluem:

```text
06/2026 - Descontar crédito pago em duplicidade
jun/2026 - Descontar crédito pago em duplicidade
junho de 2026 - Descontar crédito pago em duplicidade
06-26 - Descontar crédito pago em duplicidade
```

Regras do parser:

1. aceita mês numérico ou nome/abreviação em português, com ano de dois ou quatro
   dígitos;
2. reconhece separadores `/`, `-`, `.`, espaço e `de`;
3. cada marcador inicia um trecho que termina no próximo marcador mensal;
4. separadores, vírgulas, dois-pontos e traços nas bordas do trecho são removidos;
5. múltiplos trechos da mesma competência são unidos por `, `;
6. somente o texto da competência da linha é escrito em W;
7. texto anterior ao primeiro marcador ou sem competência reconhecível não é
   associado automaticamente a nenhuma linha.

O delta do ClickUp recalcula W para todas as linhas já existentes da task. Uma task
nova ou uma competência sem linha continua dependendo do próximo full.

#### Métricas numéricas

| Métrica | Unidade | Regra de ausência e zero |
|---|---|---|
| Valor do boleto | R$ | `None` ou vazio vira célula vazia; zero é válido; texto não parseável é mantido como texto. |
| Mês de atandimento | inteiro ordinal | Começa em 1 por UC e avança por competência distinta; não conta invoices adicionais do mesmo mês. |
| Consumo Total, espelho de detalhes | kWh | Soma dos valores absolutos de consumo HFP e HP; vazio se nenhum componente existir; zero é válido. |
| Energia compensada, espelho de detalhes | kWh | Soma dos valores absolutos de compensação HFP e HP; vazio se nenhum componente existir; zero é válido. |
| R$ Compensados, espelho de detalhes | R$ | Soma absoluta de HFP, HP e códigos `VL_COMPENSADA_BAND_*`; vazio se nenhum componente existir; zero é válido. |

O valor principal N não aplica `abs`: ele representa o `total` recebido da billing
invoice. A regra de ignorar sinal é aplicada especificamente às três métricas
extraídas do detalhe da PowerRev.

#### Booleanos

- `TRUE` e `FALSE` são valores válidos; `FALSE` não significa ausência.
- `Faturas!O` é forçada para validação visual de checkbox.
- `Faturamento!T` recebe os valores de `Faturas!O` após o espelho e também é
  formatada como checkbox.
- Uma alteração manual de `TRUE` para `FALSE` em `Faturas!O` deve ser considerada
  uma atualização real e propagada no ciclo seguinte.

#### Datas

- `Envio do boleto` usa o dia de envio e o deslocamento mensal do ClickUp.
- `Data de Vencimento` usa seu próprio deslocamento; quando o dia de vencimento é
  menor que o dia de envio, avança mais um mês para não ficar antes do envio.
- `Data de Emissão da fatura` nasce em status `Emitida` ou `Reemitida`.
- Em status posteriores à emissão, como `Paga`, `Vencida`, `Paga Externamente`,
  `Negociada`, `Cancelada` e `Expirada`, a data já existente é preservada para a
  mesma invoice.
- Em status anteriores à emissão, O fica vazia.
- `Data de Emissão` do espelho de captura tem outra origem: `dtEmissao` do endpoint
  mensal `/invoice?nuAnoMes=YYYYMM`.
- `Data de vencimento Fatura Sion`, no espelho de detalhes, vem de `dueDate` do
  detalhe da billing invoice.

### Colunas manuais protegidas

As colunas protegidas no full e no delta principal são:

- M, `Observações`;
- P, `Data de Faturamento`;
- T, `Captura de Faturas`.

Antes de um full, o código captura os valores visíveis dessas colunas. A restauração
usa, nesta ordem:

1. `UC + Mês de Referencia + Invoice ID`;
2. uma chave secundária com UC, mês, emissão, provider, status e valor para casos
   sintéticos;
3. `UC + Mês de Referencia`, somente quando a correspondência é segura.

A migração de uma linha sintética ou sem ID para uma invoice real usa o fallback
UC/mês apenas quando há exatamente um candidato manual. Se houver várias linhas na
mesma UC/competência, IDs reais diferentes mantêm as linhas separadas e o código não
deve escolher arbitrariamente qual conteúdo manual migrar.

As colunas protegidas são projetadas nas novas linhas **antes da primeira escrita**
do full. Ao final, chaves sem correspondência são registradas como dados órfãos no
log; esse aviso indica que o valor não pôde ser associado com segurança.

## Filtros de Status Detalhado

Tasks com qualquer um dos status abaixo são removidas no full e no delta:

- `Planejamento - Black`;
- `Aguardando Cadastro - Usina`;
- `Aguardando Cadastro - em Contingência`;
- `Novo Cooperado`;
- `Novo Cooperado - Em Contingência`;
- `Cancelado - Autoconsumo`;
- `Cadastro em andamento`;
- `Cadastro em andamento - Em Contingência`;
- `Aguardando saída de concorrente`.

Além disso, tasks sem início e sem fim de operação são removidas quando estiverem em
`Eliminado`, `Excluido`, `Demitido`, `Encerrado - Financeiro` ou `Baixo Consumo`.

Status como `Retirado da Usina - Black` e `A Retirar da Usina - Black` não fazem
parte da lista bloqueada e podem aparecer quando as demais condições forem atendidas.

## Full sync detalhado

1. Busca todas as tasks das listas, incluindo fechadas.
2. Aplica os filtros de status.
3. Monta o mapa UC para task e executa o fallback controlado no workspace.
4. Resolve sobreposições e trocas de plano por competência.
5. Define o período PowerRev da menor data de início até o mês atual + 1.
6. Busca invoices simples e agrupadas mês a mês.
7. Gera linhas de invoices e placeholders dentro da vigência.
8. Preserva linhas que desapareceram temporariamente, marcando
   `Validação = Erro no sistema`, salvo quando uma regra segura de substituição se
   aplica.
9. Captura e projeta as colunas manuais protegidas.
10. Reescreve os valores da aba em chunks, sem limpar formatação visual.
11. Executa o espelho de captura se ele estiver habilitado.

Se a PowerRev não retornar dados no full, o código não apaga automaticamente as
linhas existentes: ele tenta preservá-las e marca a validação de integridade.

## Delta sync detalhado

1. Busca tasks alteradas desde o último cursor com a sobreposição configurada.
2. Remove imediatamente linhas de tasks que passaram para status bloqueado.
3. Atualiza nas linhas existentes status, UCs, razão social, plano, distribuidora,
   tipo, credenciais, vigência, datas calculadas e as duas observações do ClickUp.
4. Tasks novas são conhecidas pelo processo, mas suas linhas só nascem no próximo
   full sync.
5. Consulta a PowerRev para o mês anterior, atual e seguinte.
6. Atualiza apenas linhas existentes quando encontra correspondência segura.
7. Invoices novas sem linha são registradas no log e entram no próximo full.
8. Recalcula `Mês de atandimento` por UC e competência.
9. Executa o espelho de captura se ele estiver habilitado.

O delta não reconstrói todo o histórico e não executa o espelho de detalhes.

## Espelho de captura de faturas

### Layout A:T

| Coluna | Nome | Origem ou regra |
|---|---|---|
| A | Task ID | `Faturamento!A` |
| B | Status Detalhado | `Faturamento!B` |
| C | UC | `Faturamento!C` |
| D | UC Aneel | `Faturamento!D` |
| E | Distribuidora | `Faturamento!K` |
| F | Razão Social | `Faturamento!E` |
| G | Mês de Referencia | `Faturamento!F` |
| H | Envio do boleto | `Faturamento!G` |
| I | Data de Vencimento | `Faturamento!H` |
| J | Data de Emissão | `dtEmissao` de `/invoice?nuAnoMes`, com fallbacks. |
| K | Data de Envio Ideal | Faixa calculada a partir do dia de emissão. |
| L | Mês de envio do boleto | `Mês Atual` ou `Mês Seguinte`. |
| M | Observações | Manual protegida. |
| N | Status de Fatura | `Faturamento!S` |
| O | Concluido | Manual protegida e checkbox. |
| P | Login da Distribuidora | `Faturamento!Q` |
| Q | Senha da Distribuidora | `Faturamento!R` |
| R | Invoice ID | `Faturamento!AA` |
| S | Observações Financeiras | `Faturamento!V` |
| T | Observações de Cobrança | `Faturamento!W` |

Para J, a prioridade é:

1. `dtEmissao` da invoice normal da mesma UC e competência;
2. dia mais recente conhecido da mesma UC em competência anterior ou igual;
3. campo de dia de emissão do ClickUp.

Com o dia resolvido, K usa as faixas `1 -> 10`, `2..11 -> 20`, `12..21 -> 30`
e `22..31 -> 10`. L é `Mês Seguinte` quando o dia de emissão é maior que o dia
ideal; caso contrário, é `Mês Atual`.

### Preservação de M e O

O estado visível de `Observações` e `Concluido` é a fonte prioritária. Antes de
reescrever o espelho, o módulo salva esse estado na aba técnica. A associação usa:

1. Invoice ID real, quando existe;
2. identidade estável `Task ID + UC + Mês de Referencia` para linhas sintéticas ou
   para a transição até o ID real;
3. número de ocorrência para manter linhas distintas que compartilham a identidade
   estável.

O arquivo técnico é escrito antes da reconstrução visível. Uma origem vazia
inesperada cancela a sincronização se o destino já possui linhas, evitando apagar o
espelho e seus campos manuais.

Depois que `Faturas` está consistente, o módulo copia `Faturas!O` para
`Faturamento!T`, na mesma ordem de linhas. Essa é a única coluna da origem principal
que o espelho atualiza.

## Espelho semanal de detalhes

O espelho seleciona todas as linhas não vazias de UC da aba `Faturamento`. Somente
Invoice IDs reais são usados para chamadas de detalhe; placeholders e IDs `SYN|...`
permanecem com as métricas de detalhe vazias.

Layout A:T:

1. Status Detalhado
2. UC
3. UC Aneel
4. Razão Social
5. Mês de Referencia
6. Envio do boleto
7. Data de Vencimento
8. Mês de atandimento
9. Plano de Adesão
10. Distribuidora
11. Valor do boleto
12. Data de Emissão da fatura
13. Provider
14. Status de faturamento
15. Produto
16. Data de Pagamento
17. Data de vencimento Fatura Sion
18. Consumo Total
19. Energia compensada
20. R$ Compensados

`Data de Pagamento` permanece vazia porque não há origem confiável implementada.
`Produto` vem do ClickUp. As demais colunas até Status de faturamento são copiadas
da aba principal.

Métricas do detalhe:

- Data de vencimento Fatura Sion: `dueDate`;
- Consumo Total: valor absoluto de HFP + HP, usando os códigos
  `QT_ENERGIA_CONSUMIDA_*`;
- Energia compensada: valor absoluto de HFP + HP, usando
  `QT_ENERGIA_COMPENSADA_*`;
- R$ Compensados: valor absoluto das parcelas HFP, HP e todas as bandeiras
  `VL_COMPENSADA_BAND_*`.

O cache usa o Invoice ID como chave. Somente IDs ausentes no cache geram chamada a
`/billing/invoice/{id}`. O padrão atual espaça as chamadas em 5 segundos e faz uma
pausa de 60 segundos a cada 100 requests; esses três valores são configuráveis. Uma
falha individual produz métricas vazias para aquela invoice e não cancela as demais.

## Variáveis de ambiente importantes

### Integrações

- `CLICKUP_TOKEN`
- `CLICKUP_TEAM_ID`
- `SPREADSHEET_ID`
- `SHEET_TAB_NAME`
- `GOOGLE_CREDENTIALS_JSON`
- `GOOGLE_CREDENTIALS_FILE`
- `POWERREV_BASE_URL`
- `POWERREV_AUTH_URL`
- `POWERREV_ACCOUNT_ID`
- `POWERREV_API_KEY`
- `POWERREV_TIMEOUT`
- `POWERREV_DELAY`
- `POWERREV_MAX_RETRIES`
- `POWERREV_PAGE_LIMIT`

### Ciclos e segurança

- `FULL_SYNC_DAILY_TIME`
- `FULL_SYNC_TIMEZONE`
- `FULL_SYNC_RETRY_BASE_S`
- `FULL_SYNC_RETRY_MAX_S`
- `CLICKUP_DELTA_LOOKBACK_SECONDS`
- `DISTRIBUTED_LOCK_ENABLED`
- `DISTRIBUTED_LOCK_TTL_S`
- `DISTRIBUTED_LOCK_REFRESH_S`
- `DISTRIBUTED_LOCK_TAB_NAME`
- `SHEETS_MAX_WRITE_REQUESTS_PER_MIN`
- `SHEETS_MAX_RETRIES`
- `SHEETS_RETRY_BASE_S`
- `SHEETS_RETRY_MAX_BACKOFF_S`

`DELTA_SYNC_INTERVAL_S` está fixado em 600 segundos no código atual; não é lido do
ambiente. `POWERREV_REFERENCE_MONTH_ONLY` é um limitador de diagnóstico e deve ficar
vazio em produção normal.

### Espelho de captura

- `INVOICE_CAPTURE_MIRROR_ENABLED`
- `INVOICE_CAPTURE_SPREADSHEET_ID`
- `INVOICE_CAPTURE_SHEET_TAB_NAME`
- `INVOICE_CAPTURE_OBSERVATIONS_TAB_NAME`

### Espelho de detalhes

- `BILLING_DETAILS_MIRROR_ENABLED`
- `BILLING_DETAILS_MIRROR_SPREADSHEET_ID`
- `BILLING_DETAILS_MIRROR_SHEET_TAB_NAME`
- `BILLING_DETAILS_MIRROR_CACHE_TAB_NAME`
- `BILLING_DETAILS_POWERREV_DETAIL_INTERVAL_SECONDS`
- `BILLING_DETAILS_POWERREV_DETAIL_BATCH_SIZE`
- `BILLING_DETAILS_POWERREV_DETAIL_BATCH_COOLDOWN_SECONDS`

## Diagnóstico rápido

### Cooperado ausente

Verificar, nesta ordem:

1. se a task pertence a uma das listas permitidas;
2. se o Status Detalhado está bloqueado;
3. se há UC;
4. se a competência está dentro da vigência;
5. se a task é nova, caso em que o delta não cria a linha;
6. se o full registrou fallback, conflito de vigência ou ausência de invoice;
7. se a linha existe como placeholder com `Não processado`.

### Texto de cobrança ausente

Verificar:

1. o campo `daae...` na task correta;
2. a presença de um marcador mensal reconhecível;
3. se o mês do marcador corresponde à competência da linha;
4. se a task já possuía linha no momento do delta;
5. os logs `Delta ClickUp ALTERACAO`.

### Valor manual ausente

Verificar:

1. se a coluna está entre as protegidas daquele módulo;
2. a identidade UC/mês/Invoice ID na aba principal;
3. a identidade Task ID/UC/mês e a aba `__Faturas_Observacoes` no espelho;
4. avisos de chave órfã, cabeçalho inesperado ou origem vazia;
5. se houve ambiguidade entre várias linhas sintéticas e uma nova invoice real.

### Espelho de detalhes lento

Verificar a quantidade `pendentes` após a leitura do cache. Com o padrão de 5
segundos por request e cooldown de 60 segundos a cada 100, um cache inicial de
milhares de invoices é deliberadamente demorado para respeitar o limite da PowerRev.

## Arquivos de código mais relevantes

- `poll.py`: filtros, vigência, full, delta, integridade e agendamento.
- `field_map.py`: schema A:AA e IDs dos campos ClickUp.
- `row_expander.py`: datas, observações e montagem da linha.
- `powerrev_client.py`: invoices simples/agrupadas, IDs sintéticos e statuses.
- `sheets_manager.py`: escrita principal e proteção de M, P e T.
- `invoice_capture_mirror.py`: layout A:T, `dtEmissao`, checkboxes e arquivo técnico.
- `billing_details_mirror.py`: layout A:T, cache e métricas de detalhe.
- `clickup_client.py`: paginação, retries e cache de campos.
- `config.py`: listas, planilhas, flags e parâmetros operacionais.
