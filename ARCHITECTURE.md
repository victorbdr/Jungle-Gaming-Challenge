# ARCHITECTURE

Este documento registra as decisões técnicas, os trade-offs e as limitações conhecidas. O princípio central:

> **O banco de dados é a fonte da verdade das invariantes.** A aplicação aplica as regras e as executa no caminho feliz; o schema (constraints, índices únicos parciais e triggers) recusa qualquer estado inválido, mesmo que a aplicação tenha um bug ou que alguém escreva SQL direto. Recursos do broker (FIFO, deduplicação) são otimizações, não garantias.

---

## 1. Visão geral

```
            HTTP (Nest)                    SQS wager-transactions.fifo
                │                                   │
                ▼                                   ▼
        WageringController                   WagerSqsConsumer ──(permanente/esgotado)──▶ DLQ
                │  toWagerCommand (zod + Money + payloadHash)   │
                └──────────────┬────────────────────────────────┘
                               ▼
                ProcessWagerTransactionUseCase   (mesmo use case para as duas entradas)
                               │  1 transação SQL
     ┌─────────────────────────┼───────────────────────────────────────────┐
     │ inbox (só SQS) → idempotência → SELECT wallet FOR UPDATE →            │
     │ decideWager (domínio puro) → transação + ledger + UPDATE saldo (versão) │
     │ → outbox                                                               │
     └─────────────────────────┬───────────────────────────────────────────┘
                               ▼ COMMIT
                 OutboxRelay (FOR UPDATE SKIP LOCKED) ──▶ SQS wagering-events.fifo
                 ResolvePendingReferences (worker)  ──▶ reaplica decideWager
```

Camadas e dependências apontam sempre para dentro:

| Camada | Conteúdo | Depende de |
|---|---|---|
| `*/domain` | `Money`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `decideWager`, eventos, inbox e outbox | nada (só `decimal.js`, `uuid` e `node:crypto`) |
| `application` | use cases e **portas** (`TransactionRunner`, `WageringUnitOfWork`, `WalletQueries`, `OutboxStore`, `EventPublisher`, `Metrics`, `Clock`, `AppLogger`) | domínio |
| `infrastructure` | adapters MikroORM, Nest, SQS, pino, prom-client | aplicação e domínio |
| `bootstrap` | raiz de composição sem framework (usada pelo Nest, pelos workers e pelos testes) | todas |

O domínio não conhece Nest nem MikroORM. Os agregados usam construtor `private` e factories (`create`/`open`/`opening`, `rehydrate`); `rehydrate` não revalida transições. As regras de negócio ficam numa função pura (`decideWager`), testada sem I/O.

---

## 2. ORM: MikroORM

**Por que MikroORM:** é a opção preferencial do desafio e oferece `em.transactional()`, `LockMode` (`PESSIMISTIC_WRITE` e `PESSIMISTIC_PARTIAL_WRITE` = `FOR UPDATE SKIP LOCKED`) e `fork()` por unidade de trabalho, sem Identity Map global (`allowGlobalContext: false`).

**Como foi usado:**

- Entidades de **persistência** (`WalletRecord`, `WagerTransactionRecord`, …) com `defineEntity`, sem decorators. Os agregados de domínio são classes separadas, e **mappers** convertem registro ↔ agregado (`rehydrate`).
- Cada use case roda em `orm.em.fork().transactional(...)`. A primeira instrução é `SET LOCAL lock_timeout`.
- **Escritas explícitas** (`em.insert`, `em.nativeUpdate`, SQL nativo onde é preciso `ON CONFLICT` ou `SKIP LOCKED`) em vez de `flush` implícito. Motivo: a ordem das escritas importa (FKs, o trigger de encadeamento do ledger e o `UPDATE ... WHERE version = ?`). Explicitar a ordem deixa a correção auditável no código. Assim o Unit of Work do MikroORM é usado como **escopo transacional**, não como rastreador de mudanças. É um trade-off consciente: menos "mágica", mais previsibilidade.
- Migrations com o `Migrator` do MikroORM, mas escritas **à mão em SQL** (`up`/`down`), porque constraints, índices parciais e triggers são parte central da avaliação e não são bem expressos pelo gerador de schema. A lista de migrations é explícita (`migrationsList`), sem varredura de disco.

**Mapeamento de `Money`:**

| Camada | Representação |
|---|---|
| Contrato (HTTP/SQS/eventos) | `{ "amount": "25.00", "currency": "BRL" }`, com amount como string |
| Domínio | `Money` imutável sobre `decimal.js` (instância isolada, precisão 40), escala fixa de 2 |
| Banco | `numeric(20,2)` + `char(3)` em colunas separadas |
| ORM | `p.decimal('string')`: o driver devolve string, nunca `number` |

`number` nunca é usado para dinheiro. A entrada aceita apenas `^\d{1,18}(\.\d{1,2})?$`: rejeita `NaN`, `Infinity`, notação científica, vazio, negativos, sinal `+`, vírgula e mais de 2 casas. **Não há arredondamento**: um valor com 3 casas é erro de contrato, não algo a arredondar em silêncio. Somas e subtrações de valores com 2 casas são exatas, então nunca precisam de arredondamento. O resultado é serializado sempre com 2 casas (`"25"` vira `"25.00"`).

O modelo é multi-moeda (a moeda da operação precisa ser igual à da wallet; `CurrencyMismatchError` no domínio e `CURRENCY_MISMATCH` na rejeição). Os testes usam BRL e USD.

---

## 3. Concorrência

**Unidade de concorrência:** a linha da wallet.

**Estratégia: lock pessimista por wallet**, com um update condicionado como defesa em profundidade.

1. `SELECT ... FROM wallets WHERE id = ? FOR UPDATE`: serializa todas as operações da mesma wallet, em qualquer instância. Wallets diferentes não disputam nada (não existe lock global).
2. Sob o lock: decisão de domínio, inserção da transação e do lançamento, e depois `UPDATE wallets SET balance=?, version=? WHERE id=? AND version=<anterior>`. Se o update afetar 0 linhas, é `ConcurrentWriteError`. Com o `FOR UPDATE` isso não deveria acontecer; está ali para pegar regressões.
3. `CHECK (balance >= 0)` e os triggers do ledger tornam saldo negativo ou divergente **impossível no banco**, mesmo que (1) e (2) falhem.

**Por que pessimista e não otimista:** o cenário típico de iGaming tem **hot wallets** (o mesmo jogador fazendo rajadas de apostas). Com lock otimista, a contenção vira tempestade de retries, latência imprevisível e um limite de tentativas que acaba devolvendo erro ao provedor. Com `FOR UPDATE`, as operações da mesma wallet formam uma fila curta e determinística (a seção crítica dura poucos ms), e o teste de carga registrou **0 conflitos e 0 retries** com 20% do tráfego numa única wallet. O custo é segurar uma conexão enquanto se espera o lock. Isso é limitado por `lock_timeout` (5 s, configurável): ao estourar, a resposta é `503 retryable`, nunca uma espera infinita.

**Ordem de locks (evita deadlock):** sempre **wallet → linha da transação**. O worker de referências pendentes trava a wallet antes de travar a transação pendente. O "wake" de pendências usa `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)`, que nunca espera.

**Retry de concorrência:** `withConcurrencyRetry` repete a operação inteira (numa transação nova) em `unique_violation`, deadlock, serialização, `lock_timeout` e versão divergente. São no máximo 4 tentativas com jitter; depois disso, `TransientError` (503 / retry SQS).

---

## 4. Garantias no schema

| Garantia | Mecanismo no PostgreSQL |
|---|---|
| Uma wallet por `playerId + currency` | `UNIQUE (player_id, currency)` |
| Saldo nunca negativo | `CHECK (balance >= 0)` na wallet; `CHECK (balance_before >= 0)` e `CHECK (balance_after >= 0)` no ledger |
| Lançamento aritmeticamente correto | `CHECK (balance_after = balance_before ± amount)` |
| No máximo um lançamento por transação e wallet | `UNIQUE (transaction_id, wallet_id)` |
| Ledger imutável | triggers `BEFORE UPDATE/DELETE` (por linha) e `BEFORE TRUNCATE` que lançam exceção |
| Ledger encadeado | trigger `BEFORE INSERT`: `balance_before` = `balance_after` do último lançamento da wallet (ou 0); moeda = moeda da wallet |
| **Saldo materializado = ledger** | `CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED` em `wallets`: no **commit**, `balance` = `balance_after` do último lançamento |
| Idempotência | `UNIQUE (idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)` |
| Uma reversão por referência | índice único parcial `(reference_transaction_id) WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK')` |
| Referência obrigatória ou proibida por kind | `CHECK`s em `kind` × `reference_external_transaction_id` |
| Coerência de estado | `CHECK`s: terminal ⇔ `processed_at`; `REJECTED/FAILED` ⇔ `failure_code`; `PENDING_REFERENCE` ⇒ `next_reference_attempt_at`; `PROCESSED` com referência ⇒ `reference_transaction_id` |
| Transação terminal imutável | trigger `BEFORE UPDATE/DELETE`: recusa qualquer update de linha terminal, mudança de campos de negócio e delete |
| Wallets não são excluídas | trigger `BEFORE DELETE` |
| Inbox | `PRIMARY KEY (consumer_name, message_id)` |

O teste `test/integration/schema.test.ts` tenta violar cada uma dessas garantias com **SQL direto** e confirma que o banco recusa.

`wager_transactions.wallet_id` **não** tem FK de propósito. Uma transação para uma wallet inexistente é persistida como `REJECTED / WALLET_NOT_FOUND`, para que o replay idempotente devolva o mesmo resultado. O ledger tem FK tanto para a wallet quanto para a transação.

---

## 5. Idempotência

- **Fonte da verdade:** o header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS), persistido com `UNIQUE`. Nada fica em memória.
- **`payloadHash`:** `sha256` (hex) do **JSON canônico** dos campos de negócio: `providerId`, `externalTransactionId`, `playerId`, `walletId` (minúsculo), `roundId`, `gameId`, `kind`, `money` (amount **normalizado** para 2 casas) e `referenceExternalTransactionId` (quando presente). JSON canônico significa chaves de objeto em ordem lexicográfica (por code point), sem espaços, com chaves `undefined` omitidas e arrays na ordem original. A idempotency key e os metadados de transporte (headers, `messageId`, `occurredAt`) **não** entram no hash.
- **Replay:** mesma key e mesmo hash devolvem a transação persistida, com o saldo **observado naquele momento** (`result_balance_amount`), o mesmo status HTTP e `idempotentReplay: true`.
- **Conflito:** mesma key e hash diferente devolvem `409 IDEMPOTENCY_CONFLICT` (no SQS, a mensagem vai para a DLQ). Nunca é tratado como replay.
- **Corrida:** primeiro há uma checagem sem lock (caminho rápido do replay), depois o lock da wallet e uma **re-checagem sob o lock**. Duas requisições com a mesma key e wallets diferentes (ou wallet inexistente) colidem no `UNIQUE`; a operação é repetida uma vez e cai no replay ou no conflito.
- **Inbox (SQS):** `INSERT ... ON CONFLICT DO NOTHING` em `(consumer_name, message_id)`, como **primeira instrução da mesma transação**. Um consumidor concorrente com a mesma mensagem fica bloqueado no índice até o primeiro commitar, e então vê a duplicata. Se o primeiro sofrer rollback, o segundo processa. O `messageId` usado é o do envelope de negócio (estável entre reenvios do produtor), não o `MessageId` do SQS.

Comprovado por testes: a mesma aposta 50 vezes em paralelo (num processo e distribuída entre 3 instâncias) gera **um** débito; uma mensagem SQS duplicada gera um efeito; o processo morto depois do commit e antes do ack tem a redelivery reconhecida.

---

## 6. Modelo de transação e regras

### Estados

```
PENDING ──┬─▶ PROCESSED   (terminal)
          ├─▶ REJECTED    (terminal, failureCode)
          ├─▶ FAILED      (terminal, failureCode; erro permanente de infraestrutura)
          └─▶ PENDING_REFERENCE ──┬─▶ PROCESSED | REJECTED | FAILED
                                  └─▶ PENDING_REFERENCE (nova tentativa agendada)
```

Transicionar a partir de um estado terminal lança `InvalidTransactionStateError` (erro de programação), e o trigger do banco também recusa. `PENDING` existe no modelo, mas, como a decisão acontece na mesma transação da criação, nenhuma linha chega a ser persistida em `PENDING`.

### Regras (seção 7 do desafio) e interpretações adotadas

| Kind | Efeito | Observações |
|---|---|---|
| `OPENING` | crédito | interno, criado por `POST /wallets` na mesma transação da wallet; recusado na API e na fila (400 / DLQ) |
| `BET` | débito | `INSUFFICIENT_FUNDS` se o saldo não cobrir |
| `WIN` | crédito | referência **opcional**; se informada, precisa ser uma `BET` `PROCESSED` do mesmo contexto |
| `LOSS` | nenhum | sem lançamento; amount ≥ 0 (pode ser `0.00`); gera `WagerTransactionProcessed` |
| `REFUND` | crédito | só referencia `BET`; amount igual ao da referência |
| `ROLLBACK` | inverso da referência | referencia `BET` (crédito), `WIN` ou `REFUND` (débito); amount igual |

Interpretações:

1. **Uma referência só pode ser revertida uma vez, independentemente do tipo.** O enunciado diz "pelo mesmo tipo de operação", mas permitir `REFUND` **e** `ROLLBACK` sobre a mesma `BET` creditaria o jogador duas vezes. Adotei a regra mais restritiva, garantida pelo índice único parcial. Reverter a própria reversão é outra referência e é permitido (ex.: `ROLLBACK` de um `REFUND`).
2. "Mesmo contexto" significa mesmo provider (já implícito na busca por `(providerId, referenceExternalTransactionId)`), player, wallet, moeda e rodada. Fora disso: `REFERENCE_MISMATCH`.
3. Uma referência que existe mas ainda não é terminal (ex.: ela própria em `PENDING_REFERENCE`) é tratada como ausente: aguarda.
4. Uma referência terminal que não está `PROCESSED` (ex.: `BET` rejeitada) dá `REFERENCE_NOT_PROCESSED`.
5. Uma reversão que deixaria o saldo negativo dá `REVERSAL_WOULD_OVERDRAW`, distinto de `INSUFFICIENT_FUNDS`.
6. Toda transação rejeitada é persistida (auditável e base do replay), sem lançamento e sem alterar o saldo.
7. `version` da wallet começa em 1 (já com o saldo inicial) e só incrementa quando o saldo muda.

### Failure codes

| Código | Quando | O provedor deve |
|---|---|---|
| `INSUFFICIENT_FUNDS` | BET maior que o saldo | não reenviar (resultado definitivo) |
| `REVERSAL_WOULD_OVERDRAW` | ROLLBACK de WIN/REFUND com saldo insuficiente | escalar para operação manual |
| `WALLET_NOT_FOUND` | wallet inexistente | corrigir `walletId` e enviar com **nova** key |
| `WALLET_PLAYER_MISMATCH` | wallet de outro player | corrigir o payload (nova key) |
| `CURRENCY_MISMATCH` | moeda ≠ moeda da wallet | corrigir o payload (nova key) |
| `REFERENCE_NOT_FOUND` | referência não apareceu dentro da janela de retry | investigar; reenviar com nova key se a referência chegar depois |
| `REFERENCE_MISMATCH` | referência de outro player/wallet/moeda/rodada | corrigir o payload |
| `REFERENCE_KIND_NOT_ALLOWED` | ex.: REFUND de WIN, ROLLBACK de LOSS | corrigir o payload |
| `REFERENCE_NOT_PROCESSED` | referência terminal não processada (ex.: rejeitada) | não reenviar |
| `REFERENCE_ALREADY_REVERSED` | já existe reversão processada | não reenviar |
| `REVERSAL_AMOUNT_MISMATCH` | valor ≠ valor da referência (reversão parcial fora de escopo) | corrigir o payload |
| `PROCESSING_FAILED` | `FAILED`: erro permanente de infraestrutura ao reprocessar | intervenção operacional |

Como a idempotency key fica "gasta" com o resultado, uma correção exige um novo `externalTransactionId` / key.

### Status HTTP

Consistentes em todos os endpoints (filtro único `ApplicationErrorFilter`). O corpo de erro é `{ error: { code, message, retryable, details? } }`:

| Situação | Status |
|---|---|
| Processada | `200` (criação de wallet: `201`) |
| Aceita, processamento pendente (`PENDING_REFERENCE`) | `202` |
| Payload inválido / header ausente | `400` |
| Recurso inexistente | `404` |
| Conflito de idempotência / wallet duplicada | `409` (`IDEMPOTENCY_CONFLICT`, `WALLET_ALREADY_EXISTS`) |
| Rejeição de negócio | `422`, com a transação `REJECTED` e o `failureCode` no corpo |
| Falha transitória (banco indisponível, lock timeout, retries esgotados) | `503` + `Retry-After` |
| Erro inesperado | `500` |

O `422` foi escolhido para separar "o pedido foi entendido e recusado por regra" (definitivo) de "o pedido está malformado" (`400`) e de "tente de novo" (`503`).

---

## 7. Mensageria

### Consumidor SQS (`WagerSqsConsumer`)

- Long polling (`WaitTimeSeconds=5`) e lotes de até 10 mensagens. **FIFO:** processamento sequencial dentro de cada `MessageGroupId` (o produtor usa `walletId`) e paralelo entre grupos.
- Usa o **mesmo use case** do HTTP, com o contexto `inbox`.
- **Ack (`DeleteMessage`) só depois do commit.** Se o processo morrer antes do ack, a mensagem volta e a inbox a reconhece (teste com processo real morto via `FAULT_INJECTION=crash_before_ack`, uma variável de teste).
- Classificação:

| Erro | Ação |
|---|---|
| sucesso, rejeição de negócio, duplicata de inbox | **ack** |
| `TransientError` / `ConcurrentWriteError` / erro inesperado | **retry**: `ChangeMessageVisibility` com backoff `2^receiveCount` s (máx. 300 s); as mensagens seguintes do mesmo grupo têm a visibilidade devolvida para preservar a ordem |
| JSON inválido, envelope inválido, payload inválido, conflito de idempotência | **DLQ** imediatamente (permanente) |
| `ApproximateReceiveCount ≥ SQS_MAX_ATTEMPTS` (5) | **DLQ** (`retries_exhausted` / `unexpected_error`) |

  O envio para a DLQ é **explícito** (com o atributo `dlqReason` e métrica), e a redrive policy da fila serve de rede de segurança.
- **SIGTERM:** para de receber (aborta o long poll), espera as mensagens em andamento (até 20 s), devolve com visibilidade 0 as mensagens recebidas e não iniciadas, fecha o HTTP e depois o pool do banco. Em Linux (Docker) isso foi verificado com `docker compose stop`. No Windows, `kill` termina o processo à força, por isso o teste automatizado de shutdown gracioso roda no próprio processo.

### Transactional Outbox

- Os eventos são gravados em `outbox_messages` **na mesma transação** da alteração financeira, então nenhum evento é publicado antes do commit.
- `OutboxRelay`: `SELECT ... WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY ... LIMIT n FOR UPDATE SKIP LOCKED`, publica com `SendMessageBatch` (lotes de 10) e marca publicado ou agenda retry (backoff exponencial por mensagem, até 1 min, sem limite de tentativas: evento confirmado não se perde) **na mesma transação** da reivindicação.
- Vários publishers são seguros: o `SKIP LOCKED` evita que duas instâncias publiquem a mesma linha ao mesmo tempo. Testado com 2 relays concorrentes: 0 duplicatas e 0 perdas.
- Crash depois de publicar e antes de commitar a marcação faz o evento ser **republicado** (at-least-once). Por isso o id da linha é o próprio `eventId`, usado como `MessageDeduplicationId` (o broker deduplica em 5 min) e presente no envelope, para o consumidor final deduplicar.
- Ordem: `MessageGroupId = aggregateId` e lotes sequenciais preservam a ordem dentro de um relay. Entre relays concorrentes, a ordem por agregado **não** é estrita. Consumidores de `WalletBalanceChanged` devem usar `walletVersion` (monotônico) para ordenar e descartar eventos antigos.

### Eventos

Classe abstrata `IntegrationEvent<T>`, com `eventType` e `version` **no tipo** de cada subclasse: `WagerTransactionProcessed` (inclusive `LOSS` e `OPENING`), `WagerTransactionRejected`, `WalletBalanceChanged` (só quando o saldo muda) e `WagerTransactionPendingReference` (só na primeira vez que entra em pendência, não a cada retry). O `data` carrega `MoneyProps` (string), nunca a instância de `Money`. O envelope inclui `eventId`, `correlationId` e `causationId`.

### Referências fora de ordem

- A transação fica `PENDING_REFERENCE` com `next_reference_attempt_at` (backoff 1, 2, 4, …, 256 s com jitter) e é reavaliada pelo `ResolvePendingReferencesUseCase` (worker de polling, `SKIP LOCKED`, seguro com várias instâncias).
- **Limite: 10 avaliações (janela de ~4 a 9 min).** Justificativa: entregas fora de ordem de provedores costumam se resolver em segundos. Uma reversão cuja referência não aparece em minutos indica erro do provedor, e quanto antes ele souber (`REJECTED / REFERENCE_NOT_FOUND` + `WagerTransactionRejected`), antes reconcilia. Os valores ficam em `PENDING_REFERENCE_POLICY`.
- **Otimização:** quando uma transação é finalizada, as pendências que a referenciam têm o vencimento antecipado para "agora" (wake). Assim a resolução acontece no próximo ciclo do worker, sem esperar o backoff.
- Um erro permanente ao reavaliar (ex.: constraint violada) marca a transação como `FAILED / PROCESSING_FAILED` em vez de reprocessar para sempre.

---

## 8. Reconciliação

`POST /wallets/:id/reconciliation` compara `wallets.balance` com `Σ(CREDIT) − Σ(DEBIT)` do ledger numa **única instrução SQL** (snapshot MVCC consistente). Uma divergência **não é corrigida**: é logada (`error`), contabilizada em `wagering_reconciliation_mismatches_total` e aparece na resposta (`consistent: false` e `difference`). Na prática o constraint trigger impede commits divergentes, então a reconciliação é uma auditoria independente (pega, por exemplo, triggers desabilitados ou restaurações parciais).

---

## 9. Observabilidade

- **Logs JSON** (pino) com `correlationId`, `messageId`, `sqsMessageId`, `transactionId`, `walletId` e `providerId`. Valores financeiros e payloads **não** são logados (há redaction de `payload`, `body`, `data`, `money`, `balance` e `amount` como defesa em profundidade).
- **Métricas** (`GET /metrics`, Prometheus):

| Métrica | Cobre |
|---|---|
| `wagering_transactions_total{status,kind,source}` | transações por status |
| `wagering_duplicates_total{kind}` | replays, conflitos de idempotência, duplicatas de inbox |
| `wagering_retries_total{operation}` | retries (concorrência, SQS, pendências) |
| `wagering_dlq_messages_total{reason}` | mensagens enviadas à DLQ |
| `wagering_lock_conflicts_total{reason}` | lock timeout, deadlock, versão, unique race |
| `wagering_outbox_lag_seconds`, `wagering_outbox_pending` | outbox lag |
| `wagering_processing_duration_seconds{source}` | latência |
| `wagering_reconciliation_mismatches_total` | divergências |

- **Health:** `/health/live` (processo) e `/health/ready` (`select 1` no PostgreSQL e `GetQueueAttributes` no SQS, com timeout de 2 s e `503` se algum falhar). Ambos são públicos.

---

## 10. Autenticação (não implementada)

Autenticação não pontua, então a decisão foi **não implementar** e manter o ponto de extensão explícito:

- `AuthGuard` global (`APP_GUARD`) com `ProviderIdentityPort` (`AnonymousProviderIdentity` no-op) e o decorator `@Public()` nos endpoints de health e métricas. Arquivo: `src/infrastructure/http/auth.ts`.

Desenho que eu adotaria:

1. **Keycloak** no compose, com um realm `wagering` e um client confidencial por provedor (fluxo `client_credentials`). O `providerId` fica num claim (mapper de atributo do client, ex.: `provider_id`).
2. O `AuthGuard` valida o JWT localmente via **JWKS** (cache de chaves; checa assinatura, `iss`, `aud`, `exp` e `nbf`), sem chamar o IdP a cada requisição. O `ProviderIdentityPort` extrai `providerId`.
3. O use case exige `command.providerId === identity.providerId`, senão `403`. A busca de referência já é restrita ao mesmo provider. Operações administrativas (criar wallet, reconciliação) exigem uma role própria.
4. Mensagens SQS são canal interno confiável (sem token), mas o `providerId` da mensagem continua sujeito às mesmas validações de domínio. Em produção, a autorização do produtor fica em IAM (políticas da fila).

---

## 11. Testes

| Exigência | Onde |
|---|---|
| Money (escala, entradas inválidas, sem arredondamento), Wallet, regras BET/WIN/LOSS/REFUND/ROLLBACK, conflito de moeda, hash divergente | `test/unit/*` (sem I/O) |
| Migrations (up/down/up) e constraints via SQL direto | `test/integration/schema.test.ts` |
| Atomicidade wallet + transação + ledger + outbox | `outbox-and-recovery.test.ts` (falha antes do commit não deixa nada) |
| Inbox e redelivery; retry e DLQ; SIGTERM | `sqs-consumer.test.ts` (LocalStack real, filas isoladas por teste) |
| Publishers concorrentes na mesma outbox; recuperação após reinício (outra instância publica) | `outbox-and-recovery.test.ts` |
| 50× a mesma aposta em paralelo | `wagering-flow.test.ts` (1 processo, pool 20) e `multi-instance.test.ts` (3 processos) |
| Cenário 100 − 80 − 80 | `wagering-flow.test.ts` e unit; generalizado para 30×10 sobre 100 em 3 instâncias |
| Wallets distintas em paralelo | `multi-instance.test.ts` |
| ≥ 3 processos simultâneos | `multi-instance.test.ts` (processos `bun src/main.ts` reais) |
| Worker morto depois do commit e antes do ack | `multi-instance.test.ts` (processo real sai com 137) e `sqs-consumer.test.ts` |
| ROLLBACK/REFUND antes da referência | `outbox-and-recovery.test.ts` (resolve; esgota → `REFERENCE_NOT_FOUND`; 3 workers concorrentes) |
| Reinício com consistência final | `multi-instance.test.ts` (kill -9 de uma de 3 instâncias no meio de 160 mensagens e restart) |
| **Invariante final** `wallet.balance == ledger` | `assertAllWalletsConsistent` no `afterAll` de cada suíte de integração e concorrência, para **todas** as wallets |

Nenhum teste substitui PostgreSQL ou SQS por mocks. Os stubs aparecem só para **injetar falha** (publisher que lança erro, use case que lança `TransientError`), sempre em volta de infraestrutura real. Os testes não dependem de sleeps para acertar a corrida: a concorrência é real (pool de conexões e processos), e as asserções verificam o estado final (`waitFor` com prazo).

Teste de carga: `bun run test:load` (metodologia e números em `docs/load-test-results.md`). Na máquina de desenvolvimento: ~800 req/s, p50 ≈ 35 ms, p99 ≈ 345 ms, 0% de erro, 0 conflitos de lock, hot wallet exatamente zerada e 101 wallets reconciliadas. **Leitura honesta:** o p95/p99 é dominado pela fila da hot wallet (20% do tráfego serializado numa linha, por desenho) e pelo fato de cliente, 3 instâncias, PostgreSQL e LocalStack dividirem a mesma máquina (Docker Desktop no Windows). A outbox com um único relay drena ~900 eventos/s; ela escala horizontalmente com mais instâncias `workers`.

---

## 12. Trade-offs e limitações

- **Hot wallet:** o lock pessimista serializa a wallet; a vazão de **uma** wallet fica limitada a ~1 / (duração da seção crítica). É o comportamento desejado para correção; `lock_timeout` impede filas infinitas.
- **Publicação dentro da transação da outbox:** o relay segura os locks das linhas reivindicadas enquanto chama o SQS (no máximo `batchSize` linhas). Se o SQS estiver lento, só aquele lote fica preso; os outros relays pegam outras linhas.
- **Ordem dos eventos entre relays** não é estrita (ver `walletVersion`).
- **Replay de `PENDING_REFERENCE`** devolve o estado **atual** da transação (pode já ser `PROCESSED`). Depois que a transação vira terminal, a resposta fica estável.
- **FAILED** é produzido apenas pelo worker de pendências. No fluxo síncrono, uma falha permanente faz rollback de tudo e devolve `500` (HTTP) ou DLQ (SQS), sem persistir a transação.
- **Single ledger** (não é partida dobrada). A extensão natural é uma conta de contrapartida por provedor/jogo; o `UNIQUE (transaction_id, wallet_id)` já permite dois lançamentos por transação.
- **`FAULT_INJECTION`** é um gancho de teste no consumidor. Em produção, não deve ser definido.
- **Inbox sem expurgo:** cresce indefinidamente. Em produção, um job de retenção removeria registros com mais de N dias (maior que a janela de redelivery do SQS).
- **Sem autenticação** (seção 10).
