# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) vindas de provedores via **HTTP** e **SQS**. Ele continua correto com mensagens duplicadas, fora de ordem, processadas em paralelo e com várias instâncias.

- Enunciado original: [CHALLENGE.md](CHALLENGE.md)
- Decisões, trade-offs e limitações: [ARCHITECTURE.md](ARCHITECTURE.md)
- Resultado do teste de carga: [docs/load-test-results.md](docs/load-test-results.md)

**Stack:** Bun 1.x · TypeScript strict · NestJS · PostgreSQL 16 · MikroORM 7 · AWS SQS via LocalStack · Docker Compose.

## Pré-requisitos

- [Bun](https://bun.sh) ≥ 1.4
- Docker com Docker Compose v2

## Subir tudo com Docker Compose

```bash
docker compose up -d --build --scale app=3
```

Esse comando sobe:

| Serviço | O que faz |
|---|---|
| `postgres` | PostgreSQL 16 em `localhost:5432` (`wagering`/`wagering`) |
| `localstack` | SQS em `localhost:4566`; cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` (redrive, `maxReceiveCount=5`) e `wagering-events.fifo` |
| `migrate` | aplica as migrations e sai |
| `app` ×3 | 3 réplicas com `ROLE=all` (HTTP + consumidor SQS + workers), publicadas em portas do intervalo `3000–3010` |

O Docker escolhe as portas das réplicas dentro do intervalo, sem ordem garantida. Para ver quais foram usadas:

```bash
docker compose ps
curl http://localhost:<porta>/health/ready   # {"status":"ready","checks":{"postgres":"up","sqs":"up"}}
```

As portas `5432` (PostgreSQL) e `4566` (LocalStack) precisam estar livres. Se você já tiver um PostgreSQL local rodando, pare-o antes.

## Rodar localmente (fora do Docker)

```bash
bun install
docker compose up -d postgres localstack
cp .env.example .env
bun run migration:up
bun run dev                  # ou: bun run start
```

`ROLE` define o que a instância executa: `api`, `consumer`, `workers`, combinações separadas por vírgula, ou `all` (padrão). Qualquer combinação pode ter várias instâncias.

## Comandos

| Comando | Descrição |
|---|---|
| `bun run dev` / `bun run start` | sobe a aplicação |
| `bun run migration:up` | aplica as migrations |
| `bun run migration:down` | reverte a última migration (`bun src/infrastructure/persistence/migrate.ts down --to=0` reverte todas) |
| `bun run typecheck` | `tsc --noEmit` em modo estrito |
| `bun test` | suíte completa (95 testes) |
| `bun run test:unit` | domínio puro (sem I/O) |
| `bun run test:integration` | PostgreSQL e LocalStack reais |
| `bun run test:concurrency` | várias instâncias reais (processos), kill -9, restart |
| `bun run test:load` | teste de carga; grava `docs/load-test-results.md` |

> Os testes de integração, concorrência e carga precisam de `docker compose up -d postgres localstack`. Cada suíte cria o próprio banco (`wagering_it_*`) e filas SQS isoladas, então não interferem no ambiente de desenvolvimento.

## API

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/wallets` | cria wallet (saldo inicial gera `OPENING` + `CREDIT`) |
| `GET` | `/wallets/:walletId` | consulta wallet |
| `GET` | `/wallets/:walletId/ledger?cursor=&limit=50` | ledger paginado (cursor opaco e estável) |
| `POST` | `/wallets/:walletId/reconciliation` | saldo armazenado × saldo reconstruído pelo ledger |
| `POST` | `/wagering/transactions` | submete transação (header `Idempotency-Key` obrigatório) |
| `GET` | `/wagering/transactions/:transactionId` | consulta por id interno |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | consulta por id do provedor |
| `GET` | `/health/live`, `/health/ready` | liveness e readiness (PostgreSQL + SQS) |
| `GET` | `/metrics` | métricas Prometheus |

Status HTTP de `POST /wagering/transactions`:

| Status | Significado | O provedor deve |
|---|---|---|
| `200` | `PROCESSED` | seguir |
| `202` | `PENDING_REFERENCE` (aceita, aguardando a referência) | consultar depois ou aguardar o evento |
| `422` | `REJECTED`, com `failureCode` | não reenviar o mesmo payload |
| `400` | payload inválido | corrigir o payload |
| `409` | `IDEMPOTENCY_CONFLICT` (mesma key, payload diferente) | não reenviar |
| `503` | falha transitória (`retryable: true`, `Retry-After`) | reenviar com backoff |

O replay de uma requisição idêntica devolve o mesmo corpo e o mesmo status, com `idempotentReplay: true`.

### Exemplo

```bash
curl -s -X POST localhost:3000/wallets -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'

curl -s -X POST localhost:3000/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123",
       "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<id>",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
       "money":{"amount":"25.00","currency":"BRL"}}'
```

### Enviar pela fila

```bash
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id <walletId> --message-deduplication-id <qualquer> \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-07-29T15:00:00.000Z",
    "data":{"providerId":"provider-a","externalTransactionId":"transaction-124","idempotencyKey":"provider-a:transaction-124",
    "playerId":"...","walletId":"...","roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
    "money":{"amount":"25.00","currency":"BRL"}}}'
```

Os eventos de integração publicados pela outbox podem ser lidos em `wagering-events.fifo`.

## Estrutura

```
src/
  shared/domain/      Money, erros de domínio, backoff, hash canônico, ids (UUIDv7)
  wallet/domain/      Wallet (aggregate root), WalletLedgerEntry
  wagering/domain/    WagerTransaction (máquina de estados), FailureCode, decideWager (regras)
  messaging/domain/   IntegrationEvent + eventos, InboxMessage, OutboxMessage
  application/        use cases, portas, erros de aplicação, contrato de entrada
  infrastructure/
    persistence/      MikroORM (entidades, mappers, adapters), migrations SQL
    http/             Nest (controllers, filtro de erros, AuthGuard no-op)
    sqs/              consumidor, publisher de eventos, config
    workers/          outbox relay, polling worker
    observability/    logger JSON (pino), métricas (prom-client)
  bootstrap/          raiz de composição (independente do Nest)
  main.ts
test/
  unit/ integration/ concurrency/ load/ support/
```
