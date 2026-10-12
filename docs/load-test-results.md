# Load test — 2026-10-10T19:33:54.330Z

## Ambiente
- Máquina: AMD Ryzen 7 9800X3D 8-Core Processor            × 16 vCPU, 30.9 GiB RAM, win32 10.0.26200
- Bun 1.4.3; PostgreSQL 16 e LocalStack em Docker Desktop (mesma máquina)
- 3 instâncias da aplicação (processos), pool de 10 conexões cada; 1 delas com workers (outbox/pending)

## Metodologia
- 6000 requisições POST /wagering/transactions, loop fechado com concorrência 48, round-robin entre instâncias
- Mix: ~70% BET/WIN/LOSS em 100 wallets, ~20% BET em 1 hot wallet (saldo 500.00, apostas de 1.00), ~10% replays idempotentes
- Medição no cliente (inclui HTTP local); conflitos e retries extraídos de /metrics das 3 instâncias

## Resultados
| Métrica | Valor |
|---|---|
| Duração | 7.52 s |
| Throughput | 798 req/s |
| Latência p50 / p95 / p99 | 34.9 / 178.6 / 344.8 ms |
| Latência máx | 496.9 ms |
| Respostas por status | 200: 4656, 200-replay: 572, 422: 772 |
| Taxa de erro (5xx + rede) | 0.00% |
| Conflitos de concorrência (lock_conflicts_total) | 0 |
| Retries internos | 0 |
| Lag da outbox ao fim da carga | 4.37 s |
| Tempo para drenar a outbox após a carga | 10.27 s (9225 eventos) |
| Saldo final da hot wallet | 0.00 BRL |
| Reconciliação | 101 wallets consistentes (saldo == ledger) |
