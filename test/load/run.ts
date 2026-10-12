/**
 * Teste de carga: `bun run test:load` (requer `docker compose up -d postgres localstack`).
 *
 * Metodologia: banco novo, 3 instâncias da aplicação (1 com workers de outbox), carga HTTP em
 * loop fechado com concorrência fixa. Mix: 70% BET/WIN/LOSS em wallets distintas, 20% BET em
 * uma hot wallet (contenção máxima), 10% replays idempotentes. Ao final: latência p50/p95/p99,
 * throughput, taxa de erro, conflitos de lock/retries (via /metrics), lag da outbox e reconciliação.
 */
import os from 'node:os';
import { randomUUID } from 'node:crypto';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { recreateDatabase } from '../support/env';
import { assertAllWalletsConsistent, sql, uniq, wagerBody } from '../support/fixtures';
import { type AppProcess, postJson, spawnApp, stopApp } from '../support/processes';
import { createTestQueues, testSqsClient } from '../support/sqs';

const TOTAL = Number(process.env.LOAD_REQUESTS ?? 6000);
const CONCURRENCY = Number(process.env.LOAD_CONCURRENCY ?? 48);
const WALLETS = Number(process.env.LOAD_WALLETS ?? 100);

const percentile = (sorted: number[], p: number) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;

const scrape = async (app: AppProcess, metric: string): Promise<number> => {
  const text = await (await fetch(`${app.baseUrl}/metrics`)).text();
  return text
    .split('\n')
    .filter((l) => l.startsWith(metric) && !l.startsWith('#'))
    .reduce((acc, l) => acc + Number(l.split(' ').pop()), 0);
};

const main = async () => {
  const url = await recreateDatabase('wagering_load');
  await runMigrations('up', url);
  const queues = await createTestQueues(testSqsClient());
  const apps = await Promise.all([
    spawnApp({ port: 4201, databaseUrl: url, role: 'api,workers', queues }),
    spawnApp({ port: 4202, databaseUrl: url, role: 'api', queues }),
    spawnApp({ port: 4203, databaseUrl: url, role: 'api', queues }),
  ]);

  try {
    const wallets = await Promise.all(
      Array.from({ length: WALLETS }, async (_, i) => {
        const playerId = randomUUID();
        const r = await postJson(apps[i % 3]!.baseUrl, '/wallets', { playerId, initialBalance: { amount: '1000000.00', currency: 'BRL' } });
        return { id: r.body.id as string, playerId };
      }),
    );
    const hotPlayer = randomUUID();
    const hotRes = await postJson(apps[0]!.baseUrl, '/wallets', { playerId: hotPlayer, initialBalance: { amount: '500.00', currency: 'BRL' } });
    const hot = { id: hotRes.body.id as string, playerId: hotPlayer };

    const latencies: number[] = [];
    const statuses: Record<string, number> = {};
    const sentBodies: ReturnType<typeof wagerBody>[] = [];
    let next = 0;

    const one = async (i: number) => {
      const r = Math.random();
      let body: ReturnType<typeof wagerBody>;
      if (r < 0.1 && sentBodies.length > 0) {
        body = sentBodies[Math.floor(Math.random() * sentBodies.length)]!; // replay
      } else if (r < 0.3) {
        body = wagerBody({ walletId: hot.id, playerId: hot.playerId, kind: 'BET', amount: '1.00', externalTransactionId: uniq('h') });
      } else {
        const w = wallets[Math.floor(Math.random() * wallets.length)]!;
        const kind = (['BET', 'BET', 'WIN', 'LOSS'] as const)[i % 4]!;
        body = wagerBody({ walletId: w.id, playerId: w.playerId, kind, amount: kind === 'LOSS' ? '0.00' : '2.50', externalTransactionId: uniq('l') });
      }
      const started = performance.now();
      try {
        const res = await postJson(apps[i % 3]!.baseUrl, '/wagering/transactions', body, {
          'Idempotency-Key': `${body.providerId}:${body.externalTransactionId}`,
        });
        const key = res.body.idempotentReplay ? `${res.status}-replay` : String(res.status);
        statuses[key] = (statuses[key] ?? 0) + 1;
        if (res.status === 200 && sentBodies.length < 2000) sentBodies.push(body);
      } catch {
        statuses.network_error = (statuses.network_error ?? 0) + 1;
      }
      latencies.push(performance.now() - started);
    };

    const started = performance.now();
    await Promise.all(
      Array.from({ length: CONCURRENCY }, async () => {
        while (next < TOTAL) await one(next++);
      }),
    );
    const elapsedS = (performance.now() - started) / 1000;

    // Outbox: tempo até drenar após o fim da carga + gauge de lag observado.
    const lagAtEnd = await scrape(apps[0]!, 'wagering_outbox_lag_seconds');
    const drainStart = performance.now();
    for (;;) {
      const [row] = await sql<{ n: string }>(url, `select count(*)::text as n from outbox_messages where published_at is null`);
      if (row!.n === '0') break;
      await Bun.sleep(200);
    }
    const drainS = (performance.now() - drainStart) / 1000;

    const sum = async (metric: string) => (await Promise.all(apps.map((a) => scrape(a, metric)))).reduce((a, b) => a + b, 0);
    const lockConflicts = await sum('wagering_lock_conflicts_total');
    const retries = await sum('wagering_retries_total');
    const walletsChecked = await assertAllWalletsConsistent(url);
    const [hotRow] = await sql(url, `select balance::text from wallets where id = $1`, [hot.id]);
    const [events] = await sql<{ n: string }>(url, `select count(*)::text as n from outbox_messages`);

    const sorted = [...latencies].sort((a, b) => a - b);
    const errors = Object.entries(statuses)
      .filter(([k]) => k.startsWith('5') || k === 'network_error')
      .reduce((a, [, v]) => a + v, 0);

    const report = `# Load test — ${new Date().toISOString()}

## Ambiente
- Máquina: ${os.cpus()[0]?.model ?? 'n/a'} × ${os.cpus().length} vCPU, ${(os.totalmem() / 2 ** 30).toFixed(1)} GiB RAM, ${os.platform()} ${os.release()}
- Bun ${Bun.version}; PostgreSQL 16 e LocalStack em Docker Desktop (mesma máquina)
- 3 instâncias da aplicação (processos), pool de 10 conexões cada; 1 delas com workers (outbox/pending)

## Metodologia
- ${TOTAL} requisições POST /wagering/transactions, loop fechado com concorrência ${CONCURRENCY}, round-robin entre instâncias
- Mix: ~70% BET/WIN/LOSS em ${WALLETS} wallets, ~20% BET em 1 hot wallet (saldo 500.00, apostas de 1.00), ~10% replays idempotentes
- Medição no cliente (inclui HTTP local); conflitos e retries extraídos de /metrics das 3 instâncias

## Resultados
| Métrica | Valor |
|---|---|
| Duração | ${elapsedS.toFixed(2)} s |
| Throughput | ${(TOTAL / elapsedS).toFixed(0)} req/s |
| Latência p50 / p95 / p99 | ${percentile(sorted, 50).toFixed(1)} / ${percentile(sorted, 95).toFixed(1)} / ${percentile(sorted, 99).toFixed(1)} ms |
| Latência máx | ${sorted[sorted.length - 1]?.toFixed(1)} ms |
| Respostas por status | ${Object.entries(statuses).sort().map(([k, v]) => `${k}: ${v}`).join(', ')} |
| Taxa de erro (5xx + rede) | ${((errors / TOTAL) * 100).toFixed(2)}% |
| Conflitos de concorrência (lock_conflicts_total) | ${lockConflicts} |
| Retries internos | ${retries} |
| Lag da outbox ao fim da carga | ${lagAtEnd.toFixed(2)} s |
| Tempo para drenar a outbox após a carga | ${drainS.toFixed(2)} s (${events!.n} eventos) |
| Saldo final da hot wallet | ${hotRow!.balance} BRL |
| Reconciliação | ${walletsChecked} wallets consistentes (saldo == ledger) |
`;
    console.log(report);
    await Bun.write('docs/load-test-results.md', report);
  } finally {
    await Promise.all(apps.map((a) => stopApp(a, 'SIGKILL')));
  }
};

main().then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
