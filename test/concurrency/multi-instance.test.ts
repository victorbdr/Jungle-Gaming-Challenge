import type { SQSClient } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { recreateDatabase } from '../support/env';
import { assertAllWalletsConsistent, sql, uniq, wagerBody } from '../support/fixtures';
import { type AppProcess, postJson, spawnApp, stopApp } from '../support/processes';
import { createTestQueues, queueDepth, sendWagerMessage, testSqsClient, waitFor } from '../support/sqs';

/**
 * Paralelismo real: várias instâncias da aplicação (processos separados), cada uma com seu
 * pool de conexões, disputando as mesmas wallets no mesmo PostgreSQL.
 */
let url: string;
let sqs: SQSClient;
const apps: AppProcess[] = [];

beforeAll(async () => {
  url = await recreateDatabase('wagering_cc_multi');
  await runMigrations('up', url);
  sqs = testSqsClient();
});

afterAll(async () => {
  await Promise.all(apps.map((a) => stopApp(a, 'SIGKILL')));
  await assertAllWalletsConsistent(url);
});

const createWalletVia = async (app: AppProcess, amount: string) => {
  const playerId = randomUUID();
  const r = await postJson(app.baseUrl, '/wallets', { playerId, initialBalance: { amount, currency: 'BRL' } });
  expect(r.status).toBe(201);
  return { id: r.body.id as string, playerId };
};

const submitVia = (app: AppProcess, body: ReturnType<typeof wagerBody>) =>
  postJson(app.baseUrl, '/wagering/transactions', body, { 'Idempotency-Key': `${body.providerId}:${body.externalTransactionId}` });

describe('3 instâncias via HTTP', () => {
  beforeAll(async () => {
    apps.push(...(await Promise.all([4101, 4102, 4103].map((port) => spawnApp({ port, databaseUrl: url, role: 'api' })))));
    // Subir 3 processos pode passar dos 5 s padrão quando a máquina está ocupada (suíte completa, Docker).
  }, 60_000);

  test('hot wallet: 30 apostas de 10.00 sobre saldo 100.00 distribuídas entre as instâncias', async () => {
    const w = await createWalletVia(apps[0]!, '100.00');
    const responses = await Promise.all(
      Array.from({ length: 30 }, (_, i) => submitVia(apps[i % 3]!, wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' }))),
    );
    const byStatus = responses.reduce<Record<number, number>>((acc, r) => ({ ...acc, [r.status]: (acc[r.status] ?? 0) + 1 }), {});
    expect(byStatus).toEqual({ 200: 10, 422: 20 });
    expect(responses.filter((r) => r.status === 422).every((r) => r.body.failureCode === 'INSUFFICIENT_FUNDS')).toBe(true);
    const [wallet] = await sql(url, `select balance::text, version from wallets where id = $1`, [w.id]);
    expect(wallet).toEqual({ balance: '0.00', version: 11 });
    const debits = await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [w.id]);
    expect(debits).toHaveLength(10);
  });

  test('a mesma aposta 50x em paralelo nas 3 instâncias → um único débito', async () => {
    const w = await createWalletVia(apps[1]!, '100.00');
    const body = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '7.00' });
    const responses = await Promise.all(Array.from({ length: 50 }, (_, i) => submitVia(apps[i % 3]!, body)));
    expect(new Set(responses.map((r) => r.status))).toEqual(new Set([200]));
    expect(new Set(responses.map((r) => r.body.transactionId)).size).toBe(1);
    expect(responses.filter((r) => r.body.idempotentReplay === false)).toHaveLength(1);
    expect(new Set(responses.map((r) => JSON.stringify(r.body.balance)))).toEqual(new Set([JSON.stringify({ amount: '93.00', currency: 'BRL' })]));
    const debits = await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [w.id]);
    expect(debits).toHaveLength(1);
  });

  test('wallets distintas processadas em paralelo, com mix de operações', async () => {
    const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => createWalletVia(apps[i % 3]!, '50.00')));
    const ops = wallets.flatMap((w, wi) =>
      Array.from({ length: 10 }, (_, i) => {
        const kind = (['BET', 'WIN', 'LOSS'] as const)[i % 3]!;
        return { app: apps[(wi + i) % 3]!, body: wagerBody({ walletId: w.id, playerId: w.playerId, kind, amount: kind === 'LOSS' ? '0.00' : '3.00' }) };
      }),
    );
    const started = performance.now();
    const responses = await Promise.all(ops.map((o) => submitVia(o.app, o.body)));
    const elapsed = performance.now() - started;
    expect(responses.every((r) => r.status === 200)).toBe(true);
    for (const w of wallets) {
      const [row] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
      // 4 BET (−12.00) + 3 WIN (+9.00) + 3 LOSS (0) ⇒ 47.00, em qualquer ordem (o saldo nunca fica abaixo de 12)
      expect(row!.balance).toBe('47.00');
    }
    console.log(`100 operações em 10 wallets via 3 instâncias: ${elapsed.toFixed(0)}ms`);
  });
});

describe('falhas de processo', () => {
  test('consumer morto depois do commit e antes do ack: outra instância reconhece a duplicata', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWalletVia(apps[0]!, '100.00');
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '25.00' });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);

    const doomed = await spawnApp({
      port: 4111,
      databaseUrl: url,
      role: 'consumer',
      queues: q,
      env: { FAULT_INJECTION: 'crash_before_ack', SQS_VISIBILITY_TIMEOUT: '2' },
      waitHealthy: false,
    });
    await Promise.race([doomed.proc.exited, Bun.sleep(15_000)]);
    expect(doomed.proc.exitCode).toBe(137);
    // Commit aconteceu: débito aplicado, mas a mensagem continua na fila (sem ack).
    expect(await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [w.id])).toHaveLength(1);

    const healthy = await spawnApp({ port: 4112, databaseUrl: url, role: 'consumer', queues: q, env: { SQS_VISIBILITY_TIMEOUT: '2' } });
    apps.push(healthy);
    await waitFor(async () => {
      const d = await queueDepth(sqs, q.wagerQueueUrl);
      return d.visible === 0 && d.inFlight === 0;
    }, 20_000);
    expect(await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [w.id])).toHaveLength(1);
    const [wallet] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
    expect(wallet!.balance).toBe('75.00');
    await stopApp(healthy, 'SIGKILL');
  }, 60_000);

  test('reinício do serviço no meio da carga: consistência final comprovada', async () => {
    const q = await createTestQueues(sqs, 5);
    const wallets = await Promise.all(Array.from({ length: 8 }, () => createWalletVia(apps[0]!, '200.00')));
    const fleet = await Promise.all(
      [4121, 4122, 4123].map((port) => spawnApp({ port, databaseUrl: url, role: 'all', queues: q, env: { SQS_VISIBILITY_TIMEOUT: '3' } })),
    );
    apps.push(...fleet);

    const sent: { walletId: string; ext: string }[] = [];
    for (let i = 0; i < 160; i++) {
      const w = wallets[i % wallets.length]!;
      const ext = uniq('r');
      const kind = i % 4 === 3 ? 'WIN' : 'BET';
      await sendWagerMessage(sqs, q.wagerQueueUrl, wagerBody({ walletId: w.id, playerId: w.playerId, kind, amount: '5.00', externalTransactionId: ext }));
      sent.push({ walletId: w.id, ext });
      if (i === 60) {
        // Mata uma instância abruptamente no meio do processamento e sobe outra no lugar.
        await stopApp(fleet[1]!, 'SIGKILL');
        apps.push(await spawnApp({ port: 4124, databaseUrl: url, role: 'all', queues: q, env: { SQS_VISIBILITY_TIMEOUT: '3' } }));
      }
    }

    await waitFor(async () => {
      const [row] = await sql<{ n: string }>(
        url,
        `select count(*)::text as n from wager_transactions where external_transaction_id = any($1) and status in ('PROCESSED','REJECTED')`,
        [sent.map((s) => s.ext)],
      );
      return Number(row!.n) === sent.length;
    }, 60_000, 500);
    await waitFor(async () => {
      const [row] = await sql<{ n: string }>(url, `select count(*)::text as n from outbox_messages where published_at is null`);
      return row!.n === '0';
    }, 30_000, 500);

    // Cada mensagem gerou exatamente uma transação e, se PROCESSED, exatamente um lançamento.
    const [dups] = await sql<{ n: string }>(
      url,
      `select count(*)::text as n from (select transaction_id from wallet_ledger_entries group by transaction_id having count(*) > 1) d`,
    );
    expect(dups!.n).toBe('0');
    const [orphans] = await sql<{ n: string }>(
      url,
      `select count(*)::text as n from wager_transactions t
       where t.status = 'PROCESSED' and t.kind <> 'LOSS'
         and not exists (select 1 from wallet_ledger_entries l where l.transaction_id = t.id)`,
    );
    expect(orphans!.n).toBe('0');
    expect(await assertAllWalletsConsistent(url)).toBeGreaterThan(0);
    expect((await queueDepth(sqs, q.dlqUrl)).visible).toBe(0);
  }, 120_000);
});
