import type { SQSClient } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { EventPublisher } from '../../src/application/ports';
import { createContainer, type Container } from '../../src/bootstrap/container';
import { WagerTransactionProcessed } from '../../src/messaging/domain/events';
import type { OutboxMessage } from '../../src/messaging/domain/outbox-message';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { SqsEventPublisher } from '../../src/infrastructure/sqs/sqs-event-publisher';
import { OutboxRelay } from '../../src/infrastructure/workers/outbox-relay';
import { newId } from '../../src/shared/domain/ids';
import { Money } from '../../src/shared/domain/money';
import { PENDING_REFERENCE_POLICY } from '../../src/shared/domain/backoff';
import { WagerTransaction } from '../../src/wagering/domain/wager-transaction';
import { Wallet } from '../../src/wallet/domain/wallet';
import { recreateDatabase } from '../support/env';
import { assertAllWalletsConsistent, createWallet, sql, submit, uniq } from '../support/fixtures';
import { silentLogger } from '../support/logger';
import { createTestQueues, drainQueue, testSqsClient } from '../support/sqs';

let url: string;
let c: Container;
let sqs: SQSClient;

beforeAll(async () => {
  url = await recreateDatabase('wagering_it_outbox');
  await runMigrations('up', url);
  c = await createContainer({ databaseUrl: url, logger: silentLogger() });
  sqs = testSqsClient();
});
afterAll(async () => {
  await assertAllWalletsConsistent(url);
  await c.close();
});

const pendingOutbox = async () => Number((await sql<{ n: string }>(url, `select count(*)::text as n from outbox_messages where published_at is null`))[0]!.n);

/** Publisher que registra cada publicação e simula latência de rede. */
class RecordingPublisher implements EventPublisher {
  readonly published: string[] = [];
  constructor(private readonly delayMs = 5) {}
  async publishBatch(batch: readonly OutboxMessage[]): Promise<Map<string, string>> {
    await Bun.sleep(this.delayMs);
    this.published.push(...batch.map((m) => m.id));
    return new Map();
  }
}

describe('atomicidade', () => {
  test('falha antes do commit não deixa wallet, transação, ledger nem outbox', async () => {
    const walletId = newId();
    const before = await sql<{ n: string }>(url, `select (select count(*) from outbox_messages) + (select count(*) from wallet_ledger_entries) as n`);
    await expect(
      c.runner.run(async (uow) => {
        const now = new Date();
        const txId = newId();
        const { wallet, openingEntry } = Wallet.open({
          id: walletId,
          playerId: uniq('p'),
          initialBalance: Money.from({ amount: '50.00', currency: 'BRL' }),
          at: now,
          openingTransactionId: txId,
        });
        const opening = WagerTransaction.opening({
          id: txId,
          providerId: 'internal',
          externalTransactionId: `opening:${walletId}`,
          idempotencyKey: `internal:opening:${walletId}`,
          payloadHash: 'a'.repeat(64),
          walletId,
          playerId: wallet.playerId,
          roundId: 'opening',
          gameId: 'internal',
          money: Money.from({ amount: '50.00', currency: 'BRL' }),
          createdAt: now,
        });
        opening.markProcessed(undefined, now, wallet.balance);
        await uow.insertWallet(wallet);
        await uow.insertTransaction(opening);
        await uow.insertLedgerEntry(openingEntry!);
        await uow.enqueue([WagerTransactionProcessed.from(opening, { correlationId: 'x', occurredAt: now })]);
        throw new Error('crash before commit');
      }),
    ).rejects.toThrow('crash before commit');
    expect(await sql(url, `select 1 from wallets where id = $1`, [walletId])).toHaveLength(0);
    expect(await sql(url, `select 1 from wager_transactions where wallet_id = $1`, [walletId])).toHaveLength(0);
    const after = await sql<{ n: string }>(url, `select (select count(*) from outbox_messages) + (select count(*) from wallet_ledger_entries) as n`);
    expect(after[0]!.n).toBe(before[0]!.n);
  });
});

describe('outbox', () => {
  test('dois publishers concorrentes: cada evento publicado exatamente uma vez nesta rodada', async () => {
    for (let i = 0; i < 20; i++) {
      const w = await createWallet(c, '100.00');
      await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '1.00' });
    }
    const total = await pendingOutbox();
    expect(total).toBeGreaterThanOrEqual(80);

    const p1 = new RecordingPublisher();
    const p2 = new RecordingPublisher();
    const relay1 = new OutboxRelay(c.outboxStore, p1, c.clock, c.metrics, silentLogger(), 10);
    const relay2 = new OutboxRelay(c.outboxStore, p2, c.clock, c.metrics, silentLogger(), 10);
    await Promise.all([relay1.drain(), relay2.drain()]);

    const all = [...p1.published, ...p2.published];
    expect(new Set(all).size).toBe(all.length); // nenhuma duplicata entre publishers
    expect(all.length).toBe(total);
    expect(p1.published.length).toBeGreaterThan(0);
    expect(p2.published.length).toBeGreaterThan(0);
    expect(await pendingOutbox()).toBe(0);
  });

  test('falha de publicação agenda retry com backoff sem perder o evento', async () => {
    const w = await createWallet(c, '10.00');
    const failing: EventPublisher = {
      publishBatch: async () => {
        throw new Error('sqs unavailable');
      },
    };
    const relay = new OutboxRelay(c.outboxStore, failing, c.clock, c.metrics, silentLogger());
    const r = await relay.runOnce();
    expect(r.failed).toBeGreaterThan(0);
    const rows = await sql<{ attempts: number; last_error: string; due_in_future: boolean }>(
      url,
      `select attempts, last_error, next_attempt_at > now() as due_in_future from outbox_messages where aggregate_id = $1`,
      [w.id],
    );
    expect(rows[0]).toEqual({ attempts: 1, last_error: 'sqs unavailable', due_in_future: true });
    // Recuperação: força o vencimento e publica com um publisher saudável.
    await sql(url, `update outbox_messages set next_attempt_at = now() where published_at is null`);
    const ok = new RecordingPublisher(0);
    await new OutboxRelay(c.outboxStore, ok, c.clock, c.metrics, silentLogger()).drain();
    expect(await pendingOutbox()).toBe(0);
  });

  test('commit confirmado, processo morre antes de publicar: outra instância publica no SQS real', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '100.00'); // eventos ficam pendentes: nenhum relay rodou ("processo morreu")
    await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '5.00' });
    const expected = await pendingOutbox();
    expect(expected).toBe(4);

    // "Outra instância": container novo, mesma base.
    const other = await createContainer({ databaseUrl: url, logger: silentLogger() });
    try {
      const relay = new OutboxRelay(other.outboxStore, new SqsEventPublisher(sqs, q.eventsQueue), other.clock, other.metrics, silentLogger());
      await relay.drain();
      // Publicação duplicada (ex.: crash após publicar e antes de marcar): o broker deduplica por eventId,
      // e o envelope carrega eventId para dedupe no consumidor final.
      await sql(url, `update outbox_messages set published_at = null, next_attempt_at = now() where aggregate_id = $1`, [w.id]);
      await relay.drain();
    } finally {
      await other.close();
    }
    const messages = await drainQueue(sqs, q.eventsUrl, expected, 5_000);
    const envelopes = messages.map((m) => JSON.parse(m.body) as { eventId: string; eventType: string; data: Record<string, unknown> });
    expect(new Set(envelopes.map((e) => e.eventId)).size).toBe(envelopes.length);
    expect(envelopes.map((e) => e.eventType).sort()).toEqual([
      'WagerTransactionProcessed',
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
      'WalletBalanceChanged',
    ]);
    const balanceChanged = envelopes.find((e) => e.eventType === 'WalletBalanceChanged' && (e.data.direction as string) === 'DEBIT');
    expect(balanceChanged?.data.balanceAfter).toEqual({ amount: '95.00', currency: 'BRL' });
  });
});

describe('referências fora de ordem', () => {
  test('ROLLBACK antes da BET: PENDING_REFERENCE → resolvido pelo worker quando a BET chega', async () => {
    const w = await createWallet(c, '100.00');
    const betExt = uniq('bet');
    const rb = await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: betExt });
    if (rb.type !== 'result') throw new Error('unexpected');
    expect(rb.transaction.status).toBe('PENDING_REFERENCE' as never);

    await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '40.00', externalTransactionId: betExt });
    // A chegada da BET antecipa o vencimento da pendência ("wake"); o worker resolve na próxima rodada.
    const stats = await c.resolvePendingReferences.runOnce();
    expect(stats.resolved).toBe(1);

    const [tx] = await sql(url, `select status, reference_transaction_id is not null as linked from wager_transactions where id = $1`, [rb.transaction.id]);
    expect(tx).toEqual({ status: 'PROCESSED', linked: true });
    const [wallet] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
    expect(wallet!.balance).toBe('100.00');
    const events = await sql(url, `select event_type from outbox_messages where aggregate_id = $1 order by occurred_at`, [rb.transaction.id]);
    expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionPendingReference', 'WagerTransactionProcessed']);
  });

  test('REFUND cuja referência nunca chega: REJECTED REFERENCE_NOT_FOUND após o limite, com evento', async () => {
    const w = await createWallet(c, '100.00');
    const r = await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'REFUND', amount: '10.00', referenceExternalTransactionId: uniq('ghost') });
    if (r.type !== 'result') throw new Error('unexpected');
    for (let i = 0; i < PENDING_REFERENCE_POLICY.maxAttempts + 2; i++) {
      await sql(url, `update wager_transactions set next_reference_attempt_at = now() - interval '1 second' where id = $1 and status = 'PENDING_REFERENCE'`, [
        r.transaction.id,
      ]);
      await c.resolvePendingReferences.runOnce();
    }
    const [tx] = await sql(url, `select status, failure_code, reference_attempts from wager_transactions where id = $1`, [r.transaction.id]);
    expect(tx).toEqual({ status: 'REJECTED', failure_code: 'REFERENCE_NOT_FOUND', reference_attempts: PENDING_REFERENCE_POLICY.maxAttempts - 1 });
    const events = await sql(url, `select event_type from outbox_messages where aggregate_id = $1`, [r.transaction.id]);
    expect(events.map((e) => e.event_type).sort()).toEqual(['WagerTransactionPendingReference', 'WagerTransactionRejected']);
    const [wallet] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
    expect(wallet!.balance).toBe('100.00');
  });

  test('vários workers concorrentes resolvem cada pendência uma única vez', async () => {
    const w = await createWallet(c, '100.00');
    const bets = Array.from({ length: 10 }, () => uniq('bet'));
    for (const ext of bets) {
      await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'ROLLBACK', amount: '1.00', referenceExternalTransactionId: ext });
    }
    for (const ext of bets) {
      await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '1.00', externalTransactionId: ext });
    }
    const results = await Promise.all([1, 2, 3].map(() => c.resolvePendingReferences.runOnce()));
    expect(results.reduce((acc, r) => acc + r.resolved, 0)).toBe(10);
    const [wallet] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
    expect(wallet!.balance).toBe('100.00');
  });
});
