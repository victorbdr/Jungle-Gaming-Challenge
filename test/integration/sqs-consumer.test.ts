import { ReceiveMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { TransientError } from '../../src/application/errors';
import type { ProcessWagerTransactionUseCase } from '../../src/application/process-wager.use-case';
import { toWagerCommand } from '../../src/application/wager-command';
import { createContainer, type Container } from '../../src/bootstrap/container';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { WAGER_CONSUMER_NAME, WagerSqsConsumer } from '../../src/infrastructure/sqs/wager-consumer';
import { sha256Canonical } from '../../src/shared/domain/canonical-hash';
import { recreateDatabase } from '../support/env';
import { assertAllWalletsConsistent, createWallet, sql, uniq, wagerBody } from '../support/fixtures';
import { silentLogger } from '../support/logger';
import { createTestQueues, drainQueue, queueDepth, sendWagerMessage, testSqsClient, waitFor } from '../support/sqs';

let url: string;
let c: Container;
let sqs: SQSClient;

beforeAll(async () => {
  url = await recreateDatabase('wagering_it_sqs');
  await runMigrations('up', url);
  c = await createContainer({ databaseUrl: url, logger: silentLogger() });
  sqs = testSqsClient();
});
afterAll(async () => {
  await assertAllWalletsConsistent(url);
  await c.close();
});

const consumerFor = (queues: Awaited<ReturnType<typeof createTestQueues>>, useCase: ProcessWagerTransactionUseCase = c.processWager) =>
  new WagerSqsConsumer(sqs, queues, useCase, c.metrics, silentLogger(), { waitTimeSeconds: 1 });

const debitsOf = async (walletId: string) =>
  (await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [walletId])).length;

describe('SQS consumer', () => {
  test('redelivery da mesma mensagem não duplica efeito (inbox)', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '100.00');
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' });
    const messageId = await sendWagerMessage(sqs, q.wagerQueueUrl, data);
    await sendWagerMessage(sqs, q.wagerQueueUrl, data, { messageId });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data, { messageId });

    const consumer = consumerFor(q);
    consumer.start();
    await waitFor(async () => {
      const d = await queueDepth(sqs, q.wagerQueueUrl);
      return d.visible === 0 && d.inFlight === 0;
    });
    await consumer.stop();

    expect(await debitsOf(w.id)).toBe(1);
    const inbox = await sql(url, `select message_id from inbox_messages where consumer_name = $1 and message_id = $2`, [WAGER_CONSUMER_NAME, messageId]);
    expect(inbox).toHaveLength(1);
    expect((await queueDepth(sqs, q.dlqUrl)).visible).toBe(0);
  });

  test('processo morre depois do commit e antes do ack: redelivery é reconhecida e confirmada', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '100.00');
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '30.00' });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);

    // "Worker A": recebe, commita, e morre sem DeleteMessage (visibilidade curta para simular o timeout).
    const received = await sqs.send(new ReceiveMessageCommand({ QueueUrl: q.wagerQueueUrl, MaxNumberOfMessages: 1, VisibilityTimeout: 1, WaitTimeSeconds: 2 }));
    const msg = received.Messages![0]!;
    const envelope = JSON.parse(msg.Body!);
    const { idempotencyKey, ...payload } = envelope.data;
    await c.processWager.execute(toWagerCommand(payload, idempotencyKey), {
      correlationId: envelope.messageId,
      source: 'sqs',
      inbox: { consumerName: WAGER_CONSUMER_NAME, messageId: envelope.messageId, payloadHash: sha256Canonical(envelope) },
    });
    expect(await debitsOf(w.id)).toBe(1);

    // "Worker B": recebe a mesma mensagem após o timeout de visibilidade.
    await Bun.sleep(1200);
    const consumer = consumerFor(q);
    consumer.start();
    await waitFor(async () => {
      const d = await queueDepth(sqs, q.wagerQueueUrl);
      return d.visible === 0 && d.inFlight === 0;
    });
    await consumer.stop();
    expect(await debitsOf(w.id)).toBe(1);
    const [wallet] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
    expect(wallet!.balance).toBe('70.00');
  });

  test('rejeição de negócio é terminal: ack, sem DLQ', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '5.00');
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);
    const consumer = consumerFor(q);
    consumer.start();
    await waitFor(async () => (await sql(url, `select 1 from wager_transactions where external_transaction_id = $1`, [data.externalTransactionId])).length === 1);
    await waitFor(async () => {
      const d = await queueDepth(sqs, q.wagerQueueUrl);
      return d.visible === 0 && d.inFlight === 0;
    });
    await consumer.stop();
    const [tx] = await sql(url, `select status, failure_code from wager_transactions where external_transaction_id = $1`, [data.externalTransactionId]);
    expect(tx).toEqual({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
    expect((await queueDepth(sqs, q.dlqUrl)).visible).toBe(0);
  });

  test('payload inválido / malformado vai direto para a DLQ', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '5.00');
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '1.001' });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);
    await sendWagerMessage(sqs, q.wagerQueueUrl, data, { rawBody: '{not json' });
    const consumer = consumerFor(q);
    consumer.start();
    const dlq = await drainQueue(sqs, q.dlqUrl, 2);
    await consumer.stop();
    expect(dlq.map((m) => m.attributes.dlqReason).sort()).toEqual(['invalid_payload', 'malformed_json']);
  });

  test('erro transitório: retry com backoff e DLQ ao esgotar tentativas', async () => {
    const q = await createTestQueues(sqs, 2);
    const w = await createWallet(c, '5.00');
    let calls = 0;
    const failing = {
      execute: async () => {
        calls++;
        throw new TransientError('db down', 'connection');
      },
    } as unknown as ProcessWagerTransactionUseCase;
    await sendWagerMessage(sqs, q.wagerQueueUrl, wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '1.00' }));
    const consumer = consumerFor(q, failing);
    consumer.start();
    const dlq = await drainQueue(sqs, q.dlqUrl, 1, 20_000);
    await consumer.stop();
    expect(calls).toBe(2);
    expect(dlq[0]?.attributes.dlqReason).toBe('retries_exhausted');
  }, 30_000);

  test('stop() (SIGTERM) conclui a mensagem em andamento antes de encerrar', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '100.00');
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const slow = {
      execute: async (...args: Parameters<ProcessWagerTransactionUseCase['execute']>) => {
        await gate;
        return c.processWager.execute(...args);
      },
    } as unknown as ProcessWagerTransactionUseCase;
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '1.00' });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);
    const consumer = consumerFor(q, slow);
    consumer.start();
    await waitFor(async () => (await queueDepth(sqs, q.wagerQueueUrl)).inFlight === 1);
    const stopping = consumer.stop();
    setTimeout(release, 300);
    await stopping;
    expect(await debitsOf(w.id)).toBe(1);
    const d = await queueDepth(sqs, q.wagerQueueUrl);
    expect(d.visible + d.inFlight).toBe(0);
  });
});

describe('dedupe idempotente via SQS com messageIds diferentes', () => {
  test('mesma idempotency key em duas mensagens distintas → um efeito', async () => {
    const q = await createTestQueues(sqs);
    const w = await createWallet(c, '100.00');
    const data = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00', externalTransactionId: uniq('x') });
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);
    await sendWagerMessage(sqs, q.wagerQueueUrl, data);
    const consumer = consumerFor(q);
    consumer.start();
    await waitFor(async () => {
      const d = await queueDepth(sqs, q.wagerQueueUrl);
      return d.visible === 0 && d.inFlight === 0;
    });
    await consumer.stop();
    expect(await debitsOf(w.id)).toBe(1);
  });
});
