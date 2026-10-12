import { LockMode } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { ConcurrentWriteError } from '../../application/errors';
import type {
  LedgerPage,
  OutboxStore,
  PublishBatch,
  ReconciliationSnapshot,
  TransactionRunner,
  WageringUnitOfWork,
  WalletQueries,
} from '../../application/ports';
import type { InboxMessage } from '../../messaging/domain/inbox-message';
import type { IntegrationEvent } from '../../messaging/domain/integration-event';
import { OutboxMessage } from '../../messaging/domain/outbox-message';
import { Money } from '../../shared/domain/money';
import { WagerTransactionStatus, type WagerTransaction } from '../../wagering/domain/wager-transaction';
import type { WalletLedgerEntry } from '../../wallet/domain/ledger-entry';
import type { Wallet } from '../../wallet/domain/wallet';
import { translateDbError } from './db-errors';
import { InboxRecord, LedgerEntryRecord, OutboxRecord, WagerTransactionRecord, WalletRecord } from './entities';
import { InboxMapper, LedgerEntryMapper, OutboxMapper, WagerTransactionMapper, WalletMapper } from './mappers';

/** Tempo máximo esperando o lock de uma wallet antes de devolver erro transitório (evita filas infinitas em hot wallets). */
const LOCK_TIMEOUT = process.env.DB_LOCK_TIMEOUT ?? '5s';

class MikroOrmUnitOfWork implements WageringUnitOfWork {
  constructor(private readonly em: EntityManager) {}

  async findTransactionByIdempotencyKey(key: string): Promise<WagerTransaction | null> {
    const r = await this.em.findOne(WagerTransactionRecord, { idempotencyKey: key }, { refresh: true });
    return r ? WagerTransactionMapper.toDomain(r) : null;
  }

  async findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    const r = await this.em.findOne(WagerTransactionRecord, { providerId, externalTransactionId }, { refresh: true });
    return r ? WagerTransactionMapper.toDomain(r) : null;
  }

  async lockWallet(walletId: string): Promise<Wallet | null> {
    const r = await this.em.findOne(WalletRecord, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE, refresh: true });
    return r ? WalletMapper.toDomain(r) : null;
  }

  async lockPendingTransaction(transactionId: string): Promise<WagerTransaction | null> {
    const r = await this.em.findOne(
      WagerTransactionRecord,
      { id: transactionId, status: WagerTransactionStatus.PendingReference, nextReferenceAttemptAt: { $lte: new Date() } },
      { lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE, refresh: true },
    );
    return r ? WagerTransactionMapper.toDomain(r) : null;
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const count = await this.em.count(WagerTransactionRecord, {
      referenceTransactionId,
      status: WagerTransactionStatus.Processed,
      kind: { $in: ['REFUND', 'ROLLBACK'] },
    });
    return count > 0;
  }

  async insertWallet(wallet: Wallet): Promise<void> {
    await this.em.insert(WalletRecord, WalletMapper.toRecord(wallet));
  }

  async updateWalletBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const affected = await this.em.nativeUpdate(
      WalletRecord,
      { id: wallet.id, version: expectedVersion },
      { balance: wallet.balance.toString(), version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affected !== 1) throw new ConcurrentWriteError('version_mismatch');
  }

  async insertTransaction(tx: WagerTransaction): Promise<void> {
    await this.em.insert(WagerTransactionRecord, WagerTransactionMapper.toRecord(tx));
  }

  async updateTransaction(tx: WagerTransaction): Promise<void> {
    const { id, ...rest } = WagerTransactionMapper.toRecord(tx);
    // Só campos mutáveis; o trigger do banco rejeita qualquer mudança nos demais ou em linha terminal.
    await this.em.nativeUpdate(
      WagerTransactionRecord,
      { id },
      {
        status: rest.status,
        referenceTransactionId: rest.referenceTransactionId,
        failureCode: rest.failureCode,
        resultBalanceAmount: rest.resultBalanceAmount,
        referenceAttempts: rest.referenceAttempts,
        nextReferenceAttemptAt: rest.nextReferenceAttemptAt,
        processedAt: rest.processedAt,
      },
    );
  }

  async insertLedgerEntry(entry: WalletLedgerEntry): Promise<void> {
    await this.em.insert(LedgerEntryRecord, LedgerEntryMapper.toRecord(entry));
  }

  async enqueue(events: readonly IntegrationEvent<unknown>[]): Promise<void> {
    if (events.length === 0) return;
    await this.em.insertMany(
      OutboxRecord,
      events.map((e) => OutboxMapper.toRecord(OutboxMessage.enqueue(e))),
    );
  }

  async registerInbox(message: InboxMessage): Promise<{ inserted: true } | { inserted: false; existing: InboxMessage }> {
    const rows = await this.em.execute<{ message_id: string }[]>(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, ?) on conflict (consumer_name, message_id) do nothing returning message_id`,
      [message.consumerName, message.messageId, message.payloadHash, message.receivedAt, message.processedAt ?? null],
    );
    if (rows.length > 0) return { inserted: true };
    const existing = await this.em.findOneOrFail(InboxRecord, { consumerName: message.consumerName, messageId: message.messageId });
    return { inserted: false, existing: InboxMapper.toDomain(existing) };
  }

  async wakePendingReferences(providerId: string, externalTransactionId: string, at: Date): Promise<number> {
    // SKIP LOCKED: nunca espera por uma linha que um worker esteja reavaliando.
    const res = await this.em.execute<{ affectedRows?: number }>(
      `update wager_transactions set next_reference_attempt_at = ?
       where id in (
         select id from wager_transactions
         where status = 'PENDING_REFERENCE' and provider_id = ? and reference_external_transaction_id = ?
           and next_reference_attempt_at > ?
         for update skip locked)`,
      [at, providerId, externalTransactionId, at],
      'run',
    );
    return Number(res.affectedRows ?? 0);
  }
}

export class MikroOrmTransactionRunner implements TransactionRunner {
  constructor(private readonly orm: MikroORM) {}

  async run<T>(work: (uow: WageringUnitOfWork) => Promise<T>): Promise<T> {
    try {
      return await this.orm.em.fork().transactional(async (em) => {
        await em.execute(`set local lock_timeout = '${LOCK_TIMEOUT}'`, [], 'run');
        return work(new MikroOrmUnitOfWork(em));
      });
    } catch (err) {
      throw translateDbError(err);
    }
  }
}

export class MikroOrmWalletQueries implements WalletQueries {
  constructor(private readonly orm: MikroORM) {}

  private em(): EntityManager {
    return this.orm.em.fork();
  }

  private async guard<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      throw translateDbError(err);
    }
  }

  findWallet(walletId: string): Promise<Wallet | null> {
    return this.guard(async () => {
      const r = await this.em().findOne(WalletRecord, { id: walletId });
      return r ? WalletMapper.toDomain(r) : null;
    });
  }

  findTransaction(transactionId: string): Promise<WagerTransaction | null> {
    return this.guard(async () => {
      const r = await this.em().findOne(WagerTransactionRecord, { id: transactionId });
      return r ? WagerTransactionMapper.toDomain(r) : null;
    });
  }

  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    return this.guard(async () => {
      const r = await this.em().findOne(WagerTransactionRecord, { providerId, externalTransactionId });
      return r ? WagerTransactionMapper.toDomain(r) : null;
    });
  }

  listLedger(walletId: string, afterSeq: string | null, limit: number): Promise<LedgerPage> {
    return this.guard(async () => {
      const where = afterSeq ? { walletId, seq: { $gt: afterSeq } } : { walletId };
      const rows = await this.em().find(LedgerEntryRecord, where, { orderBy: { seq: 'asc' }, limit: limit + 1 });
      const page = rows.slice(0, limit);
      const hasMore = rows.length > limit;
      return {
        entries: page.map((r) => ({ entry: LedgerEntryMapper.toDomain(r), seq: String(r.seq) })),
        nextCursor: hasMore ? String(page[page.length - 1]!.seq) : null,
      };
    });
  }

  reconciliationSnapshot(walletId: string): Promise<ReconciliationSnapshot | null> {
    return this.guard(async () => {
      // Instrução única ⇒ snapshot MVCC consistente entre saldo e ledger.
      const rows = await this.em().execute<{ currency: string; balance: string; calculated: string; entries: string }[]>(
        `select w.currency, w.balance::text as balance,
                coalesce(sum(case l.direction when 'CREDIT' then l.amount else -l.amount end), 0)::numeric(20,2)::text as calculated,
                count(l.id)::text as entries
         from wallets w left join wallet_ledger_entries l on l.wallet_id = w.id
         where w.id = ? group by w.id`,
        [walletId],
      );
      const row = rows[0];
      if (!row) return null;
      const currency = row.currency.trim();
      return {
        storedBalance: Money.fromSigned({ amount: row.balance, currency }),
        calculatedBalance: Money.fromSigned({ amount: row.calculated, currency }),
        checkedEntries: Number(row.entries),
      };
    });
  }

  findDuePendingReferenceIds(now: Date, limit: number): Promise<{ id: string; walletId: string }[]> {
    return this.guard(async () => {
      const rows = await this.em().find(
        WagerTransactionRecord,
        { status: WagerTransactionStatus.PendingReference, nextReferenceAttemptAt: { $lte: now } },
        { orderBy: { nextReferenceAttemptAt: 'asc' }, limit, fields: ['id', 'walletId'] },
      );
      return rows.map((r) => ({ id: r.id, walletId: r.walletId }));
    });
  }
}

export class MikroOrmOutboxStore implements OutboxStore {
  constructor(private readonly orm: MikroORM) {}

  async processBatch(now: Date, limit: number, publish: PublishBatch): Promise<{ published: number; failed: number }> {
    try {
      return await this.orm.em.fork().transactional(async (em) => {
        // Reivindicação exclusiva: publishers concorrentes pulam as linhas já travadas.
        const rows = await em.execute<Record<string, unknown>[]>(
          `select * from outbox_messages
           where published_at is null and next_attempt_at <= ?
           order by next_attempt_at, occurred_at
           limit ? for update skip locked`,
          [now, limit],
        );
        const messages = rows.map((row) => OutboxMapper.toDomain(row));
        let failures: Map<string, string>;
        try {
          failures = messages.length > 0 ? await publish(messages) : new Map();
        } catch (err) {
          failures = new Map(messages.map((m) => [m.id, (err as Error).message]));
        }
        const at = new Date();
        const publishedIds: string[] = [];
        const failedMessages: OutboxMessage[] = [];
        for (const message of messages) {
          const error = failures.get(message.id);
          if (error === undefined) {
            message.markPublished(at);
            publishedIds.push(message.id);
          } else {
            message.scheduleRetry(at, error);
            failedMessages.push(message);
          }
        }
        if (publishedIds.length > 0) {
          // Um único UPDATE para o caminho feliz.
          await em.execute(
            `update outbox_messages set published_at = ?, attempts = attempts + 1, last_error = null where id = any(?::uuid[])`,
            [at, `{${publishedIds.join(',')}}`],
            'run',
          );
        }
        for (const message of failedMessages) {
          await em.nativeUpdate(
            OutboxRecord,
            { id: message.id },
            { attempts: message.attempts, nextAttemptAt: message.nextAttemptAt ?? at, lastError: message.lastError ?? null },
          );
        }
        return { published: publishedIds.length, failed: failedMessages.length };
      });
    } catch (err) {
      throw translateDbError(err);
    }
  }

  async oldestPendingAgeMs(now: Date): Promise<number> {
    const rows = await this.orm.em
      .fork()
      .execute<{ oldest: Date | null }[]>(`select min(occurred_at) as oldest from outbox_messages where published_at is null`);
    const oldest = rows[0]?.oldest;
    return oldest ? Math.max(0, now.getTime() - new Date(oldest).getTime()) : 0;
  }

  async pendingCount(): Promise<number> {
    return this.orm.em.fork().count(OutboxRecord, { publishedAt: null });
  }
}
