import { InboxMessage } from '../../messaging/domain/inbox-message';
import { OutboxMessage } from '../../messaging/domain/outbox-message';
import { Money } from '../../shared/domain/money';
import type { FailureCode } from '../../wagering/domain/failure-code';
import { WagerTransaction, type WagerTransactionKind, type WagerTransactionStatus } from '../../wagering/domain/wager-transaction';
import { type LedgerDirection, WalletLedgerEntry } from '../../wallet/domain/ledger-entry';
import { Wallet } from '../../wallet/domain/wallet';
import type { InboxRecord, LedgerEntryRecord, OutboxRecord, WagerTransactionRecord, WalletRecord } from './entities';

/** Linhas cruas (snake_case) vindas de SQL nativo ou registros do ORM (camelCase). */
type Row = Record<string, unknown>;

const money = (amount: unknown, currency: string): Money => Money.from({ amount: String(amount), currency: currency.trim() });
const date = (v: unknown): Date => (v instanceof Date ? v : new Date(String(v)));
const optDate = (v: unknown): Date | undefined => (v === null || v === undefined ? undefined : date(v));
const optStr = (v: unknown): string | undefined => (v === null || v === undefined ? undefined : String(v));

export const WalletMapper = {
  toDomain(r: Pick<WalletRecord, 'id' | 'playerId' | 'currency' | 'balance' | 'version' | 'createdAt' | 'updatedAt'>): Wallet {
    return Wallet.rehydrate({
      id: r.id,
      playerId: r.playerId,
      currency: r.currency.trim(),
      balance: money(r.balance, r.currency),
      version: Number(r.version),
      createdAt: date(r.createdAt),
      updatedAt: date(r.updatedAt),
    });
  },
  toRecord(w: Wallet) {
    return {
      id: w.id,
      playerId: w.playerId,
      currency: w.currency,
      balance: w.balance.toString(),
      version: w.version,
      createdAt: w.createdAt,
      updatedAt: w.updatedAt,
    };
  },
};

export const WagerTransactionMapper = {
  toDomain(r: WagerTransactionRecord): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: r.id,
      providerId: r.providerId,
      externalTransactionId: r.externalTransactionId,
      idempotencyKey: r.idempotencyKey,
      payloadHash: r.payloadHash,
      walletId: r.walletId,
      playerId: r.playerId,
      roundId: r.roundId,
      gameId: r.gameId,
      kind: r.kind as WagerTransactionKind,
      money: money(r.amount, r.currency),
      referenceExternalTransactionId: optStr(r.referenceExternalTransactionId),
      createdAt: date(r.createdAt),
      status: r.status as WagerTransactionStatus,
      referenceTransactionId: optStr(r.referenceTransactionId),
      failureCode: optStr(r.failureCode) as FailureCode | undefined,
      processedAt: optDate(r.processedAt),
      resultBalance:
        r.resultBalanceAmount === null || r.resultBalanceAmount === undefined ? undefined : money(r.resultBalanceAmount, r.currency),
      referenceAttempts: Number(r.referenceAttempts),
      nextReferenceAttemptAt: optDate(r.nextReferenceAttemptAt),
    });
  },
  toRecord(t: WagerTransaction) {
    return {
      id: t.id,
      providerId: t.providerId,
      externalTransactionId: t.externalTransactionId,
      idempotencyKey: t.idempotencyKey,
      payloadHash: t.payloadHash,
      walletId: t.walletId,
      playerId: t.playerId,
      roundId: t.roundId,
      gameId: t.gameId,
      kind: t.kind,
      amount: t.money.toString(),
      currency: t.money.currency,
      referenceExternalTransactionId: t.referenceExternalTransactionId ?? null,
      referenceTransactionId: t.referenceTransactionId ?? null,
      status: t.status,
      failureCode: t.failureCode ?? null,
      resultBalanceAmount: t.resultBalance?.toString() ?? null,
      referenceAttempts: t.referenceAttempts,
      nextReferenceAttemptAt: t.nextReferenceAttemptAt ?? null,
      createdAt: t.createdAt,
      processedAt: t.processedAt ?? null,
    };
  },
};

export const LedgerEntryMapper = {
  toDomain(r: LedgerEntryRecord): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: r.id,
      walletId: r.walletId,
      transactionId: r.transactionId,
      direction: r.direction as LedgerDirection,
      money: money(r.amount, r.currency),
      balanceBefore: money(r.balanceBefore, r.currency),
      balanceAfter: money(r.balanceAfter, r.currency),
      createdAt: date(r.createdAt),
    });
  },
  toRecord(e: WalletLedgerEntry) {
    return {
      id: e.id,
      walletId: e.walletId,
      transactionId: e.transactionId,
      direction: e.direction,
      amount: e.money.toString(),
      currency: e.money.currency,
      balanceBefore: e.balanceBefore.toString(),
      balanceAfter: e.balanceAfter.toString(),
      createdAt: e.createdAt,
    };
  },
};

export const InboxMapper = {
  toDomain(r: InboxRecord | Row): InboxMessage {
    const x = r as Row;
    return InboxMessage.rehydrate({
      consumerName: String(x.consumerName ?? x.consumer_name),
      messageId: String(x.messageId ?? x.message_id),
      payloadHash: String(x.payloadHash ?? x.payload_hash),
      receivedAt: date(x.receivedAt ?? x.received_at),
      processedAt: optDate(x.processedAt ?? x.processed_at),
    });
  },
};

export const OutboxMapper = {
  toDomain(r: OutboxRecord | Row): OutboxMessage {
    const x = r as Row;
    return OutboxMessage.rehydrate({
      id: String(x.id),
      aggregateId: String(x.aggregateId ?? x.aggregate_id),
      eventType: String(x.eventType ?? x.event_type),
      payload: (typeof x.payload === 'string' ? JSON.parse(x.payload) : x.payload) as Record<string, unknown>,
      occurredAt: date(x.occurredAt ?? x.occurred_at),
      attempts: Number(x.attempts),
      nextAttemptAt: optDate(x.nextAttemptAt ?? x.next_attempt_at),
      publishedAt: optDate(x.publishedAt ?? x.published_at),
      lastError: optStr(x.lastError ?? x.last_error),
    });
  },
  toRecord(m: OutboxMessage) {
    return {
      id: m.id,
      aggregateId: m.aggregateId,
      eventType: m.eventType,
      payload: m.payload as Record<string, unknown>,
      occurredAt: m.occurredAt,
      attempts: m.attempts,
      nextAttemptAt: m.nextAttemptAt ?? m.occurredAt,
      publishedAt: m.publishedAt ?? null,
      lastError: m.lastError ?? null,
    };
  },
};
