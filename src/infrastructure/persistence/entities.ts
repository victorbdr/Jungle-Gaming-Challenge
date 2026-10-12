import { defineEntity, p } from '@mikro-orm/core';

/**
 * Modelos de persistência (registros). O domínio não conhece estas classes:
 * os mappers convertem registro ↔ agregado via `rehydrate`. O schema real (constraints,
 * triggers, índices parciais) está nas migrations SQL — estas definições só descrevem colunas.
 * Valores monetários são `numeric(20,2)` mapeados como string (decimal mode 'string').
 */

const WalletSchema = defineEntity({
  name: 'WalletRecord',
  tableName: 'wallets',
  properties: {
    id: p.uuid().primary(),
    playerId: p.text().fieldName('player_id'),
    currency: p.string().columnType('char(3)'),
    balance: p.decimal('string').columnType('numeric(20,2)'),
    version: p.integer(),
    createdAt: p.datetime().fieldName('created_at'),
    updatedAt: p.datetime().fieldName('updated_at'),
  },
});
export class WalletRecord extends WalletSchema.class {}
WalletSchema.setClass(WalletRecord);

const WagerTransactionSchema = defineEntity({
  name: 'WagerTransactionRecord',
  tableName: 'wager_transactions',
  properties: {
    id: p.uuid().primary(),
    providerId: p.text().fieldName('provider_id'),
    externalTransactionId: p.text().fieldName('external_transaction_id'),
    idempotencyKey: p.text().fieldName('idempotency_key'),
    payloadHash: p.string().columnType('char(64)').fieldName('payload_hash'),
    walletId: p.uuid().fieldName('wallet_id'),
    playerId: p.text().fieldName('player_id'),
    roundId: p.text().fieldName('round_id'),
    gameId: p.text().fieldName('game_id'),
    kind: p.text(),
    amount: p.decimal('string').columnType('numeric(20,2)'),
    currency: p.string().columnType('char(3)'),
    referenceExternalTransactionId: p.text().nullable().fieldName('reference_external_transaction_id'),
    referenceTransactionId: p.uuid().nullable().fieldName('reference_transaction_id'),
    status: p.text(),
    failureCode: p.text().nullable().fieldName('failure_code'),
    resultBalanceAmount: p.decimal('string').columnType('numeric(20,2)').nullable().fieldName('result_balance_amount'),
    referenceAttempts: p.integer().fieldName('reference_attempts'),
    nextReferenceAttemptAt: p.datetime().nullable().fieldName('next_reference_attempt_at'),
    createdAt: p.datetime().fieldName('created_at'),
    processedAt: p.datetime().nullable().fieldName('processed_at'),
  },
});
export class WagerTransactionRecord extends WagerTransactionSchema.class {}
WagerTransactionSchema.setClass(WagerTransactionRecord);

const LedgerEntrySchema = defineEntity({
  name: 'LedgerEntryRecord',
  tableName: 'wallet_ledger_entries',
  properties: {
    id: p.uuid().primary(),
    // gerado pelo banco (identity); usado como cursor estável
    seq: p.bigint('string').persist(false),
    walletId: p.uuid().fieldName('wallet_id'),
    transactionId: p.uuid().fieldName('transaction_id'),
    direction: p.text(),
    amount: p.decimal('string').columnType('numeric(20,2)'),
    currency: p.string().columnType('char(3)'),
    balanceBefore: p.decimal('string').columnType('numeric(20,2)').fieldName('balance_before'),
    balanceAfter: p.decimal('string').columnType('numeric(20,2)').fieldName('balance_after'),
    createdAt: p.datetime().fieldName('created_at'),
  },
});
export class LedgerEntryRecord extends LedgerEntrySchema.class {}
LedgerEntrySchema.setClass(LedgerEntryRecord);

const InboxSchema = defineEntity({
  name: 'InboxRecord',
  tableName: 'inbox_messages',
  primaryKeys: ['consumerName', 'messageId'],
  properties: {
    consumerName: p.text().primary().fieldName('consumer_name'),
    messageId: p.text().primary().fieldName('message_id'),
    payloadHash: p.string().columnType('char(64)').fieldName('payload_hash'),
    receivedAt: p.datetime().fieldName('received_at'),
    processedAt: p.datetime().nullable().fieldName('processed_at'),
  },
});
export class InboxRecord extends InboxSchema.class {}
InboxSchema.setClass(InboxRecord);

const OutboxSchema = defineEntity({
  name: 'OutboxRecord',
  tableName: 'outbox_messages',
  properties: {
    id: p.uuid().primary(),
    aggregateId: p.text().fieldName('aggregate_id'),
    eventType: p.text().fieldName('event_type'),
    payload: p.json<Record<string, unknown>>(),
    occurredAt: p.datetime().fieldName('occurred_at'),
    attempts: p.integer(),
    nextAttemptAt: p.datetime().fieldName('next_attempt_at'),
    publishedAt: p.datetime().nullable().fieldName('published_at'),
    lastError: p.text().nullable().fieldName('last_error'),
  },
});
export class OutboxRecord extends OutboxSchema.class {}
OutboxSchema.setClass(OutboxRecord);

export const ENTITIES = [WalletRecord, WagerTransactionRecord, LedgerEntryRecord, InboxRecord, OutboxRecord];
