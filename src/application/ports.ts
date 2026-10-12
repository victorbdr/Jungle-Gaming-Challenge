import type { InboxMessage } from '../messaging/domain/inbox-message';
import type { IntegrationEvent } from '../messaging/domain/integration-event';
import type { OutboxMessage } from '../messaging/domain/outbox-message';
import type { Money } from '../shared/domain/money';
import type { WagerTransaction } from '../wagering/domain/wager-transaction';
import type { WalletLedgerEntry } from '../wallet/domain/ledger-entry';
import type { Wallet } from '../wallet/domain/wallet';

export const CLOCK = Symbol('Clock');
export const TRANSACTION_RUNNER = Symbol('TransactionRunner');
export const WALLET_QUERIES = Symbol('WalletQueries');
export const OUTBOX_STORE = Symbol('OutboxStore');
export const EVENT_PUBLISHER = Symbol('EventPublisher');
export const METRICS = Symbol('Metrics');
export const LOGGER = Symbol('Logger');

export interface Clock {
  now(): Date;
}

/**
 * Operações disponíveis dentro de UMA transação SQL. Tudo que é escrito por meio desta
 * interface é confirmado ou descartado junto (wallet, transação, ledger, inbox, outbox).
 */
export interface WageringUnitOfWork {
  findTransactionByIdempotencyKey(key: string): Promise<WagerTransaction | null>;
  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  /** SELECT ... FOR UPDATE na linha da wallet: unidade de concorrência. */
  lockWallet(walletId: string): Promise<Wallet | null>;
  /** Trava a transação PENDING_REFERENCE se ainda estiver pendente; SKIP LOCKED. */
  lockPendingTransaction(transactionId: string): Promise<WagerTransaction | null>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;

  insertWallet(wallet: Wallet): Promise<void>;
  /** UPDATE condicionado à versão anterior; falha se outra escrita ocorreu (defesa em profundidade). */
  updateWalletBalance(wallet: Wallet, expectedVersion: number): Promise<void>;
  insertTransaction(tx: WagerTransaction): Promise<void>;
  updateTransaction(tx: WagerTransaction): Promise<void>;
  insertLedgerEntry(entry: WalletLedgerEntry): Promise<void>;
  enqueue(events: readonly IntegrationEvent<unknown>[]): Promise<void>;
  /** INSERT ... ON CONFLICT DO NOTHING. Retorna o registro existente em caso de duplicata. */
  registerInbox(message: InboxMessage): Promise<{ inserted: true } | { inserted: false; existing: InboxMessage }>;
  /** Antecipa a próxima tentativa das transações que aguardam esta referência (sem bloquear). */
  wakePendingReferences(providerId: string, externalTransactionId: string, at: Date): Promise<number>;
}

export interface TransactionRunner {
  run<T>(work: (uow: WageringUnitOfWork) => Promise<T>): Promise<T>;
}

export interface LedgerPage {
  entries: { entry: WalletLedgerEntry; seq: string }[];
  nextCursor: string | null;
}

export interface ReconciliationSnapshot {
  storedBalance: Money;
  calculatedBalance: Money;
  checkedEntries: number;
}

export interface WalletQueries {
  findWallet(walletId: string): Promise<Wallet | null>;
  findTransaction(transactionId: string): Promise<WagerTransaction | null>;
  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  listLedger(walletId: string, afterSeq: string | null, limit: number): Promise<LedgerPage>;
  /** Snapshot consistente (uma única instrução SQL) de saldo armazenado vs. soma do ledger. */
  reconciliationSnapshot(walletId: string): Promise<ReconciliationSnapshot | null>;
  findDuePendingReferenceIds(now: Date, limit: number): Promise<{ id: string; walletId: string }[]>;
}

export interface OutboxStore {
  /**
   * Reivindica até `limit` mensagens vencidas (FOR UPDATE SKIP LOCKED), publica o lote
   * e grava o resultado de cada mensagem na mesma transação. Publishers concorrentes nunca
   * pegam a mesma linha ao mesmo tempo.
   */
  processBatch(now: Date, limit: number, publish: PublishBatch): Promise<{ published: number; failed: number }>;
  oldestPendingAgeMs(now: Date): Promise<number>;
  pendingCount(): Promise<number>;
}

/** Publica um lote; devolve as falhas por id (id → erro). Ausente no mapa ⇒ publicado. */
export type PublishBatch = (messages: readonly OutboxMessage[]) => Promise<Map<string, string>>;

export interface EventPublisher {
  publishBatch: PublishBatch;
}

export type TransactionSource = 'http' | 'sqs' | 'worker';

export interface Metrics {
  transactionFinalized(status: string, kind: string, source: TransactionSource): void;
  duplicateDetected(kind: 'idempotency_replay' | 'idempotency_conflict' | 'inbox'): void;
  retry(operation: string): void;
  lockConflict(reason: 'lock_timeout' | 'deadlock' | 'serialization' | 'version_mismatch' | 'unique_race'): void;
  processingLatency(source: TransactionSource, seconds: number): void;
  dlqMessage(reason: string): void;
  outboxPublished(count: number): void;
  outboxPublishFailed(count: number): void;
  outboxLag(seconds: number, pending: number): void;
  reconciliationMismatch(): void;
}

export interface AppLogger {
  debug(obj: object, msg?: string): void;
  info(obj: object, msg?: string): void;
  warn(obj: object, msg?: string): void;
  error(obj: object, msg?: string): void;
  child(bindings: Record<string, unknown>): AppLogger;
}
