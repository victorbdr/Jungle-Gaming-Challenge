import { WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected, WalletBalanceChanged } from '../messaging/domain/events';
import { InboxMessage } from '../messaging/domain/inbox-message';
import type { EventContext, IntegrationEvent } from '../messaging/domain/integration-event';
import { PENDING_REFERENCE_POLICY } from '../shared/domain/backoff';
import { newId } from '../shared/domain/ids';
import { decideWager, type DecisionOutcome } from '../wagering/domain/wager-decision';
import { WagerTransaction, WagerTransactionStatus } from '../wagering/domain/wager-transaction';
import type { Wallet } from '../wallet/domain/wallet';
import { IdempotencyConflictError } from './errors';
import type { AppLogger, Clock, Metrics, TransactionRunner, TransactionSource, WageringUnitOfWork } from './ports';
import { withConcurrencyRetry } from './retry';
import type { WagerCommand } from './wager-command';

export interface InboxContext {
  consumerName: string;
  messageId: string;
  payloadHash: string;
}

export interface ProcessContext {
  correlationId: string;
  causationId?: string | undefined;
  source: TransactionSource;
  /** Presente quando a entrada é SQS: deduplicação persistente na mesma transação. */
  inbox?: InboxContext | undefined;
}

export type ProcessWagerResult =
  | { type: 'result'; transaction: WagerTransaction; idempotentReplay: boolean }
  /** Mensagem SQS já processada (inbox): nada a fazer além do ack. */
  | { type: 'duplicate_message'; transactionId: string | undefined };

/**
 * Use case único para HTTP e SQS. Em uma única transação SQL:
 * inbox → idempotência → lock da wallet → regras → transação + ledger + saldo + outbox.
 */
export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly runner: TransactionRunner,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(command: WagerCommand, ctx: ProcessContext): Promise<ProcessWagerResult> {
    const started = performance.now();
    try {
      return await withConcurrencyRetry('process_wager', this.metrics, () => this.attempt(command, ctx));
    } finally {
      this.metrics.processingLatency(ctx.source, (performance.now() - started) / 1000);
    }
  }

  private async attempt(command: WagerCommand, ctx: ProcessContext): Promise<ProcessWagerResult> {
    const result = await this.runner.run(async (uow) => {
      const now = this.clock.now();

      if (ctx.inbox) {
        const inbox = InboxMessage.receive({ ...ctx.inbox, receivedAt: now });
        inbox.markProcessed(now);
        // O registro existente (duplicata) só é visível após o commit do concorrente: o INSERT espera o lock do índice.
        const registered = await uow.registerInbox(inbox);
        if (!registered.inserted) {
          return { type: 'duplicate_message', transactionId: undefined, inboxHashMismatch: registered.existing.payloadHash !== inbox.payloadHash } as const;
        }
      }

      // Caminho rápido: replay/conflito sem tocar no lock da wallet.
      const existing = await uow.findTransactionByIdempotencyKey(command.idempotencyKey);
      if (existing) return this.replayOrConflict(existing, command);

      const wallet = await uow.lockWallet(command.walletId);
      // Re-checagem sob lock: um concorrente na mesma wallet pode ter acabado de confirmar.
      const raced = await uow.findTransactionByIdempotencyKey(command.idempotencyKey);
      if (raced) return this.replayOrConflict(raced, command);

      const tx = WagerTransaction.create({
        id: newId(),
        providerId: command.providerId,
        externalTransactionId: command.externalTransactionId,
        idempotencyKey: command.idempotencyKey,
        payloadHash: command.payloadHash,
        walletId: command.walletId,
        playerId: command.playerId,
        roundId: command.roundId,
        gameId: command.gameId,
        kind: command.kind,
        money: command.money,
        referenceExternalTransactionId: command.referenceExternalTransactionId,
        createdAt: now,
      });

      const eventCtx: EventContext = { correlationId: ctx.correlationId, causationId: ctx.causationId, occurredAt: now };
      await applyDecision(uow, tx, wallet, now, eventCtx, 'insert');
      return { type: 'result', transaction: tx, idempotentReplay: false } as const;
    });

    if (result.type === 'duplicate_message') {
      this.metrics.duplicateDetected('inbox');
      if (result.inboxHashMismatch) {
        this.logger.warn({ messageId: ctx.inbox?.messageId }, 'inbox duplicate with different payload hash');
      }
      return { type: 'duplicate_message', transactionId: result.transactionId };
    }
    if (result.idempotentReplay) {
      this.metrics.duplicateDetected('idempotency_replay');
    } else {
      this.metrics.transactionFinalized(result.transaction.status, result.transaction.kind, ctx.source);
    }
    this.logger.info(
      {
        transactionId: result.transaction.id,
        walletId: result.transaction.walletId,
        providerId: result.transaction.providerId,
        kind: result.transaction.kind,
        status: result.transaction.status,
        failureCode: result.transaction.failureCode,
        idempotentReplay: result.idempotentReplay,
      },
      'wager transaction handled',
    );
    return { type: 'result', transaction: result.transaction, idempotentReplay: result.idempotentReplay };
  }

  private replayOrConflict(existing: WagerTransaction, command: WagerCommand) {
    if (!existing.matchesPayload(command.payloadHash)) {
      this.metrics.duplicateDetected('idempotency_conflict');
      throw new IdempotencyConflictError(command.idempotencyKey, existing.id);
    }
    return { type: 'result', transaction: existing, idempotentReplay: true } as const;
  }
}

/**
 * Aplica a decisão de domínio e persiste tudo na transação corrente.
 * Compartilhado com o worker de referências pendentes.
 */
export const applyDecision = async (
  uow: WageringUnitOfWork,
  tx: WagerTransaction,
  wallet: Wallet | null,
  now: Date,
  eventCtx: EventContext,
  mode: 'insert' | 'update',
): Promise<DecisionOutcome> => {
  let reference: WagerTransaction | null = null;
  let referenceAlreadyReversed = false;
  if (tx.referenceExternalTransactionId) {
    reference = await uow.findTransactionByExternalId(tx.providerId, tx.referenceExternalTransactionId);
    if (reference && tx.requiresReference() && reference.status === WagerTransactionStatus.Processed) {
      referenceAlreadyReversed = await uow.hasProcessedReversal(reference.id);
    }
  }

  const versionBefore = wallet?.version;
  const outcome = decideWager({
    tx,
    wallet,
    reference,
    referenceAlreadyReversed,
    now,
    pendingReferencePolicy: PENDING_REFERENCE_POLICY,
  });

  // Ordem respeita as FKs: transação → lançamento → saldo.
  if (mode === 'insert') await uow.insertTransaction(tx);
  else await uow.updateTransaction(tx);

  const events: IntegrationEvent<unknown>[] = [];
  if (outcome.type === 'processed') {
    if (outcome.entry && wallet && versionBefore !== undefined) {
      await uow.insertLedgerEntry(outcome.entry);
      await uow.updateWalletBalance(wallet, versionBefore);
      events.push(WalletBalanceChanged.from(wallet, outcome.entry, eventCtx));
    }
    events.push(WagerTransactionProcessed.from(tx, eventCtx));
    // Transações que aguardavam esta como referência podem ser resolvidas já.
    await uow.wakePendingReferences(tx.providerId, tx.externalTransactionId, now);
  } else if (outcome.type === 'rejected') {
    events.push(WagerTransactionRejected.from(tx, eventCtx));
    await uow.wakePendingReferences(tx.providerId, tx.externalTransactionId, now);
  } else if (tx.referenceAttempts === 0) {
    // Evento de pendência só na primeira vez, não a cada nova tentativa.
    events.push(WagerTransactionPendingReference.from(tx, eventCtx));
  }
  await uow.enqueue(events);
  return outcome;
};
