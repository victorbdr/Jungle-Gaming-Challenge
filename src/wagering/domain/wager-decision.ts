import type { BackoffPolicy } from '../../shared/domain/backoff';
import { LedgerDirection, type WalletLedgerEntry } from '../../wallet/domain/ledger-entry';
import type { Wallet } from '../../wallet/domain/wallet';
import { FailureCode } from './failure-code';
import { type WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from './wager-transaction';

export interface DecisionInput {
  tx: WagerTransaction;
  /** Wallet já travada pelo chamador (ou null se inexistente). */
  wallet: Wallet | null;
  /** Transação referenciada, resolvida por (providerId, referenceExternalTransactionId). */
  reference: WagerTransaction | null;
  /** A referência já possui uma reversão PROCESSED? */
  referenceAlreadyReversed: boolean;
  now: Date;
  pendingReferencePolicy: BackoffPolicy;
}

export type DecisionOutcome =
  | { type: 'processed'; entry: WalletLedgerEntry | undefined }
  | { type: 'rejected'; code: FailureCode }
  | { type: 'pending_reference' };

/**
 * Aplica as regras de negócio sobre uma transação PENDING/PENDING_REFERENCE, mutando
 * a transação (status) e a wallet (saldo + lançamento) de forma consistente.
 * Puro: sem I/O; a atomicidade e o lock ficam a cargo do use case.
 */
export const decideWager = (input: DecisionInput): DecisionOutcome => {
  const { tx, wallet, reference, now } = input;

  const reject = (code: FailureCode): DecisionOutcome => {
    tx.reject(code, now, wallet?.balance);
    return { type: 'rejected', code };
  };

  if (!wallet) return reject(FailureCode.WalletNotFound);
  if (wallet.playerId !== tx.playerId) return reject(FailureCode.WalletPlayerMismatch);
  if (wallet.currency !== tx.money.currency) return reject(FailureCode.CurrencyMismatch);

  if (tx.referenceExternalTransactionId) {
    // Referência ausente ou ainda não finalizada: aguardar (com limite).
    if (!reference || !reference.isTerminal()) {
      if (tx.status === WagerTransactionStatus.PendingReference && tx.referenceRetriesExhausted(input.pendingReferencePolicy)) {
        return reject(FailureCode.ReferenceNotFound);
      }
      tx.markPendingReference(now, input.pendingReferencePolicy);
      return { type: 'pending_reference' };
    }
    if (!tx.acceptsReferenceKind(reference.kind)) return reject(FailureCode.ReferenceKindNotAllowed);
    const sameContext =
      reference.providerId === tx.providerId &&
      reference.playerId === tx.playerId &&
      reference.walletId === tx.walletId &&
      reference.money.currency === tx.money.currency &&
      reference.roundId === tx.roundId;
    if (!sameContext) return reject(FailureCode.ReferenceMismatch);
    if (reference.status !== WagerTransactionStatus.Processed) return reject(FailureCode.ReferenceNotProcessed);
    if (tx.requiresReference()) {
      if (input.referenceAlreadyReversed) return reject(FailureCode.ReferenceAlreadyReversed);
      if (!tx.money.equals(reference.money)) return reject(FailureCode.ReversalAmountMismatch);
    }
  }

  if (!tx.affectsBalance()) {
    tx.markProcessed(reference?.id, now, wallet.balance);
    return { type: 'processed', entry: undefined };
  }

  const direction = tx.ledgerDirectionFor(reference ?? undefined);
  if (direction === LedgerDirection.Debit && !wallet.canDebit(tx.money)) {
    return reject(tx.kind === WagerTransactionKind.Bet ? FailureCode.InsufficientFunds : FailureCode.ReversalWouldOverdraw);
  }
  const entry =
    direction === LedgerDirection.Debit ? wallet.debit(tx.money, tx.id, now) : wallet.credit(tx.money, tx.id, now);
  tx.markProcessed(reference?.id, now, wallet.balance);
  return { type: 'processed', entry };
};
