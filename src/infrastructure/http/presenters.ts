import type { ReconciliationReport } from '../../application/reconcile-wallet.use-case';
import { WagerTransactionStatus, type WagerTransaction } from '../../wagering/domain/wager-transaction';
import type { WalletLedgerEntry } from '../../wallet/domain/ledger-entry';
import type { Wallet } from '../../wallet/domain/wallet';

export const presentWallet = (w: Wallet) => ({
  id: w.id,
  playerId: w.playerId,
  balance: w.balance.toJSON(),
  version: w.version,
  createdAt: w.createdAt.toISOString(),
  updatedAt: w.updatedAt.toISOString(),
});

/** Resposta da submissão: idêntica em replays, exceto `idempotentReplay`. */
export const presentSubmission = (tx: WagerTransaction, idempotentReplay: boolean) => ({
  transactionId: tx.id,
  status: tx.status,
  balance: tx.resultBalance?.toJSON() ?? null,
  ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
  ...(tx.referenceTransactionId ? { referenceTransactionId: tx.referenceTransactionId } : {}),
  idempotentReplay,
});

export const presentTransaction = (tx: WagerTransaction) => ({
  id: tx.id,
  providerId: tx.providerId,
  externalTransactionId: tx.externalTransactionId,
  walletId: tx.walletId,
  playerId: tx.playerId,
  roundId: tx.roundId,
  gameId: tx.gameId,
  kind: tx.kind,
  money: tx.money.toJSON(),
  status: tx.status,
  failureCode: tx.failureCode ?? null,
  referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
  referenceTransactionId: tx.referenceTransactionId ?? null,
  resultBalance: tx.resultBalance?.toJSON() ?? null,
  referenceAttempts: tx.referenceAttempts,
  nextReferenceAttemptAt: tx.nextReferenceAttemptAt?.toISOString() ?? null,
  createdAt: tx.createdAt.toISOString(),
  processedAt: tx.processedAt?.toISOString() ?? null,
});

export const presentLedgerEntry = (e: WalletLedgerEntry) => ({
  id: e.id,
  transactionId: e.transactionId,
  direction: e.direction,
  money: e.money.toJSON(),
  balanceBefore: e.balanceBefore.toJSON(),
  balanceAfter: e.balanceAfter.toJSON(),
  createdAt: e.createdAt.toISOString(),
});

export const presentReconciliation = (r: ReconciliationReport) => ({
  walletId: r.walletId,
  storedBalance: r.storedBalance.toJSON(),
  calculatedBalance: r.calculatedBalance.toJSON(),
  difference: r.difference.toJSON(),
  consistent: r.consistent,
  checkedEntries: r.checkedEntries,
});

/**
 * Status HTTP por resultado (o mesmo no replay):
 * PROCESSED → 200, PENDING_REFERENCE → 202, REJECTED → 422, FAILED → 500.
 */
export const httpStatusFor = (tx: WagerTransaction): number => {
  switch (tx.status) {
    case WagerTransactionStatus.Processed:
      return 200;
    case WagerTransactionStatus.PendingReference:
    case WagerTransactionStatus.Pending:
      return 202;
    case WagerTransactionStatus.Rejected:
      return 422;
    case WagerTransactionStatus.Failed:
      return 500;
  }
};
