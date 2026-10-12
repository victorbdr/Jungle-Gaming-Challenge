import type { MoneyProps } from '../../shared/domain/money';
import type { FailureCode } from '../../wagering/domain/failure-code';
import type { WagerTransaction, WagerTransactionKind } from '../../wagering/domain/wager-transaction';
import type { LedgerDirection, WalletLedgerEntry } from '../../wallet/domain/ledger-entry';
import type { Wallet } from '../../wallet/domain/wallet';
import { type EventContext, IntegrationEvent } from './integration-event';

interface TransactionRef {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
}

const refOf = (tx: WagerTransaction): TransactionRef => ({
  transactionId: tx.id,
  providerId: tx.providerId,
  externalTransactionId: tx.externalTransactionId,
  walletId: tx.walletId,
  playerId: tx.playerId,
  roundId: tx.roundId,
  gameId: tx.gameId,
  kind: tx.kind,
  money: tx.money.toJSON(),
});

export interface WagerTransactionProcessedData extends TransactionRef {
  referenceTransactionId: string | null;
  balance: MoneyProps | null;
  processedAt: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed(
      IntegrationEvent.propsFrom(tx.id, ctx, {
        ...refOf(tx),
        referenceTransactionId: tx.referenceTransactionId ?? null,
        balance: tx.resultBalance?.toJSON() ?? null,
        processedAt: (tx.processedAt ?? ctx.occurredAt).toISOString(),
      }),
    );
  }
}

export interface WagerTransactionRejectedData extends TransactionRef {
  failureCode: FailureCode;
  referenceExternalTransactionId: string | null;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    if (!tx.failureCode) {
      throw new Error('Rejected transaction without failureCode');
    }
    return new WagerTransactionRejected(
      IntegrationEvent.propsFrom(tx.id, ctx, {
        ...refOf(tx),
        failureCode: tx.failureCode,
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
      }),
    );
  }
}

export interface WagerTransactionPendingReferenceData extends TransactionRef {
  referenceExternalTransactionId: string;
  attempts: number;
  nextAttemptAt: string | null;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(
      IntegrationEvent.propsFrom(tx.id, ctx, {
        ...refOf(tx),
        referenceExternalTransactionId: tx.referenceExternalTransactionId ?? '',
        attempts: tx.referenceAttempts,
        nextAttemptAt: tx.nextReferenceAttemptAt?.toISOString() ?? null,
      }),
    );
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged(
      IntegrationEvent.propsFrom(wallet.id, ctx, {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      }),
    );
  }
}
