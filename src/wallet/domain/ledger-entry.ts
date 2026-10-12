import { InvariantViolationError } from '../../shared/domain/errors';
import { Money } from '../../shared/domain/money';
import { newId } from '../../shared/domain/ids';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface CreateLedgerEntryProps {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export interface LedgerEntryState extends CreateLedgerEntryProps {
  id: string;
}

/** Lançamento imutável: só campos readonly e nenhuma transição. */
export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    const entry = new WalletLedgerEntry(
      newId(),
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt,
    );
    if (!props.money.isPositive()) {
      throw new InvariantViolationError('Ledger entry amount must be positive');
    }
    if (props.balanceAfter.isNegative()) {
      throw new InvariantViolationError('Ledger entry cannot produce a negative balance');
    }
    if (!entry.isBalanced()) {
      throw new InvariantViolationError('Ledger entry is not balanced');
    }
    return Object.freeze(entry);
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return Object.freeze(
      new WalletLedgerEntry(
        state.id,
        state.walletId,
        state.transactionId,
        state.direction,
        state.money,
        state.balanceBefore,
        state.balanceAfter,
        state.createdAt,
      ),
    );
  }

  /** balanceBefore ± money === balanceAfter */
  isBalanced(): boolean {
    const expected =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }

  /** Efeito com sinal sobre o saldo (crédito positivo, débito negativo). */
  signedAmount(): Money {
    return this.direction === LedgerDirection.Credit ? this.money : this.money.negate();
  }
}
