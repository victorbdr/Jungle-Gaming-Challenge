import { CurrencyMismatchError, InsufficientFundsError, InvalidMoneyError } from '../../shared/domain/errors';
import { Money } from '../../shared/domain/money';
import { LedgerDirection, WalletLedgerEntry } from './ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Aggregate root. Toda mudança de saldo passa por debit/credit, que retornam o lançamento
 * correspondente — saldo e ledger nunca divergem dentro do agregado.
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /**
   * Abre a wallet com saldo zero (version 1). O saldo inicial, se positivo, é aplicado
   * pelo chamador via `credit` com a transação OPENING, sem incrementar a versão —
   * a wallet nasce já com o saldo inicial.
   */
  static open(props: { id: string; playerId: string; initialBalance: Money; at: Date; openingTransactionId: string }): {
    wallet: Wallet;
    openingEntry: WalletLedgerEntry | undefined;
  } {
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError('Initial balance cannot be negative');
    }
    const currency = props.initialBalance.currency;
    const wallet = new Wallet(props.id, props.playerId, currency, Money.zero(currency), 1, props.at, props.at);
    if (props.initialBalance.isZero()) {
      return { wallet, openingEntry: undefined };
    }
    const openingEntry = wallet.apply(LedgerDirection.Credit, props.initialBalance, props.openingTransactionId, props.at);
    wallet._version = 1;
    return { wallet, openingEntry };
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  debit(money: Money, transactionId: string, at: Date): WalletLedgerEntry {
    this.assertSameCurrency(money);
    if (!this.canDebit(money)) {
      throw new InsufficientFundsError();
    }
    return this.apply(LedgerDirection.Debit, money, transactionId, at);
  }

  credit(money: Money, transactionId: string, at: Date): WalletLedgerEntry {
    this.assertSameCurrency(money);
    return this.apply(LedgerDirection.Credit, money, transactionId, at);
  }

  private apply(direction: LedgerDirection, money: Money, transactionId: string, at: Date): WalletLedgerEntry {
    if (!money.isPositive()) {
      throw new InvalidMoneyError('Movement amount must be positive');
    }
    const before = this._balance;
    const after = direction === LedgerDirection.Credit ? before.add(money) : before.subtract(money);
    const entry = WalletLedgerEntry.create({
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: at,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
