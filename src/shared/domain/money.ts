import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './errors';

/** DTO serializável de Money. */
export interface MoneyProps {
  amount: string;
  currency: string;
}

const SCALE = 2;
const AMOUNT_PATTERN = /^\d{1,18}(\.\d{1,2})?$/;
const SIGNED_AMOUNT_PATTERN = /^-?\d{1,18}(\.\d{1,2})?$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;

// Instância isolada: não altera a configuração global do decimal.js.
const D = Decimal.clone({ precision: 40, rounding: Decimal.ROUND_HALF_EVEN });

/**
 * Valor monetário imutável com escala fixa de 2 casas.
 * Nunca usa `number`: entrada e saída são strings decimais.
 */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  /** Contrato de entrada: string decimal não negativa com no máximo 2 casas. */
  static from(props: MoneyProps): Money {
    Money.assertProps(props);
    if (!AMOUNT_PATTERN.test(props.amount)) {
      throw new InvalidMoneyError(`Invalid amount "${props.amount}": expected non-negative decimal with up to 2 places`);
    }
    return new Money(new D(props.amount), props.currency);
  }

  /** Reconstrói valores persistidos/calculados internamente, que podem ser negativos (ex.: diferença de reconciliação). */
  static fromSigned(props: MoneyProps): Money {
    Money.assertProps(props);
    if (!SIGNED_AMOUNT_PATTERN.test(props.amount)) {
      throw new InvalidMoneyError(`Invalid amount "${props.amount}"`);
    }
    return new Money(new D(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(new D(0), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.equals(other.value);
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  /** Valor com escala fixa, ex.: "25.00". */
  toString(): string {
    const fixed = this.value.toFixed(SCALE);
    return fixed === '-0.00' ? '0.00' : fixed;
  }

  private assertSameCurrency(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  private static assertProps(props: MoneyProps): void {
    if (props === null || typeof props !== 'object') {
      throw new InvalidMoneyError('Money must be an object');
    }
    if (typeof props.amount !== 'string') {
      throw new InvalidMoneyError('Amount must be a decimal string');
    }
    Money.assertCurrency(props.currency);
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`Invalid currency "${String(currency)}": expected ISO-4217 code`);
    }
  }
}
