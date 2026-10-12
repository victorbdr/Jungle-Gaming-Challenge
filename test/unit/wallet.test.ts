import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InsufficientFundsError, InvariantViolationError } from '../../src/shared/domain/errors';
import { Money } from '../../src/shared/domain/money';
import { LedgerDirection, WalletLedgerEntry } from '../../src/wallet/domain/ledger-entry';
import { Wallet } from '../../src/wallet/domain/wallet';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-01-01T00:00:00Z');

const open = (initial = '100.00') =>
  Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: brl(initial), at, openingTransactionId: 'tx-open' });

describe('Wallet', () => {
  test('abre com version 1 e lançamento OPENING quando saldo inicial > 0', () => {
    const { wallet, openingEntry } = open('100.00');
    expect(wallet.version).toBe(1);
    expect(wallet.balance.toString()).toBe('100.00');
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.balanceBefore.toString()).toBe('0.00');
    expect(openingEntry?.balanceAfter.toString()).toBe('100.00');
  });

  test('saldo inicial zero não gera lançamento', () => {
    const { wallet, openingEntry } = open('0.00');
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeUndefined();
  });

  test('débito e crédito incrementam version e produzem lançamento balanceado', () => {
    const { wallet } = open('100.00');
    const d = wallet.debit(brl('30.00'), 'tx1', at);
    expect(wallet.balance.toString()).toBe('70.00');
    expect(wallet.version).toBe(2);
    expect(d.isBalanced()).toBe(true);
    const c = wallet.credit(brl('5.00'), 'tx2', at);
    expect(wallet.balance.toString()).toBe('75.00');
    expect(wallet.version).toBe(3);
    expect(c.balanceBefore.toString()).toBe('70.00');
  });

  test('nunca fica negativo', () => {
    const { wallet } = open('10.00');
    expect(() => wallet.debit(brl('10.01'), 'tx', at)).toThrow(InsufficientFundsError);
    expect(wallet.balance.toString()).toBe('10.00');
    expect(wallet.version).toBe(1);
    wallet.debit(brl('10.00'), 'tx', at);
    expect(wallet.balance.isZero()).toBe(true);
  });

  test('moeda da operação deve ser igual à da wallet', () => {
    const { wallet } = open('10.00');
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expect(() => wallet.credit(usd, 'tx', at)).toThrow(CurrencyMismatchError);
    expect(() => wallet.debit(usd, 'tx', at)).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test('saldo == soma do ledger', () => {
    const { wallet, openingEntry } = open('50.00');
    const entries = [openingEntry!, wallet.debit(brl('20.00'), 'a', at), wallet.credit(brl('7.25'), 'b', at)];
    const sum = entries.reduce((acc, e) => acc.add(e.signedAmount()), Money.zero('BRL'));
    expect(sum.equals(wallet.balance)).toBe(true);
  });
});

describe('WalletLedgerEntry', () => {
  test('factory rejeita aritmética inconsistente', () => {
    expect(() =>
      WalletLedgerEntry.create({
        walletId: 'w',
        transactionId: 't',
        direction: LedgerDirection.Credit,
        money: brl('10.00'),
        balanceBefore: brl('0.00'),
        balanceAfter: brl('9.99'),
        createdAt: at,
      }),
    ).toThrow(InvariantViolationError);
  });

  test('é estruturalmente imutável', () => {
    const e = WalletLedgerEntry.create({
      walletId: 'w',
      transactionId: 't',
      direction: LedgerDirection.Debit,
      money: brl('1.00'),
      balanceBefore: brl('1.00'),
      balanceAfter: brl('0.00'),
      createdAt: at,
    });
    expect(Object.isFrozen(e)).toBe(true);
    expect(() => {
      (e as unknown as { walletId: string }).walletId = 'x';
    }).toThrow();
  });
});
