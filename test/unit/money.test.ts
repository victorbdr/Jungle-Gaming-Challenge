import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../src/shared/domain/errors';
import { Money } from '../../src/shared/domain/money';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money', () => {
  test('normaliza para escala fixa de 2 casas', () => {
    expect(brl('25').toString()).toBe('25.00');
    expect(brl('25.5').toString()).toBe('25.50');
    expect(brl('0.01').toJSON()).toEqual({ amount: '0.01', currency: 'BRL' });
  });

  test('aritmética exata, sem erro de ponto flutuante', () => {
    expect(brl('0.10').add(brl('0.20')).toString()).toBe('0.30');
    expect(brl('100.00').subtract(brl('80.00')).toString()).toBe('20.00');
    expect(brl('999999999999999999.99').add(brl('0.01')).toString()).toBe('1000000000000000000.00');
  });

  test('é imutável', () => {
    const a = brl('10.00');
    const b = a.add(brl('5.00'));
    expect(a.toString()).toBe('10.00');
    expect(b.toString()).toBe('15.00');
  });

  test('negate e predicados', () => {
    const neg = brl('5.00').negate();
    expect(neg.isNegative()).toBe(true);
    expect(neg.toString()).toBe('-5.00');
    expect(Money.zero('BRL').isZero()).toBe(true);
    expect(Money.zero('BRL').negate().toString()).toBe('0.00');
    expect(brl('1.00').isPositive()).toBe(true);
    expect(brl('1.00').isLessThan(brl('1.01'))).toBe(true);
    expect(brl('1.0').equals(brl('1.00'))).toBe(true);
  });

  test.each([
    ['NaN'],
    ['Infinity'],
    ['-Infinity'],
    ['1e3'],
    ['1E-2'],
    [''],
    [' '],
    ['1.001'],
    ['0.123'],
    ['-1.00'],
    ['+1.00'],
    ['1,00'],
    ['.5'],
    ['1.'],
    ['0x10'],
  ])('rejeita entrada inválida %p (sem arredondar)', (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test('rejeita amount numérico (dinheiro nunca é number)', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('rejeita moeda inválida', () => {
    expect(() => Money.from({ amount: '1.00', currency: 'brl' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '1.00', currency: 'REAL' })).toThrow(InvalidMoneyError);
  });

  test('operações entre moedas diferentes lançam erro de domínio', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expect(() => brl('1.00').add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1.00').subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1.00').isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(brl('1.00').equals(usd)).toBe(false);
  });

  test('fromSigned aceita negativos internos (reconciliação), mas mantém escala', () => {
    expect(Money.fromSigned({ amount: '-3.50', currency: 'BRL' }).toString()).toBe('-3.50');
    expect(() => Money.fromSigned({ amount: '-3.501', currency: 'BRL' })).toThrow(InvalidMoneyError);
  });
});
