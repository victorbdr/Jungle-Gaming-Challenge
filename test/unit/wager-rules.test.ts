import { describe, expect, test } from 'bun:test';
import { PENDING_REFERENCE_POLICY } from '../../src/shared/domain/backoff';
import { sha256Canonical } from '../../src/shared/domain/canonical-hash';
import { InvalidTransactionStateError } from '../../src/shared/domain/errors';
import { Money } from '../../src/shared/domain/money';
import { FailureCode } from '../../src/wagering/domain/failure-code';
import { decideWager } from '../../src/wagering/domain/wager-decision';
import {
  InvalidWagerRequestError,
  WagerTransaction,
  WagerTransactionKind as K,
  WagerTransactionStatus as S,
} from '../../src/wagering/domain/wager-transaction';
import { LedgerDirection } from '../../src/wallet/domain/ledger-entry';
import { Wallet } from '../../src/wallet/domain/wallet';

const now = new Date('2026-01-01T00:00:00Z');
const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

let seq = 0;
const tx = (kind: K, amount: string, extra: Partial<{ ref: string; round: string; player: string; wallet: string; currency: string }> = {}) => {
  seq += 1;
  return WagerTransaction.create({
    id: `tx-${seq}`,
    providerId: 'provider-a',
    externalTransactionId: `ext-${seq}`,
    idempotencyKey: `provider-a:ext-${seq}`,
    payloadHash: 'h',
    walletId: extra.wallet ?? 'w1',
    playerId: extra.player ?? 'p1',
    roundId: extra.round ?? 'r1',
    gameId: 'g',
    kind,
    money: Money.from({ amount, currency: extra.currency ?? 'BRL' }),
    referenceExternalTransactionId: extra.ref,
    createdAt: now,
  });
};

const wallet = (initial = '100.00') =>
  Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: brl(initial), at: now, openingTransactionId: 'open' }).wallet;

const decide = (t: WagerTransaction, w: Wallet | null, reference: WagerTransaction | null = null, referenceAlreadyReversed = false) =>
  decideWager({ tx: t, wallet: w, reference, referenceAlreadyReversed, now, pendingReferencePolicy: PENDING_REFERENCE_POLICY });

const processedBet = (w: Wallet, amount = '30.00') => {
  const bet = tx(K.Bet, amount);
  decide(bet, w);
  expect(bet.status).toBe(S.Processed);
  return bet;
};

describe('BET', () => {
  test('debita e gera 1 lançamento DEBIT', () => {
    const w = wallet();
    const bet = tx(K.Bet, '25.00');
    const out = decide(bet, w);
    expect(out.type).toBe('processed');
    expect(out.type === 'processed' && out.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.toString()).toBe('75.00');
    expect(bet.resultBalance?.toString()).toBe('75.00');
  });

  test('rejeita por saldo insuficiente sem mover saldo', () => {
    const w = wallet('10.00');
    const bet = tx(K.Bet, '10.01');
    expect(decide(bet, w)).toEqual({ type: 'rejected', code: FailureCode.InsufficientFunds });
    expect(bet.status).toBe(S.Rejected);
    expect(w.balance.toString()).toBe('10.00');
    expect(w.version).toBe(1);
  });

  test('cenário da seção 8: 100 - 80 - 80', () => {
    const w = wallet('100.00');
    const a = tx(K.Bet, '80.00');
    const b = tx(K.Bet, '80.00');
    decide(a, w);
    decide(b, w);
    expect(a.status).toBe(S.Processed);
    expect(b.status).toBe(S.Rejected);
    expect(b.failureCode).toBe(FailureCode.InsufficientFunds);
    expect(w.balance.toString()).toBe('20.00');
  });
});

describe('WIN', () => {
  test('credita sem referência', () => {
    const w = wallet();
    decide(tx(K.Win, '50.00'), w);
    expect(w.balance.toString()).toBe('150.00');
  });

  test('com referência à BET da mesma rodada', () => {
    const w = wallet();
    const bet = processedBet(w);
    const win = tx(K.Win, '60.00', { ref: bet.externalTransactionId });
    decide(win, w, bet);
    expect(win.status).toBe(S.Processed);
    expect(win.referenceTransactionId).toBe(bet.id);
  });

  test('referência de outra rodada é rejeitada', () => {
    const w = wallet();
    const bet = processedBet(w);
    const win = tx(K.Win, '60.00', { ref: bet.externalTransactionId, round: 'other' });
    expect(decide(win, w, bet)).toEqual({ type: 'rejected', code: FailureCode.ReferenceMismatch });
  });
});

describe('LOSS', () => {
  test('não move saldo nem gera lançamento', () => {
    const w = wallet();
    const loss = tx(K.Loss, '0.00');
    expect(decide(loss, w)).toEqual({ type: 'processed', entry: undefined });
    expect(w.balance.toString()).toBe('100.00');
    expect(w.version).toBe(1);
    expect(loss.affectsBalance()).toBe(false);
  });
});

describe('REFUND', () => {
  test('exige referência', () => {
    expect(() => tx(K.Refund, '10.00')).toThrow(InvalidWagerRequestError);
  });

  test('reverte BET processada', () => {
    const w = wallet();
    const bet = processedBet(w, '30.00');
    const refund = tx(K.Refund, '30.00', { ref: bet.externalTransactionId });
    decide(refund, w, bet);
    expect(refund.status).toBe(S.Processed);
    expect(w.balance.toString()).toBe('100.00');
  });

  test('só referencia BET', () => {
    const w = wallet();
    const win = tx(K.Win, '10.00');
    decide(win, w);
    const refund = tx(K.Refund, '10.00', { ref: win.externalTransactionId });
    expect(decide(refund, w, win)).toEqual({ type: 'rejected', code: FailureCode.ReferenceKindNotAllowed });
  });

  test('uma única vez', () => {
    const w = wallet();
    const bet = processedBet(w);
    const refund = tx(K.Refund, '30.00', { ref: bet.externalTransactionId });
    expect(decide(refund, w, bet, true)).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
  });

  test('valor deve ser igual ao da referência', () => {
    const w = wallet();
    const bet = processedBet(w, '30.00');
    const refund = tx(K.Refund, '10.00', { ref: bet.externalTransactionId });
    expect(decide(refund, w, bet)).toEqual({ type: 'rejected', code: FailureCode.ReversalAmountMismatch });
  });

  test('referência REJECTED não pode ser revertida', () => {
    const w = wallet('10.00');
    const bet = tx(K.Bet, '50.00');
    decide(bet, w);
    const refund = tx(K.Refund, '50.00', { ref: bet.externalTransactionId });
    expect(decide(refund, w, bet)).toEqual({ type: 'rejected', code: FailureCode.ReferenceNotProcessed });
    expect(w.balance.toString()).toBe('10.00');
  });
});

describe('ROLLBACK', () => {
  test('de BET credita', () => {
    const w = wallet();
    const bet = processedBet(w, '40.00');
    const rb = tx(K.Rollback, '40.00', { ref: bet.externalTransactionId });
    const out = decide(rb, w, bet);
    expect(out.type === 'processed' && out.entry?.direction).toBe(LedgerDirection.Credit);
    expect(w.balance.toString()).toBe('100.00');
  });

  test('de WIN debita', () => {
    const w = wallet();
    const win = tx(K.Win, '40.00');
    decide(win, w);
    const rb = tx(K.Rollback, '40.00', { ref: win.externalTransactionId });
    const out = decide(rb, w, win);
    expect(out.type === 'processed' && out.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.toString()).toBe('100.00');
  });

  test('que deixaria saldo negativo é rejeitado com código distinto', () => {
    const w = wallet('0.00');
    const win = tx(K.Win, '40.00');
    decide(win, w);
    decide(tx(K.Bet, '40.00'), w);
    const rb = tx(K.Rollback, '40.00', { ref: win.externalTransactionId });
    expect(decide(rb, w, win)).toEqual({ type: 'rejected', code: FailureCode.ReversalWouldOverdraw });
    expect(w.balance.toString()).toBe('0.00');
  });

  test('não referencia LOSS nem ROLLBACK', () => {
    const w = wallet();
    const loss = tx(K.Loss, '0.00');
    decide(loss, w);
    expect(decide(tx(K.Rollback, '0.01', { ref: loss.externalTransactionId }), w, loss)).toEqual({
      type: 'rejected',
      code: FailureCode.ReferenceKindNotAllowed,
    });
  });

  test('referência ausente → PENDING_REFERENCE, e REJECTED ao esgotar tentativas', () => {
    const w = wallet();
    const rb = tx(K.Rollback, '10.00', { ref: 'missing' });
    expect(decide(rb, w)).toEqual({ type: 'pending_reference' });
    expect(rb.status).toBe(S.PendingReference);
    expect(rb.nextReferenceAttemptAt!.getTime()).toBeGreaterThan(now.getTime());
    let out = decide(rb, w);
    for (let i = 0; i < 20 && out.type === 'pending_reference'; i++) out = decide(rb, w);
    expect(out).toEqual({ type: 'rejected', code: FailureCode.ReferenceNotFound });
    expect(rb.referenceAttempts).toBe(PENDING_REFERENCE_POLICY.maxAttempts - 1);
    expect(w.balance.toString()).toBe('100.00');
  });
});

describe('validações contra a wallet', () => {
  test('conflito de moeda', () => {
    const w = wallet();
    expect(decide(tx(K.Bet, '1.00', { currency: 'USD' }), w)).toEqual({ type: 'rejected', code: FailureCode.CurrencyMismatch });
  });
  test('wallet de outro player', () => {
    expect(decide(tx(K.Bet, '1.00', { player: 'p2' }), wallet())).toEqual({
      type: 'rejected',
      code: FailureCode.WalletPlayerMismatch,
    });
  });
  test('wallet inexistente', () => {
    expect(decide(tx(K.Bet, '1.00'), null)).toEqual({ type: 'rejected', code: FailureCode.WalletNotFound });
  });
});

describe('WagerTransaction', () => {
  test('OPENING não pode ser submetido', () => {
    expect(() =>
      WagerTransaction.create({
        id: 'x',
        providerId: 'p',
        externalTransactionId: 'e',
        idempotencyKey: 'k',
        payloadHash: 'h',
        walletId: 'w',
        playerId: 'p',
        roundId: 'r',
        gameId: 'g',
        kind: K.Opening,
        money: brl('1.00'),
        createdAt: now,
      }),
    ).toThrow(InvalidWagerRequestError);
  });

  test('estados terminais não transicionam', () => {
    const w = wallet();
    const bet = processedBet(w);
    expect(bet.isTerminal()).toBe(true);
    expect(() => bet.reject(FailureCode.InsufficientFunds, now)).toThrow(InvalidTransactionStateError);
    expect(() => bet.markProcessed(undefined, now, w.balance)).toThrow(InvalidTransactionStateError);
    expect(() => bet.markPendingReference(now, PENDING_REFERENCE_POLICY)).toThrow(InvalidTransactionStateError);
    expect(() => bet.fail(FailureCode.ProcessingFailed, now)).toThrow(InvalidTransactionStateError);
  });

  test('BET com valor zero é inválida', () => {
    expect(() => tx(K.Bet, '0.00')).toThrow(InvalidWagerRequestError);
  });
});

describe('idempotência: hash de payload', () => {
  const payload = { providerId: 'a', kind: 'BET', money: { amount: '25.00', currency: 'BRL' } };

  test('independe da ordem das chaves', () => {
    const reordered = { money: { currency: 'BRL', amount: '25.00' }, kind: 'BET', providerId: 'a' };
    expect(sha256Canonical(payload)).toBe(sha256Canonical(reordered));
  });

  test('payload divergente com mesma key é detectado', () => {
    const t = tx(K.Bet, '25.00');
    const stored = WagerTransaction.rehydrate({ ...snapshot(t), payloadHash: sha256Canonical(payload) });
    expect(stored.matchesPayload(sha256Canonical(payload))).toBe(true);
    expect(stored.matchesPayload(sha256Canonical({ ...payload, money: { amount: '26.00', currency: 'BRL' } }))).toBe(false);
  });
});

function snapshot(t: WagerTransaction) {
  return {
    id: t.id,
    providerId: t.providerId,
    externalTransactionId: t.externalTransactionId,
    idempotencyKey: t.idempotencyKey,
    payloadHash: t.payloadHash,
    walletId: t.walletId,
    playerId: t.playerId,
    roundId: t.roundId,
    gameId: t.gameId,
    kind: t.kind,
    money: t.money,
    referenceExternalTransactionId: t.referenceExternalTransactionId,
    createdAt: t.createdAt,
    status: t.status,
    referenceAttempts: t.referenceAttempts,
  };
}
