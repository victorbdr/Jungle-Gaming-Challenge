import { describe, expect, test } from 'bun:test';
import { WalletBalanceChanged } from '../../src/messaging/domain/events';
import { InboxMessage } from '../../src/messaging/domain/inbox-message';
import { OutboxMessage } from '../../src/messaging/domain/outbox-message';
import { Money } from '../../src/shared/domain/money';
import { Wallet } from '../../src/wallet/domain/wallet';

const now = new Date('2026-01-01T00:00:00Z');

describe('IntegrationEvent / Outbox', () => {
  const { wallet } = Wallet.open({
    id: 'w1',
    playerId: 'p1',
    initialBalance: Money.from({ amount: '10.00', currency: 'BRL' }),
    at: now,
    openingTransactionId: 'o',
  });
  const entry = wallet.credit(Money.from({ amount: '5.00', currency: 'BRL' }), 'tx1', now);
  const event = WalletBalanceChanged.from(wallet, entry, { correlationId: 'c1', occurredAt: now });

  test('envelope com eventType/version no tipo e Money como string', () => {
    const json = event.toJSON();
    expect(json.eventType).toBe('WalletBalanceChanged');
    expect(json.version).toBe(1);
    expect(json.occurredAt).toBe('2026-01-01T00:00:00.000Z');
    expect(json.data.balanceAfter).toEqual({ amount: '15.00', currency: 'BRL' });
    expect(json.data.walletVersion).toBe(2);
  });

  test('outbox: id == eventId, backoff e publicação', () => {
    const msg = OutboxMessage.enqueue(event);
    expect(msg.id).toBe(event.eventId);
    expect(msg.isDue(now)).toBe(true);
    msg.scheduleRetry(now, 'boom', undefined, () => 1);
    expect(msg.attempts).toBe(1);
    expect(msg.isDue(now)).toBe(false);
    expect(msg.isDue(new Date(now.getTime() + 500))).toBe(true);
    msg.scheduleRetry(now, 'boom', undefined, () => 1);
    expect(msg.nextAttemptAt!.getTime() - now.getTime()).toBe(1000);
    msg.markPublished(now);
    expect(msg.isPending()).toBe(false);
    expect(() => msg.markPublished(now)).toThrow();
  });
});

describe('InboxMessage', () => {
  test('marca processada uma vez', () => {
    const m = InboxMessage.receive({ messageId: 'm1', consumerName: 'c', payloadHash: 'h', receivedAt: now });
    expect(m.isProcessed()).toBe(false);
    m.markProcessed(now);
    expect(m.isProcessed()).toBe(true);
    expect(() => m.markProcessed(now)).toThrow();
  });
});
