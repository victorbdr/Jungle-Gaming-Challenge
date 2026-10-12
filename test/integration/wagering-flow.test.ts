import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { IdempotencyConflictError } from '../../src/application/errors';
import { toWagerCommand } from '../../src/application/wager-command';
import { createContainer, type Container } from '../../src/bootstrap/container';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { recreateDatabase } from '../support/env';
import { silentLogger } from '../support/logger';
import { assertAllWalletsConsistent, createWallet, sql, submit, uniq, wagerBody } from '../support/fixtures';

let c: Container;
let url: string;

beforeAll(async () => {
  url = await recreateDatabase('wagering_it_flow');
  await runMigrations('up', url);
  c = await createContainer({ databaseUrl: url, logger: silentLogger() });
});

afterAll(async () => {
  await assertAllWalletsConsistent(url);
  await c.close();
});

describe('wallet', () => {
  test('cria wallet com OPENING + CREDIT na mesma transação', async () => {
    const w = await createWallet(c, '1000.00');
    expect(w.version).toBe(1);
    const tx = await sql(url, `select kind, status from wager_transactions where wallet_id = $1`, [w.id]);
    expect(tx).toEqual([{ kind: 'OPENING', status: 'PROCESSED' }]);
    const ledger = await sql(url, `select direction, amount::text from wallet_ledger_entries where wallet_id = $1`, [w.id]);
    expect(ledger).toEqual([{ direction: 'CREDIT', amount: '1000.00' }]);
    const outbox = await sql(url, `select event_type from outbox_messages where aggregate_id in ($1, $2) order by event_type`, [
      w.id,
      (await sql<{ id: string }>(url, `select id from wager_transactions where wallet_id = $1`, [w.id]))[0]!.id,
    ]);
    expect(outbox.map((r) => r.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
  });

  test('wallet duplicada para player+moeda é conflito', async () => {
    const w = await createWallet(c, '0.00');
    await expect(
      c.createWallet.execute({ playerId: w.playerId, initialBalance: { amount: '5.00', currency: 'BRL' } }, { correlationId: 'x' }),
    ).rejects.toMatchObject({ code: 'WALLET_ALREADY_EXISTS' });
    // outra moeda é permitida
    await c.createWallet.execute({ playerId: w.playerId, initialBalance: { amount: '5.00', currency: 'USD' } }, { correlationId: 'x' });
  });
});

describe('idempotência', () => {
  test('replay devolve o resultado original, inclusive o saldo daquele momento', async () => {
    const w = await createWallet(c, '100.00');
    const body = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' });
    const key = `provider-a:${body.externalTransactionId}`;
    const first = await c.processWager.execute(toWagerCommand(body, key), { correlationId: 'a', source: 'http' });
    await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '5.00' });
    const replay = await c.processWager.execute(toWagerCommand(body, key), { correlationId: 'b', source: 'http' });
    if (first.type !== 'result' || replay.type !== 'result') throw new Error('unexpected');
    expect(replay.idempotentReplay).toBe(true);
    expect(replay.transaction.id).toBe(first.transaction.id);
    expect(replay.transaction.resultBalance?.toString()).toBe('90.00');
  });

  test('mesma key com payload diferente é conflito, não replay', async () => {
    const w = await createWallet(c, '100.00');
    const body = wagerBody({ walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' });
    const key = `provider-a:${body.externalTransactionId}`;
    await c.processWager.execute(toWagerCommand(body, key), { correlationId: 'a', source: 'http' });
    const changed = { ...body, money: { amount: '11.00', currency: 'BRL' } };
    await expect(c.processWager.execute(toWagerCommand(changed, key), { correlationId: 'b', source: 'http' })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
  });
});

describe('concorrência', () => {
  test('seção 8: saldo 100, duas apostas de 80 simultâneas', async () => {
    const w = await createWallet(c, '100.00');
    const results = await Promise.all([
      submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '80.00' }),
      submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '80.00' }),
    ]);
    const statuses = results.map((r) => (r.type === 'result' ? String(r.transaction.status) : 'dup')).sort();
    expect(statuses).toEqual(['PROCESSED', 'REJECTED']);
    const rejected = results.find((r) => r.type === 'result' && r.transaction.status === 'REJECTED');
    expect(rejected?.type === 'result' ? String(rejected.transaction.failureCode) : '').toBe('INSUFFICIENT_FUNDS');
    const [wallet] = await sql(url, `select balance::text from wallets where id = $1`, [w.id]);
    expect(wallet!.balance).toBe('20.00');
    const debits = await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [w.id]);
    expect(debits).toHaveLength(1);
  });

  test('a mesma aposta 50x em paralelo gera um único débito', async () => {
    const w = await createWallet(c, '100.00');
    const ext = uniq('same');
    const results = await Promise.all(
      Array.from({ length: 50 }, () => submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00', externalTransactionId: ext })),
    );
    const ids = new Set(results.map((r) => (r.type === 'result' ? r.transaction.id : '')));
    expect(ids.size).toBe(1);
    expect(results.filter((r) => r.type === 'result' && !r.idempotentReplay)).toHaveLength(1);
    const debits = await sql(url, `select 1 from wallet_ledger_entries where wallet_id = $1 and direction = 'DEBIT'`, [w.id]);
    expect(debits).toHaveLength(1);
    const [wallet] = await sql(url, `select balance::text, version from wallets where id = $1`, [w.id]);
    expect(wallet).toEqual({ balance: '90.00', version: 2 });
  });
});

