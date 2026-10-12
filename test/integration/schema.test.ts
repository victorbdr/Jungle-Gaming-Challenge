import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Client } from 'pg';
import { createContainer, type Container } from '../../src/bootstrap/container';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { recreateDatabase } from '../support/env';
import { createWallet, sql, submit } from '../support/fixtures';
import { silentLogger } from '../support/logger';

/**
 * As garantias da seção 6 precisam valer no BANCO: aqui escrevemos SQL direto,
 * contornando a aplicação, e esperamos que o schema recuse.
 */
let url: string;
let c: Container;

beforeAll(async () => {
  url = await recreateDatabase('wagering_it_schema');
  await runMigrations('up', url);
  c = await createContainer({ databaseUrl: url, logger: silentLogger() });
});
afterAll(async () => {
  await c.close();
});

const expectSqlError = async (query: string, params: unknown[], pattern: RegExp) => {
  const client = new Client({ connectionString: url });
  await client.connect();
  try {
    await client.query('begin');
    await client.query(query, params);
    await client.query('commit'); // constraint triggers DEFERRED disparam aqui
    throw new Error(`expected failure for: ${query}`);
  } catch (err) {
    expect((err as Error).message).toMatch(pattern);
  } finally {
    await client.query('rollback').catch(() => undefined);
    await client.end();
  }
};

describe('migrations', () => {
  test('são reversíveis (down → up)', async () => {
    const tmp = await recreateDatabase('wagering_it_schema_rev');
    expect(await runMigrations('up', tmp)).toEqual(['Migration20261010000000_initial']);
    expect(await runMigrations('down', tmp, true)).toEqual(['Migration20261010000000_initial']);
    const tables = await sql(tmp, `select table_name from information_schema.tables where table_schema='public' and table_name <> 'mikro_orm_migrations'`);
    expect(tables).toEqual([]);
    expect(await runMigrations('up', tmp)).toEqual(['Migration20261010000000_initial']);
  });
});

describe('constraints', () => {
  test('saldo negativo é recusado', async () => {
    const w = await createWallet(c, '10.00');
    await expectSqlError(`update wallets set balance = -1 where id = $1`, [w.id], /check constraint|wallets_balance_check/);
  });

  test('saldo que diverge do ledger é recusado no commit', async () => {
    const w = await createWallet(c, '10.00');
    await expectSqlError(`update wallets set balance = 999 where id = $1`, [w.id], /does not match ledger/);
  });

  test('uma wallet por player+moeda', async () => {
    const w = await createWallet(c, '0.00');
    await expectSqlError(
      `insert into wallets (id, player_id, currency, balance, version, created_at, updated_at) values (gen_random_uuid(), $1, 'BRL', 0, 1, now(), now())`,
      [w.playerId],
      /wallets_player_currency_uq/,
    );
  });

  test('ledger é append-only: UPDATE, DELETE e TRUNCATE bloqueados', async () => {
    const w = await createWallet(c, '10.00');
    await expectSqlError(`update wallet_ledger_entries set amount = 1 where wallet_id = $1`, [w.id], /append-only/);
    await expectSqlError(`delete from wallet_ledger_entries where wallet_id = $1`, [w.id], /append-only/);
    await expectSqlError(`truncate wallet_ledger_entries cascade`, [], /append-only/);
  });

  test('lançamento desbalanceado ou fora da cadeia é recusado', async () => {
    const w = await createWallet(c, '10.00');
    const [opening] = await sql<{ id: string }>(url, `select id from wager_transactions where wallet_id = $1`, [w.id]);
    await expectSqlError(
      `insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
       values (gen_random_uuid(), $1, $2, 'CREDIT', 5, 'BRL', 10, 14, now())`,
      [w.id, opening!.id],
      /ledger_balanced_ck/,
    );
    await expectSqlError(
      `insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
       values (gen_random_uuid(), $1, $2, 'CREDIT', 5, 'BRL', 0, 5, now())`,
      [w.id, opening!.id],
      /ledger chain broken|ledger_tx_wallet_uq/,
    );
  });

  test('transação terminal é imutável e não pode ser excluída', async () => {
    const w = await createWallet(c, '100.00');
    const r = await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' });
    if (r.type !== 'result') throw new Error('unexpected');
    await expectSqlError(`update wager_transactions set status = 'REJECTED', failure_code = 'X' where id = $1`, [r.transaction.id], /is terminal/);
    await expectSqlError(`delete from wager_transactions where id = $1`, [r.transaction.id], /cannot be deleted/);
  });

  test('idempotency key e (provider, externalId) são únicos', async () => {
    const w = await createWallet(c, '100.00');
    const r = await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '1.00' });
    if (r.type !== 'result') throw new Error('unexpected');
    await expectSqlError(
      `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, status, created_at)
       select gen_random_uuid(), provider_id, external_transaction_id || '-x', idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, 'PENDING', now()
       from wager_transactions where id = $1`,
      [r.transaction.id],
      /wager_tx_idempotency_key_uq/,
    );
  });

  test('uma referência só pode ser revertida uma vez (índice único parcial)', async () => {
    const w = await createWallet(c, '100.00');
    const bet = await submit(c, { walletId: w.id, playerId: w.playerId, kind: 'BET', amount: '10.00' });
    if (bet.type !== 'result') throw new Error('unexpected');
    const refund = await submit(c, {
      walletId: w.id,
      playerId: w.playerId,
      kind: 'REFUND',
      amount: '10.00',
      referenceExternalTransactionId: bet.transaction.externalTransactionId,
    });
    if (refund.type !== 'result') throw new Error('unexpected');
    expect(refund.transaction.status).toBe('PROCESSED' as never);
    await expectSqlError(
      `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency,
         reference_external_transaction_id, reference_transaction_id, status, processed_at, created_at)
       select gen_random_uuid(), provider_id, 'dup-rb', 'dup-rb', payload_hash, wallet_id, player_id, round_id, game_id, 'ROLLBACK', amount, currency,
         reference_external_transaction_id, reference_transaction_id, 'PROCESSED', now(), now()
       from wager_transactions where id = $1`,
      [refund.transaction.id],
      /wager_tx_single_reversal_uq/,
    );
  });

  test('REFUND sem referência é recusado pelo schema', async () => {
    await expectSqlError(
      `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount, currency, status, created_at)
       values (gen_random_uuid(), 'p', 'e-ref', 'k-ref', repeat('a', 64), gen_random_uuid(), 'pl', 'r', 'g', 'REFUND', 1, 'BRL', 'PENDING', now())`,
      [],
      /wager_tx_reference_required_ck/,
    );
  });
});
