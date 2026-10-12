import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import type { Container } from '../../src/bootstrap/container';
import { toWagerCommand } from '../../src/application/wager-command';

let counter = 0;
export const uniq = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${(++counter).toString(36)}-${randomUUID().slice(0, 8)}`;

export const createWallet = async (c: Container, amount = '100.00', currency = 'BRL') => {
  const wallet = await c.createWallet.execute(
    { playerId: randomUUID(), initialBalance: { amount, currency } },
    { correlationId: uniq('corr') },
  );
  return wallet;
};

export interface WagerInput {
  walletId: string;
  playerId: string;
  kind: 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
  amount: string;
  externalTransactionId?: string;
  referenceExternalTransactionId?: string;
  roundId?: string;
  providerId?: string;
  currency?: string;
}

export const wagerBody = (i: WagerInput) => ({
  providerId: i.providerId ?? 'provider-a',
  externalTransactionId: i.externalTransactionId ?? uniq('ext'),
  playerId: i.playerId,
  walletId: i.walletId,
  roundId: i.roundId ?? 'round-1',
  gameId: 'fortune-chimp',
  kind: i.kind,
  money: { amount: i.amount, currency: i.currency ?? 'BRL' },
  ...(i.referenceExternalTransactionId ? { referenceExternalTransactionId: i.referenceExternalTransactionId } : {}),
});

export const submit = (c: Container, i: WagerInput, source: 'http' | 'sqs' = 'http') => {
  const body = wagerBody(i);
  return c.processWager.execute(toWagerCommand(body, `${body.providerId}:${body.externalTransactionId}`), {
    correlationId: uniq('corr'),
    source,
  });
};

/** Consulta SQL direta, sem passar pela aplicação. */
export const sql = async <T = Record<string, unknown>>(url: string, query: string, params: unknown[] = []): Promise<T[]> => {
  const client = new Client({ connectionString: url });
  await client.connect();
  try {
    return (await client.query(query, params)).rows as T[];
  } finally {
    await client.end();
  }
};

/** Invariante final: saldo == saldo reconstruído pelo ledger, para TODAS as wallets. */
export const assertAllWalletsConsistent = async (url: string) => {
  const rows = await sql<{ id: string; balance: string; calculated: string }>(
    url,
    `select w.id, w.balance::text,
            coalesce(sum(case l.direction when 'CREDIT' then l.amount else -l.amount end), 0)::numeric(20,2)::text as calculated
     from wallets w left join wallet_ledger_entries l on l.wallet_id = w.id group by w.id`,
  );
  const broken = rows.filter((r) => r.balance !== r.calculated);
  if (broken.length > 0) throw new Error(`Inconsistent wallets: ${JSON.stringify(broken)}`);
  return rows.length;
};
