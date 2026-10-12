import { z } from 'zod';
import { WagerTransactionProcessed, WalletBalanceChanged } from '../messaging/domain/events';
import { sha256Canonical } from '../shared/domain/canonical-hash';
import { newId } from '../shared/domain/ids';
import { Money } from '../shared/domain/money';
import { WagerTransaction } from '../wagering/domain/wager-transaction';
import { Wallet } from '../wallet/domain/wallet';
import { ConcurrentWriteError, ConflictError, ValidationError } from './errors';
import type { AppLogger, Clock, Metrics, TransactionRunner } from './ports';
import { moneySchema } from './wager-command';

export const createWalletSchema = z
  .object({
    playerId: z.string().trim().min(1).max(128),
    initialBalance: moneySchema,
  })
  .strict();

/** Provider interno usado nas transações OPENING. */
export const INTERNAL_PROVIDER = 'internal';

export class CreateWalletUseCase {
  constructor(
    private readonly runner: TransactionRunner,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(raw: unknown, ctx: { correlationId: string }): Promise<Wallet> {
    const parsed = createWalletSchema.safeParse(raw);
    if (!parsed.success) throw new ValidationError('Invalid wallet payload', z.treeifyError(parsed.error));
    let initialBalance: Money;
    try {
      initialBalance = Money.from(parsed.data.initialBalance);
    } catch (err) {
      throw new ValidationError((err as Error).message);
    }
    const playerId = parsed.data.playerId;

    try {
      const wallet = await this.runner.run(async (uow) => {
        const now = this.clock.now();
        const walletId = newId();
        const openingId = newId();
        const { wallet, openingEntry } = Wallet.open({ id: walletId, playerId, initialBalance, at: now, openingTransactionId: openingId });
        // Wallet, OPENING e lançamento na mesma transação; o constraint trigger saldo↔ledger valida no commit.
        await uow.insertWallet(wallet);
        if (openingEntry) {
          const externalId = `opening:${walletId}`;
          const opening = WagerTransaction.opening({
            id: openingId,
            providerId: INTERNAL_PROVIDER,
            externalTransactionId: externalId,
            idempotencyKey: `${INTERNAL_PROVIDER}:${externalId}`,
            payloadHash: sha256Canonical({ playerId, initialBalance: initialBalance.toJSON() }),
            walletId,
            playerId,
            roundId: 'opening',
            gameId: 'internal',
            money: initialBalance,
            createdAt: now,
          });
          opening.markProcessed(undefined, now, wallet.balance);
          await uow.insertTransaction(opening);
          await uow.insertLedgerEntry(openingEntry);
          const eventCtx = { correlationId: ctx.correlationId, occurredAt: now };
          await uow.enqueue([WagerTransactionProcessed.from(opening, eventCtx), WalletBalanceChanged.from(wallet, openingEntry, eventCtx)]);
          this.metrics.transactionFinalized(opening.status, opening.kind, 'http');
        }
        return wallet;
      });
      this.logger.info({ walletId: wallet.id }, 'wallet created');
      return wallet;
    } catch (err) {
      if (err instanceof ConcurrentWriteError && err.reason === 'unique_race' && err.constraint === 'wallets_player_currency_uq') {
        throw new ConflictError('WALLET_ALREADY_EXISTS', `Wallet for player and currency ${initialBalance.currency} already exists`);
      }
      throw err;
    }
  }
}
