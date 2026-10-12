import type { Money } from '../shared/domain/money';
import { NotFoundError } from './errors';
import type { AppLogger, Metrics, WalletQueries } from './ports';

export interface ReconciliationReport {
  walletId: string;
  storedBalance: Money;
  calculatedBalance: Money;
  difference: Money;
  consistent: boolean;
  checkedEntries: number;
}

/** Compara saldo materializado com o saldo reconstruído pelo ledger. Nunca corrige: só reporta. */
export class ReconcileWalletUseCase {
  constructor(
    private readonly queries: WalletQueries,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(walletId: string): Promise<ReconciliationReport> {
    const snapshot = await this.queries.reconciliationSnapshot(walletId);
    if (!snapshot) throw new NotFoundError(`Wallet ${walletId} not found`);
    const difference = snapshot.storedBalance.subtract(snapshot.calculatedBalance);
    const consistent = difference.isZero();
    if (!consistent) {
      this.metrics.reconciliationMismatch();
      this.logger.error(
        { walletId, difference: difference.toString(), checkedEntries: snapshot.checkedEntries },
        'wallet reconciliation mismatch',
      );
    }
    return { walletId, ...snapshot, difference, consistent };
  }
}
