import type { EventContext } from '../messaging/domain/integration-event';
import { FailureCode } from '../wagering/domain/failure-code';
import { TransientError } from './errors';
import { applyDecision } from './process-wager.use-case';
import type { AppLogger, Clock, Metrics, TransactionRunner, WalletQueries } from './ports';
import { withConcurrencyRetry } from './retry';

/**
 * Reprocessa transações PENDING_REFERENCE vencidas. Seguro com várias instâncias:
 * a ordem de locks é a mesma do fluxo principal (wallet → transação), e a linha da
 * transação é travada com SKIP LOCKED e re-checada (status/vencimento) sob o lock.
 */
export class ResolvePendingReferencesUseCase {
  constructor(
    private readonly runner: TransactionRunner,
    private readonly queries: WalletQueries,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async runOnce(batchSize = 50): Promise<{ resolved: number; stillPending: number; rejected: number }> {
    const candidates = await this.queries.findDuePendingReferenceIds(this.clock.now(), batchSize);
    const stats = { resolved: 0, stillPending: 0, rejected: 0 };
    for (const candidate of candidates) {
      try {
        const outcome = await withConcurrencyRetry('resolve_pending_reference', this.metrics, () =>
          this.runner.run(async (uow) => {
            const wallet = await uow.lockWallet(candidate.walletId);
            const tx = await uow.lockPendingTransaction(candidate.id);
            if (!tx) return null; // outro worker já resolveu, ou ainda não venceu
            const now = this.clock.now();
            const eventCtx: EventContext = { correlationId: tx.id, causationId: tx.id, occurredAt: now };
            const result = await applyDecision(uow, tx, wallet, now, eventCtx, 'update');
            return { result, tx };
          }),
        );
        if (!outcome) continue;
        const { result, tx } = outcome;
        if (result.type === 'pending_reference') {
          stats.stillPending++;
          this.metrics.retry('pending_reference');
        } else {
          if (result.type === 'processed') stats.resolved++;
          else stats.rejected++;
          this.metrics.transactionFinalized(tx.status, tx.kind, 'worker');
        }
        this.logger.info(
          { transactionId: tx.id, walletId: tx.walletId, providerId: tx.providerId, status: tx.status, failureCode: tx.failureCode, attempts: tx.referenceAttempts },
          'pending reference re-evaluated',
        );
      } catch (err) {
        this.logger.error({ transactionId: candidate.id, err: (err as Error).message }, 'pending reference re-evaluation failed');
        if (!(err instanceof TransientError)) {
          // Erro permanente (ex.: violação de constraint/trigger): congela como FAILED para auditoria
          // em vez de reprocessar indefinidamente.
          await this.markFailed(candidate.id).catch((e: Error) =>
            this.logger.error({ transactionId: candidate.id, err: e.message }, 'could not mark transaction as FAILED'),
          );
        }
      }
    }
    return stats;
  }

  private async markFailed(transactionId: string): Promise<void> {
    await this.runner.run(async (uow) => {
      const tx = await uow.lockPendingTransaction(transactionId);
      if (!tx) return;
      tx.fail(FailureCode.ProcessingFailed, this.clock.now());
      await uow.updateTransaction(tx);
      this.metrics.transactionFinalized(tx.status, tx.kind, 'worker');
    });
  }
}
