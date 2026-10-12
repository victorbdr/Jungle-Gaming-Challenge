import { ConcurrentWriteError, TransientError } from './errors';
import type { Metrics } from './ports';

/**
 * Repete a operação inteira (nova transação) quando o banco detecta uma corrida.
 * Limite baixo: com lock pessimista, corridas são raras (unique race de idempotência, deadlock).
 */
export const withConcurrencyRetry = async <T>(
  operation: string,
  metrics: Metrics,
  work: () => Promise<T>,
  maxAttempts = 4,
): Promise<T> => {
  for (let attempt = 1; ; attempt++) {
    try {
      return await work();
    } catch (err) {
      if (!(err instanceof ConcurrentWriteError)) throw err;
      metrics.lockConflict(err.reason);
      if (attempt >= maxAttempts) {
        throw new TransientError(`${operation}: concurrency retries exhausted`, err.reason, err);
      }
      metrics.retry(operation);
      await Bun.sleep(Math.round(5 * 2 ** attempt * Math.random()));
    }
  }
};
