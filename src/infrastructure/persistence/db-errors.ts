import { ConcurrentWriteError, TransientError } from '../../application/errors';

interface PgLikeError {
  code?: string;
  constraint?: string;
  message?: string;
  cause?: unknown;
}

const findPgError = (err: unknown): PgLikeError | undefined => {
  let current: unknown = err;
  for (let depth = 0; current && depth < 5; depth++) {
    const e = current as PgLikeError;
    if (typeof e.code === 'string' && /^[0-9A-Z]{5}$/.test(e.code)) return e;
    current = e.cause;
  }
  return undefined;
};

const constraintOf = (e: PgLikeError): string | undefined =>
  e.constraint ?? /constraint "([^"]+)"/.exec(e.message ?? '')?.[1] ?? /index "([^"]+)"/.exec(e.message ?? '')?.[1];

const TRANSIENT_NETWORK = /ECONNREFUSED|ECONNRESET|ETIMEDOUT|EPIPE|Connection terminated|connect ECONN|timeout exceeded when trying to connect|Client has encountered a connection error/i;

/**
 * Traduz erros do PostgreSQL para a taxonomia da aplicação:
 * - corridas (repetíveis pela própria operação) → ConcurrentWriteError;
 * - indisponibilidade/recursos → TransientError;
 * - o resto (CHECK/trigger, bugs) é propagado como está: erro permanente.
 */
export const translateDbError = (err: unknown): unknown => {
  if (err instanceof ConcurrentWriteError || err instanceof TransientError) return err;
  const pg = findPgError(err);
  if (pg?.code) {
    switch (pg.code) {
      case '23505':
        return new ConcurrentWriteError('unique_race', constraintOf(pg), err);
      case '40P01':
        return new ConcurrentWriteError('deadlock', undefined, err);
      case '40001':
        return new ConcurrentWriteError('serialization', undefined, err);
      case '55P03':
        return new ConcurrentWriteError('lock_timeout', undefined, err);
    }
    // 08xxx conexão, 53xxx recursos, 57P0x shutdown/cancel do servidor
    if (/^(08|53|57P)/.test(pg.code)) return new TransientError('Database unavailable', pg.code, err);
  }
  const message = (err as Error | undefined)?.message ?? '';
  if (TRANSIENT_NETWORK.test(message)) return new TransientError('Database unavailable', 'connection', err);
  return err;
};
