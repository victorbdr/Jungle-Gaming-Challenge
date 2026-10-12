/** Backoff exponencial com teto e jitter (injetável para testes determinísticos). */
export interface BackoffPolicy {
  readonly baseMs: number;
  readonly maxMs: number;
  readonly maxAttempts: number;
}

export const nextAttemptDelayMs = (policy: BackoffPolicy, attempt: number, jitter: () => number = Math.random): number => {
  const exp = Math.min(policy.maxMs, policy.baseMs * 2 ** Math.max(0, attempt - 1));
  // jitter na metade superior: evita rajadas sincronizadas entre instâncias.
  return Math.round(exp / 2 + (exp / 2) * jitter());
};

/**
 * Referência ausente: 10 avaliações (1 inicial + 9 reagendadas), base 1s, teto 5min.
 * Atrasos 1,2,4,…,256s com jitter ⇒ janela total entre ~4,3 e ~8,5 minutos antes de REJECTED.
 */
export const PENDING_REFERENCE_POLICY: BackoffPolicy = { baseMs: 1_000, maxMs: 300_000, maxAttempts: 10 };

/** Outbox: tenta indefinidamente (evento confirmado não pode ser perdido), com teto de 1min. */
export const OUTBOX_POLICY: BackoffPolicy = { baseMs: 500, maxMs: 60_000, maxAttempts: Number.POSITIVE_INFINITY };
