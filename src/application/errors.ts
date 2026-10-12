/**
 * Erros de aplicação, classificados pelo que o chamador deve fazer:
 * - ValidationError: corrigir o payload (não reenviar igual);
 * - IdempotencyConflictError / ConflictError: conflito de estado (não reenviar igual);
 * - NotFoundError: recurso inexistente;
 * - TransientError: reenviar mais tarde (backoff).
 * Rejeições de negócio NÃO são erros: são transações REJECTED com failureCode.
 */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;
  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class ValidationError extends ApplicationError {
  readonly code = 'VALIDATION_FAILED';
  constructor(
    message: string,
    readonly details?: unknown,
  ) {
    super(message);
  }
}

export class IdempotencyConflictError extends ApplicationError {
  readonly code = 'IDEMPOTENCY_CONFLICT';
  constructor(
    readonly idempotencyKey: string,
    readonly existingTransactionId: string,
  ) {
    super(`Idempotency key "${idempotencyKey}" was already used with a different payload`);
  }
}

export class ConflictError extends ApplicationError {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

export class NotFoundError extends ApplicationError {
  readonly code = 'NOT_FOUND';
  constructor(message: string) {
    super(message);
  }
}

/** Corrida detectada pelo banco (unique, deadlock, lock timeout, versão). Seguro repetir a operação inteira. */
export class ConcurrentWriteError extends ApplicationError {
  readonly code = 'CONCURRENT_WRITE';
  constructor(
    readonly reason: 'lock_timeout' | 'deadlock' | 'serialization' | 'version_mismatch' | 'unique_race',
    readonly constraint?: string,
    override readonly cause?: unknown,
  ) {
    super(`Concurrent write detected (${reason}${constraint ? `: ${constraint}` : ''})`);
  }
}

export class TransientError extends ApplicationError {
  readonly code = 'TRANSIENT_FAILURE';
  constructor(
    message: string,
    readonly reason: string,
    override readonly cause?: unknown,
  ) {
    super(message);
  }
}
