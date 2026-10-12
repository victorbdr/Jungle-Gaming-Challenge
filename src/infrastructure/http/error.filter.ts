import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException, Inject } from '@nestjs/common';
import type { Request, Response } from 'express';
import {
  ConcurrentWriteError,
  ConflictError,
  IdempotencyConflictError,
  NotFoundError,
  TransientError,
  ValidationError,
} from '../../application/errors';
import { type AppLogger, LOGGER } from '../../application/ports';
import { DomainError } from '../../shared/domain/errors';

interface ErrorBody {
  error: { code: string; message: string; retryable: boolean; details?: unknown };
}

/**
 * Mapeamento consistente erro → HTTP, para o provedor decidir sem interpretar mensagens:
 * 400 payload inválido · 404 inexistente · 409 conflito (idempotência/estado) · 503 transitório (reenviar) · 500 inesperado.
 * Rejeição de negócio não passa por aqui: é resposta 422 com a transação REJECTED.
 */
@Catch()
export class ApplicationErrorFilter implements ExceptionFilter {
  constructor(@Inject(LOGGER) private readonly logger: AppLogger) {}

  catch(err: unknown, host: ArgumentsHost): void {
    const res = host.switchToHttp().getResponse<Response>();
    const req = host.switchToHttp().getRequest<Request>();
    const [status, body] = this.map(err);
    if (status >= 500) {
      this.logger.error(
        { correlationId: req.headers['x-correlation-id'], path: req.path, code: body.error.code, err: (err as Error)?.message },
        'request failed',
      );
    }
    if (status === 503) res.setHeader('Retry-After', '1');
    res.status(status).json(body);
  }

  private map(err: unknown): [number, ErrorBody] {
    const body = (code: string, message: string, retryable: boolean, details?: unknown): ErrorBody => ({
      error: { code, message, retryable, ...(details !== undefined ? { details } : {}) },
    });
    if (err instanceof ValidationError) return [400, body(err.code, err.message, false, err.details)];
    if (err instanceof DomainError) return [400, body(err.code, err.message, false)];
    if (err instanceof IdempotencyConflictError)
      return [409, body(err.code, err.message, false, { existingTransactionId: err.existingTransactionId })];
    if (err instanceof ConflictError) return [409, body(err.code, err.message, false)];
    if (err instanceof NotFoundError) return [404, body(err.code, err.message, false)];
    if (err instanceof TransientError) return [503, body(err.code, err.message, true, { reason: err.reason })];
    if (err instanceof ConcurrentWriteError) return [503, body('TRANSIENT_FAILURE', err.message, true, { reason: err.reason })];
    if (err instanceof HttpException) {
      const status = err.getStatus();
      const code = status === 400 ? 'VALIDATION_FAILED' : status === 401 ? 'UNAUTHENTICATED' : status === 404 ? 'NOT_FOUND' : 'HTTP_ERROR';
      return [status, body(code, err.message, status >= 500)];
    }
    return [500, body('INTERNAL_ERROR', 'Unexpected error', false)];
  }
}
