import pino from 'pino';
import type { AppLogger } from '../../application/ports';

/**
 * Logs JSON estruturados. Campos de correlação (correlationId, messageId, transactionId,
 * walletId, providerId) entram via `child`/objeto. Payloads e valores financeiros completos
 * são removidos por redaction como defesa em profundidade.
 */
export const createLogger = (bindings: Record<string, unknown> = {}): AppLogger =>
  pino({
    level: process.env.LOG_LEVEL ?? 'info',
    base: { service: 'wagering-processor', pid: process.pid, ...bindings },
    timestamp: pino.stdTimeFunctions.isoTime,
    messageKey: 'msg',
    formatters: { level: (label) => ({ level: label }) },
    redact: {
      paths: ['payload', 'body', 'data', 'money', 'balance', 'amount', '*.payload', '*.body', '*.money', '*.balance', 'headers.authorization'],
      remove: true,
    },
  }) as unknown as AppLogger;
