import { z } from 'zod';
import { sha256Canonical } from '../shared/domain/canonical-hash';
import { Money, type MoneyProps } from '../shared/domain/money';
import { SUBMITTABLE_KINDS, type WagerTransactionKind } from '../wagering/domain/wager-transaction';
import { ValidationError } from './errors';

const id = (max: number) => z.string().trim().min(1).max(max);

export const moneySchema = z.object({ amount: z.string(), currency: z.string() }).strict();

/** Contrato de entrada (HTTP body e `data` da mensagem SQS, sem a idempotencyKey). */
export const wagerRequestSchema = z
  .object({
    providerId: id(128),
    externalTransactionId: id(256),
    playerId: id(128),
    walletId: z.uuid(),
    roundId: id(256),
    gameId: id(256),
    kind: z.enum(SUBMITTABLE_KINDS),
    money: moneySchema,
    referenceExternalTransactionId: id(256).optional(),
  })
  .strict();

export type WagerRequest = z.infer<typeof wagerRequestSchema>;

export interface WagerCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId: string | undefined;
  payloadHash: string;
}

/**
 * Valida e normaliza a requisição. O `payloadHash` é o sha256 do JSON canônico dos campos de negócio
 * (sem idempotencyKey nem metadados de transporte), com o amount já normalizado para 2 casas.
 */
export const toWagerCommand = (raw: unknown, idempotencyKey: string | undefined): WagerCommand => {
  if (!idempotencyKey || idempotencyKey.trim() === '' || idempotencyKey.length > 512) {
    throw new ValidationError('Idempotency-Key is required (1..512 chars)');
  }
  const parsed = wagerRequestSchema.safeParse(raw);
  if (!parsed.success) {
    throw new ValidationError('Invalid wager transaction payload', z.treeifyError(parsed.error));
  }
  const req = parsed.data;
  let money: Money;
  try {
    money = Money.from(req.money as MoneyProps);
  } catch (err) {
    throw new ValidationError((err as Error).message);
  }
  const business = {
    providerId: req.providerId,
    externalTransactionId: req.externalTransactionId,
    playerId: req.playerId,
    walletId: req.walletId.toLowerCase(),
    roundId: req.roundId,
    gameId: req.gameId,
    kind: req.kind,
    money: money.toJSON(),
    referenceExternalTransactionId: req.referenceExternalTransactionId,
  };
  return {
    ...business,
    idempotencyKey: idempotencyKey.trim(),
    money,
    payloadHash: sha256Canonical(business),
  };
};
