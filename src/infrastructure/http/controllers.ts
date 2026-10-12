import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Res } from '@nestjs/common';
import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { Response } from 'express';
import { z } from 'zod';
import { NotFoundError, ValidationError } from '../../application/errors';
import { toWagerCommand } from '../../application/wager-command';
import type { Container } from '../../bootstrap/container';
import { newId } from '../../shared/domain/ids';
import { resolveQueueUrl, type SqsConfig } from '../sqs/sqs-config';
import { Public } from './auth';
import {
  httpStatusFor,
  presentLedgerEntry,
  presentReconciliation,
  presentSubmission,
  presentTransaction,
  presentWallet,
} from './presenters';

export const CONTAINER = Symbol('Container');
export const SQS_CLIENT = Symbol('SqsClient');
export const SQS_CONFIG = Symbol('SqsConfig');

const uuid = z.uuid();
const parseUuid = (value: string, what: string): string => {
  const r = uuid.safeParse(value);
  if (!r.success) throw new ValidationError(`Invalid ${what}`);
  return r.data.toLowerCase();
};

const correlationIdOf = (header: string | undefined) => (header && header.length <= 128 ? header : newId());

/** Cursor opaco e estável: base64url de {walletId, seq}. `seq` é a identity do ledger (monotônica, append-only). */
const encodeCursor = (walletId: string, seq: string) => Buffer.from(JSON.stringify({ w: walletId, s: seq })).toString('base64url');
const decodeCursor = (walletId: string, cursor: string | undefined): string | null => {
  if (!cursor) return null;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { w?: unknown; s?: unknown };
    if (parsed.w !== walletId || typeof parsed.s !== 'string' || !/^\d+$/.test(parsed.s)) throw new Error('mismatch');
    return parsed.s;
  } catch {
    throw new ValidationError('Invalid cursor');
  }
};

@Controller('wallets')
export class WalletsController {
  constructor(@Inject(CONTAINER) private readonly c: Container) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown, @Headers('x-correlation-id') correlationId?: string) {
    const wallet = await this.c.createWallet.execute(body, { correlationId: correlationIdOf(correlationId) });
    const { id, playerId, balance, version } = presentWallet(wallet);
    return { id, playerId, balance, version };
  }

  @Get(':walletId')
  async get(@Param('walletId') walletId: string) {
    const wallet = await this.c.queries.findWallet(parseUuid(walletId, 'walletId'));
    if (!wallet) throw new NotFoundError(`Wallet ${walletId} not found`);
    return presentWallet(wallet);
  }

  @Get(':walletId/ledger')
  async ledger(@Param('walletId') rawWalletId: string, @Query('cursor') cursor?: string, @Query('limit') rawLimit?: string) {
    const walletId = parseUuid(rawWalletId, 'walletId');
    const limit = rawLimit === undefined ? 50 : Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) throw new ValidationError('limit must be an integer between 1 and 200');
    const wallet = await this.c.queries.findWallet(walletId);
    if (!wallet) throw new NotFoundError(`Wallet ${walletId} not found`);
    const page = await this.c.queries.listLedger(walletId, decodeCursor(walletId, cursor), limit);
    return {
      walletId,
      entries: page.entries.map(({ entry }) => presentLedgerEntry(entry)),
      nextCursor: page.nextCursor ? encodeCursor(walletId, page.nextCursor) : null,
    };
  }

  @Post(':walletId/reconciliation')
  @HttpCode(200)
  async reconcile(@Param('walletId') walletId: string) {
    return presentReconciliation(await this.c.reconcileWallet.execute(parseUuid(walletId, 'walletId')));
  }
}

@Controller()
export class WageringController {
  constructor(@Inject(CONTAINER) private readonly c: Container) {}

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Headers('x-correlation-id') correlationId: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ) {
    const command = toWagerCommand(body, idempotencyKey);
    const result = await this.c.processWager.execute(command, { correlationId: correlationIdOf(correlationId), source: 'http' });
    if (result.type !== 'result') throw new Error('unexpected duplicate_message result on HTTP');
    res.status(httpStatusFor(result.transaction));
    return presentSubmission(result.transaction, result.idempotentReplay);
  }

  @Get('wagering/transactions/:transactionId')
  async get(@Param('transactionId') transactionId: string) {
    const tx = await this.c.queries.findTransaction(parseUuid(transactionId, 'transactionId'));
    if (!tx) throw new NotFoundError(`Transaction ${transactionId} not found`);
    return presentTransaction(tx);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getByExternal(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string) {
    const tx = await this.c.queries.findTransactionByExternalId(providerId, externalTransactionId);
    if (!tx) throw new NotFoundError(`Transaction ${providerId}/${externalTransactionId} not found`);
    return presentTransaction(tx);
  }
}

@Controller()
@Public()
export class HealthController {
  constructor(
    @Inject(CONTAINER) private readonly c: Container,
    @Inject(SQS_CLIENT) private readonly sqs: SQSClient,
    @Inject(SQS_CONFIG) private readonly sqsConfig: SqsConfig,
  ) {}

  @Get('health/live')
  live() {
    return { status: 'ok' };
  }

  @Get('health/ready')
  async ready(@Res({ passthrough: true }) res: Response) {
    const withTimeout = <T>(p: Promise<T>) =>
      Promise.race([p, new Promise<never>((_, reject) => setTimeout(() => reject(new Error('timeout')), 2000))]);
    const check = async (fn: () => Promise<unknown>) => {
      try {
        await withTimeout(fn());
        return 'up' as const;
      } catch {
        return 'down' as const;
      }
    };
    const [postgres, sqs] = await Promise.all([
      check(() => this.c.orm.em.fork().execute('select 1')),
      check(async () => {
        const url = await resolveQueueUrl(this.sqs, this.sqsConfig.wagerQueue);
        await this.sqs.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ['QueueArn'] }));
      }),
    ]);
    const ok = postgres === 'up' && sqs === 'up';
    res.status(ok ? 200 : 503);
    return { status: ok ? 'ready' : 'not_ready', checks: { postgres, sqs } };
  }

  @Get('metrics')
  async metrics(@Res() res: Response) {
    res.setHeader('Content-Type', this.c.metrics.registry.contentType);
    res.send(await this.c.metrics.registry.metrics());
  }
}
