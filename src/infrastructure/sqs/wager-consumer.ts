import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { z } from 'zod';
import { ConcurrentWriteError, IdempotencyConflictError, TransientError, ValidationError } from '../../application/errors';
import type { AppLogger, Metrics } from '../../application/ports';
import type { ProcessWagerTransactionUseCase } from '../../application/process-wager.use-case';
import { toWagerCommand } from '../../application/wager-command';
import { sha256Canonical } from '../../shared/domain/canonical-hash';
import { DomainError } from '../../shared/domain/errors';
import { resolveQueueUrl, type SqsConfig } from './sqs-config';

export const WAGER_CONSUMER_NAME = 'wager-transactions-consumer';

const envelopeSchema = z.object({
  messageId: z.string().min(1).max(256),
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.string(),
  data: z.object({ idempotencyKey: z.string().min(1).max(512) }).passthrough(),
});

type Classification =
  | { kind: 'ack' }
  | { kind: 'retry'; reason: string }
  | { kind: 'dlq'; reason: string };

/**
 * Consumidor de wager-transactions.fifo.
 * - reutiliza o mesmo use case do HTTP; inbox persistente por (consumerName, messageId) na mesma transação;
 * - ack (DeleteMessage) somente após o commit;
 * - negócio → ack (a transação REJECTED é o resultado); transitório → visibilidade com backoff;
 *   permanente (payload inválido, conflito de idempotência) ou tentativas esgotadas → DLQ;
 * - SIGTERM: para de receber, conclui o que está em andamento e devolve a visibilidade do resto.
 */
export class WagerSqsConsumer {
  private running = false;
  private loop: Promise<void> | undefined;
  private abort: AbortController | undefined;
  private readonly inFlight = new Set<Promise<void>>();

  constructor(
    private readonly client: SQSClient,
    private readonly config: SqsConfig,
    private readonly useCase: ProcessWagerTransactionUseCase,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly options: { waitTimeSeconds?: number; visibilityTimeout?: number; batchSize?: number } = {},
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  /** Parada graciosa: aguarda mensagens em processamento (até `graceMs`). */
  async stop(graceMs = 20_000): Promise<void> {
    this.running = false;
    this.abort?.abort();
    await this.loop?.catch(() => undefined);
    await Promise.race([Promise.allSettled([...this.inFlight]), Bun.sleep(graceMs)]);
  }

  private async run(): Promise<void> {
    const queueUrl = await resolveQueueUrl(this.client, this.config.wagerQueue);
    while (this.running) {
      this.abort = new AbortController();
      let messages: Message[] = [];
      try {
        const res = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: this.options.batchSize ?? 10,
            WaitTimeSeconds: this.options.waitTimeSeconds ?? 5,
            VisibilityTimeout: this.options.visibilityTimeout ?? 30,
            MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
          }),
          { abortSignal: this.abort.signal },
        );
        messages = res.Messages ?? [];
      } catch (err) {
        if (!this.running) break;
        this.logger.warn({ err: (err as Error).message }, 'sqs receive failed');
        await Bun.sleep(1000);
        continue;
      }
      if (!this.running) {
        // Recebidas durante o shutdown: devolve imediatamente para outra instância.
        await Promise.allSettled(messages.map((m) => this.releaseVisibility(queueUrl, m)));
        break;
      }
      // FIFO: sequencial dentro do grupo, paralelo entre grupos.
      const groups = new Map<string, Message[]>();
      for (const m of messages) {
        const group = m.Attributes?.MessageGroupId ?? 'default';
        groups.set(group, [...(groups.get(group) ?? []), m]);
      }
      const batch = [...groups.values()].map((group) => this.track(this.processGroup(queueUrl, group)));
      await Promise.allSettled(batch);
    }
  }

  private track(p: Promise<void>): Promise<void> {
    this.inFlight.add(p);
    return p.finally(() => this.inFlight.delete(p));
  }

  private async processGroup(queueUrl: string, group: Message[]): Promise<void> {
    for (let i = 0; i < group.length; i++) {
      const message = group[i]!;
      if (!this.running && i > 0) {
        // Shutdown no meio do grupo: devolve o restante, preservando a ordem do FIFO.
        await Promise.allSettled(group.slice(i).map((m) => this.releaseVisibility(queueUrl, m)));
        return;
      }
      const outcome = await this.handle(queueUrl, message);
      if (outcome.kind === 'retry') {
        // Em FIFO, as próximas do grupo não podem passar à frente: devolve todas.
        await Promise.allSettled(group.slice(i + 1).map((m) => this.releaseVisibility(queueUrl, m)));
        return;
      }
    }
  }

  /** Processa uma mensagem e aplica ack / retry / DLQ. Exposto para testes. */
  async handle(queueUrl: string, message: Message): Promise<Classification> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
    let log = this.logger.child({ sqsMessageId: message.MessageId });
    let classification: Classification;
    try {
      const raw = JSON.parse(message.Body ?? '');
      const envelope = envelopeSchema.safeParse(raw);
      if (!envelope.success) throw new ValidationError('Malformed message envelope');
      const { idempotencyKey, ...data } = envelope.data.data;
      log = log.child({ messageId: envelope.data.messageId, providerId: data.providerId });
      const command = toWagerCommand(data, idempotencyKey);
      const result = await this.useCase.execute(command, {
        correlationId: envelope.data.messageId,
        causationId: envelope.data.messageId,
        source: 'sqs',
        inbox: { consumerName: WAGER_CONSUMER_NAME, messageId: envelope.data.messageId, payloadHash: sha256Canonical(raw) },
      });
      if (result.type === 'duplicate_message') log.info({}, 'duplicate message acknowledged (inbox)');
      if (process.env.FAULT_INJECTION === 'crash_before_ack' && result.type === 'result') {
        // Gancho de teste: simula a morte do processo depois do commit e antes do ack.
        log.warn({}, 'fault injection: exiting before ack');
        process.exit(137);
      }
      classification = { kind: 'ack' };
    } catch (err) {
      classification = this.classify(err, receiveCount);
      log[classification.kind === 'retry' ? 'warn' : 'error'](
        { err: (err as Error).message, receiveCount, outcome: classification.kind },
        'wager message failed',
      );
    }

    switch (classification.kind) {
      case 'ack':
        await this.ack(queueUrl, message);
        break;
      case 'retry':
        await this.client
          .send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: Math.min(300, 2 ** receiveCount),
            }),
          )
          .catch(() => undefined);
        this.metrics.retry('sqs_message');
        break;
      case 'dlq':
        await this.sendToDlq(message, classification.reason);
        await this.ack(queueUrl, message);
        this.metrics.dlqMessage(classification.reason);
        break;
    }
    return classification;
  }

  private classify(err: unknown, receiveCount: number): Classification {
    if (err instanceof SyntaxError) return { kind: 'dlq', reason: 'malformed_json' };
    if (err instanceof ValidationError || err instanceof DomainError) return { kind: 'dlq', reason: 'invalid_payload' };
    if (err instanceof IdempotencyConflictError) return { kind: 'dlq', reason: 'idempotency_conflict' };
    const transient = err instanceof TransientError || err instanceof ConcurrentWriteError;
    if (receiveCount >= this.config.maxAttempts) {
      return { kind: 'dlq', reason: transient ? 'retries_exhausted' : 'unexpected_error' };
    }
    // Erros inesperados também são repetidos até o limite: pode ser transitório não classificado.
    return { kind: 'retry', reason: transient ? 'transient' : 'unexpected' };
  }

  private async ack(queueUrl: string, message: Message): Promise<void> {
    // Se o ack falhar, a mensagem volta e a inbox garante que não haverá efeito duplicado.
    await this.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle })).catch((err) => {
      this.logger.warn({ sqsMessageId: message.MessageId, err: (err as Error).message }, 'sqs ack failed');
    });
  }

  private async sendToDlq(message: Message, reason: string): Promise<void> {
    const dlqUrl = await resolveQueueUrl(this.client, this.config.wagerDlq);
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: dlqUrl,
        MessageBody: message.Body ?? '',
        MessageGroupId: message.Attributes?.MessageGroupId ?? 'dlq',
        MessageDeduplicationId: message.MessageId,
        MessageAttributes: { dlqReason: { DataType: 'String', StringValue: reason } },
      }),
    );
  }

  private async releaseVisibility(queueUrl: string, message: Message): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }),
    );
  }
}
