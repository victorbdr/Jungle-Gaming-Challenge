import { SendMessageBatchCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { EventPublisher } from '../../application/ports';
import type { OutboxMessage } from '../../messaging/domain/outbox-message';
import { resolveQueueUrl } from './sqs-config';

const SQS_BATCH_LIMIT = 10;

/**
 * Publica eventos de integração em uma fila FIFO, em lotes de 10 (SendMessageBatch).
 * Ordem por agregado (MessageGroupId) e deduplicação do broker por eventId — uma otimização:
 * consumidores devem deduplicar por eventId. Falhas parciais do lote são reportadas por mensagem.
 */
export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly client: SQSClient,
    private readonly queueName: string,
  ) {}

  async publishBatch(messages: readonly OutboxMessage[]): Promise<Map<string, string>> {
    const failures = new Map<string, string>();
    const queueUrl = await resolveQueueUrl(this.client, this.queueName);
    // Lotes sequenciais: preserva a ordem relativa dentro de cada MessageGroupId.
    for (let i = 0; i < messages.length; i += SQS_BATCH_LIMIT) {
      const chunk = messages.slice(i, i + SQS_BATCH_LIMIT);
      try {
        const res = await this.client.send(
          new SendMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: chunk.map((m, idx) => ({
              Id: String(idx),
              MessageBody: JSON.stringify(m.payload),
              MessageGroupId: m.aggregateId,
              MessageDeduplicationId: m.id,
              MessageAttributes: { eventType: { DataType: 'String', StringValue: m.eventType } },
            })),
          }),
        );
        for (const f of res.Failed ?? []) {
          const m = chunk[Number(f.Id)];
          if (m) failures.set(m.id, `${f.Code ?? 'error'}: ${f.Message ?? ''}`);
        }
      } catch (err) {
        for (const m of chunk) failures.set(m.id, (err as Error).message);
      }
    }
    return failures;
  }
}
