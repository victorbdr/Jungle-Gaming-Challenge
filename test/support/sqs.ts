import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import { createSqsClient, type SqsConfig, sqsConfigFromEnv } from '../../src/infrastructure/sqs/sqs-config';
import { AWS_ENDPOINT } from './env';

export const testSqsClient = (): SQSClient => createSqsClient(sqsConfigFromEnv({ endpoint: AWS_ENDPOINT }));

/** Cria um conjunto isolado de filas FIFO (principal + DLQ com redrive + eventos) para um teste. */
export const createTestQueues = async (client: SQSClient, maxAttempts = 3): Promise<SqsConfig & { wagerQueueUrl: string; dlqUrl: string; eventsUrl: string }> => {
  const suffix = randomUUID().slice(0, 8);
  const names = {
    wagerQueue: `wt-${suffix}.fifo`,
    wagerDlq: `wt-dlq-${suffix}.fifo`,
    eventsQueue: `ev-${suffix}.fifo`,
  };
  const dlq = await client.send(new CreateQueueCommand({ QueueName: names.wagerDlq, Attributes: { FifoQueue: 'true' } }));
  const dlqArn = await client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl!, AttributeNames: ['QueueArn'] }));
  const main = await client.send(
    new CreateQueueCommand({
      QueueName: names.wagerQueue,
      Attributes: {
        FifoQueue: 'true',
        VisibilityTimeout: '30',
        // redrive policy como rede de segurança; o consumidor move explicitamente antes disso
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn.Attributes!.QueueArn, maxReceiveCount: String(maxAttempts + 2) }),
      },
    }),
  );
  const events = await client.send(new CreateQueueCommand({ QueueName: names.eventsQueue, Attributes: { FifoQueue: 'true' } }));
  return {
    ...sqsConfigFromEnv({ endpoint: AWS_ENDPOINT }),
    ...names,
    maxAttempts,
    wagerQueueUrl: main.QueueUrl!,
    dlqUrl: dlq.QueueUrl!,
    eventsUrl: events.QueueUrl!,
  };
};

export const sendWagerMessage = async (
  client: SQSClient,
  queueUrl: string,
  data: Record<string, unknown> & { providerId: string; externalTransactionId: string; walletId: string },
  opts: { messageId?: string; dedupId?: string; rawBody?: string } = {},
) => {
  const messageId = opts.messageId ?? `msg-${randomUUID()}`;
  const body =
    opts.rawBody ??
    JSON.stringify({
      messageId,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: { idempotencyKey: `${data.providerId}:${data.externalTransactionId}`, ...data },
    });
  await client.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: body,
      MessageGroupId: data.walletId,
      // dedup do broker distinto por envio: simula redelivery/duplicação que o FIFO não pegaria
      MessageDeduplicationId: opts.dedupId ?? randomUUID(),
    }),
  );
  return messageId;
};

export const queueDepth = async (client: SQSClient, queueUrl: string) => {
  const r = await client.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return {
    visible: Number(r.Attributes?.ApproximateNumberOfMessages ?? 0),
    inFlight: Number(r.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0),
  };
};

export const drainQueue = async (client: SQSClient, queueUrl: string, expected: number, timeoutMs = 15_000) => {
  const bodies: { body: string; attributes: Record<string, string> }[] = [];
  const deadline = Date.now() + timeoutMs;
  while (bodies.length < expected && Date.now() < deadline) {
    const r = await client.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['All'] }),
    );
    for (const m of r.Messages ?? []) {
      bodies.push({
        body: m.Body ?? '',
        attributes: Object.fromEntries(Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue ?? ''])),
      });
      await client.send(new (await import('@aws-sdk/client-sqs')).DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
    }
  }
  return bodies;
};

export const waitFor = async (predicate: () => Promise<boolean>, timeoutMs = 15_000, intervalMs = 100) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(intervalMs);
  }
  throw new Error('waitFor: condition not met in time');
};
