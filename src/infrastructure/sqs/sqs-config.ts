import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

export interface SqsConfig {
  region: string;
  endpoint: string | undefined;
  wagerQueue: string;
  wagerDlq: string;
  eventsQueue: string;
  /** Tentativas antes da DLQ (deve coincidir com o maxReceiveCount da redrive policy). */
  maxAttempts: number;
}

export const sqsConfigFromEnv = (overrides: Partial<SqsConfig> = {}): SqsConfig => ({
  region: process.env.AWS_REGION ?? 'us-east-1',
  endpoint: process.env.AWS_ENDPOINT ?? 'http://localhost:4566',
  wagerQueue: process.env.SQS_WAGER_QUEUE ?? 'wager-transactions.fifo',
  wagerDlq: process.env.SQS_WAGER_DLQ ?? 'wager-transactions-dlq.fifo',
  eventsQueue: process.env.SQS_EVENTS_QUEUE ?? 'wagering-events.fifo',
  maxAttempts: Number(process.env.SQS_MAX_ATTEMPTS ?? 5),
  ...overrides,
});

export const createSqsClient = (config: SqsConfig): SQSClient =>
  new SQSClient({
    region: config.region,
    ...(config.endpoint ? { endpoint: config.endpoint } : {}),
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
    maxAttempts: 3,
  });

const urlCache = new Map<string, string>();

export const resolveQueueUrl = async (client: SQSClient, queueName: string): Promise<string> => {
  const cached = urlCache.get(queueName);
  if (cached) return cached;
  const res = await client.send(new GetQueueUrlCommand({ QueueName: queueName }));
  if (!res.QueueUrl) throw new Error(`Queue ${queueName} not found`);
  urlCache.set(queueName, res.QueueUrl);
  return res.QueueUrl;
};
