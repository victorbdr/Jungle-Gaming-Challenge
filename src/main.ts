import 'reflect-metadata';
import { createContainer } from './bootstrap/container';
import { createHttpApp } from './infrastructure/http/http.module';
import { createSqsClient, sqsConfigFromEnv } from './infrastructure/sqs/sqs-config';
import { SqsEventPublisher } from './infrastructure/sqs/sqs-event-publisher';
import { WagerSqsConsumer } from './infrastructure/sqs/wager-consumer';
import { OutboxRelay } from './infrastructure/workers/outbox-relay';
import { PollingWorker } from './infrastructure/workers/polling-worker';

/**
 * ROLE controla o que a instância executa (todas as combinações são seguras com N instâncias):
 *   api | consumer | workers | all (padrão)
 */
const main = async () => {
  const role = process.env.ROLE ?? 'all';
  const has = (r: string) => role === 'all' || role.split(',').includes(r);

  const container = await createContainer();
  const { logger, metrics, clock } = container;
  const sqsConfig = sqsConfigFromEnv();
  const sqs = createSqsClient(sqsConfig);

  // O endpoint HTTP (health/metrics) sobe em todos os papéis; as rotas de negócio são inofensivas em workers.
  const app = await createHttpApp(container, sqs, sqsConfig);
  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);

  const stoppables: { name: string; stop: () => Promise<void> }[] = [];

  if (has('consumer')) {
    const consumer = new WagerSqsConsumer(sqs, sqsConfig, container.processWager, metrics, logger, {
      visibilityTimeout: Number(process.env.SQS_VISIBILITY_TIMEOUT ?? 30),
    });
    consumer.start();
    stoppables.push({ name: 'sqs-consumer', stop: () => consumer.stop() });
  }
  if (has('workers')) {
    const relay = new OutboxRelay(container.outboxStore, new SqsEventPublisher(sqs, sqsConfig.eventsQueue), clock, metrics, logger);
    const outboxWorker = new PollingWorker('outbox-relay', Number(process.env.OUTBOX_POLL_MS ?? 200), () => relay.runOnce(), logger);
    const pendingWorker = new PollingWorker(
      'pending-references',
      Number(process.env.PENDING_REFERENCE_POLL_MS ?? 1000),
      () => container.resolvePendingReferences.runOnce(),
      logger,
    );
    outboxWorker.start();
    pendingWorker.start();
    stoppables.push({ name: 'outbox-relay', stop: () => outboxWorker.stop() }, { name: 'pending-references', stop: () => pendingWorker.stop() });
  }

  logger.info({ role, port }, 'wagering processor started');

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down: draining in-flight work');
    await Promise.allSettled(stoppables.map((s) => s.stop()));
    await app.close().catch(() => undefined);
    await container.close().catch(() => undefined);
    logger.info({}, 'shutdown complete');
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
};

main().catch((err) => {
  console.error(JSON.stringify({ level: 'fatal', msg: 'startup failed', err: (err as Error).message }));
  process.exit(1);
});
