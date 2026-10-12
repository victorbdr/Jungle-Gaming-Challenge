import { MikroORM } from '@mikro-orm/postgresql';
import { CreateWalletUseCase } from '../application/create-wallet.use-case';
import type { AppLogger, Clock } from '../application/ports';
import { ProcessWagerTransactionUseCase } from '../application/process-wager.use-case';
import { ReconcileWalletUseCase } from '../application/reconcile-wallet.use-case';
import { ResolvePendingReferencesUseCase } from '../application/resolve-pending-references.use-case';
import { createLogger } from '../infrastructure/observability/logger';
import { PrometheusMetrics } from '../infrastructure/observability/metrics';
import { MikroOrmOutboxStore, MikroOrmTransactionRunner, MikroOrmWalletQueries } from '../infrastructure/persistence/mikro-orm.adapters';
import { createOrmConfig } from '../infrastructure/persistence/orm.config';

export interface ContainerOptions {
  databaseUrl?: string;
  poolMax?: number;
  clock?: Clock;
  logger?: AppLogger;
}

/** Raiz de composição independente de framework: usada pelo Nest, pelos workers e pelos testes. */
export const createContainer = async (options: ContainerOptions = {}) => {
  const orm = await MikroORM.init(createOrmConfig({ clientUrl: options.databaseUrl, poolMax: options.poolMax }));
  const clock: Clock = options.clock ?? { now: () => new Date() };
  const logger = options.logger ?? createLogger();
  const metrics = new PrometheusMetrics();
  const runner = new MikroOrmTransactionRunner(orm);
  const queries = new MikroOrmWalletQueries(orm);
  const outboxStore = new MikroOrmOutboxStore(orm);

  return {
    orm,
    clock,
    logger,
    metrics,
    runner,
    queries,
    outboxStore,
    processWager: new ProcessWagerTransactionUseCase(runner, clock, metrics, logger),
    createWallet: new CreateWalletUseCase(runner, clock, metrics, logger),
    resolvePendingReferences: new ResolvePendingReferencesUseCase(runner, queries, clock, metrics, logger),
    reconcileWallet: new ReconcileWalletUseCase(queries, metrics, logger),
    close: () => orm.close(true),
  };
};

export type Container = Awaited<ReturnType<typeof createContainer>>;
