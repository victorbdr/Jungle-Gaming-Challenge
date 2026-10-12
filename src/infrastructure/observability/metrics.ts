import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import type { Metrics, TransactionSource } from '../../application/ports';

export class PrometheusMetrics implements Metrics {
  readonly registry = new Registry();

  private readonly transactions = new Counter({
    name: 'wagering_transactions_total',
    help: 'Transações finalizadas por status, kind e origem',
    labelNames: ['status', 'kind', 'source'] as const,
    registers: [this.registry],
  });
  private readonly duplicates = new Counter({
    name: 'wagering_duplicates_total',
    help: 'Duplicatas detectadas (replay idempotente, conflito de payload, inbox)',
    labelNames: ['kind'] as const,
    registers: [this.registry],
  });
  private readonly retries = new Counter({
    name: 'wagering_retries_total',
    help: 'Novas tentativas por operação',
    labelNames: ['operation'] as const,
    registers: [this.registry],
  });
  private readonly lockConflicts = new Counter({
    name: 'wagering_lock_conflicts_total',
    help: 'Conflitos de concorrência detectados pelo banco',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });
  private readonly latency = new Histogram({
    name: 'wagering_processing_duration_seconds',
    help: 'Latência de processamento de transações',
    labelNames: ['source'] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [this.registry],
  });
  private readonly dlq = new Counter({
    name: 'wagering_dlq_messages_total',
    help: 'Mensagens enviadas para a DLQ',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });
  private readonly outboxPublishedCounter = new Counter({
    name: 'wagering_outbox_published_total',
    help: 'Eventos publicados pela outbox',
    registers: [this.registry],
  });
  private readonly outboxFailedCounter = new Counter({
    name: 'wagering_outbox_publish_failures_total',
    help: 'Falhas de publicação da outbox',
    registers: [this.registry],
  });
  private readonly outboxLagGauge = new Gauge({
    name: 'wagering_outbox_lag_seconds',
    help: 'Idade do evento pendente mais antigo na outbox',
    registers: [this.registry],
  });
  private readonly outboxPendingGauge = new Gauge({
    name: 'wagering_outbox_pending',
    help: 'Eventos pendentes na outbox',
    registers: [this.registry],
  });
  private readonly reconciliation = new Counter({
    name: 'wagering_reconciliation_mismatches_total',
    help: 'Divergências entre saldo materializado e ledger',
    registers: [this.registry],
  });

  constructor() {
    collectDefaultMetrics({ register: this.registry });
  }

  transactionFinalized(status: string, kind: string, source: TransactionSource): void {
    this.transactions.inc({ status, kind, source });
  }
  duplicateDetected(kind: 'idempotency_replay' | 'idempotency_conflict' | 'inbox'): void {
    this.duplicates.inc({ kind });
  }
  retry(operation: string): void {
    this.retries.inc({ operation });
  }
  lockConflict(reason: string): void {
    this.lockConflicts.inc({ reason });
  }
  processingLatency(source: TransactionSource, seconds: number): void {
    this.latency.observe({ source }, seconds);
  }
  dlqMessage(reason: string): void {
    this.dlq.inc({ reason });
  }
  outboxPublished(count: number): void {
    this.outboxPublishedCounter.inc(count);
  }
  outboxPublishFailed(count: number): void {
    this.outboxFailedCounter.inc(count);
  }
  outboxLag(seconds: number, pending: number): void {
    this.outboxLagGauge.set(seconds);
    this.outboxPendingGauge.set(pending);
  }
  reconciliationMismatch(): void {
    this.reconciliation.inc();
  }
}
