import type { AppLogger, Clock, EventPublisher, Metrics, OutboxStore } from '../../application/ports';

/**
 * Publica eventos pendentes da outbox. Garantias:
 * - só lê eventos já confirmados (estão na tabela ⇒ a transação financeira commitou);
 * - várias instâncias em paralelo: FOR UPDATE SKIP LOCKED evita disputa pela mesma linha;
 * - crash após publicar e antes do commit ⇒ republicação (at-least-once); o eventId permite dedup no consumidor;
 * - falha de publicação ⇒ backoff exponencial por mensagem, sem perder o evento.
 */
export class OutboxRelay {
  constructor(
    private readonly store: OutboxStore,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly batchSize = 100,
  ) {}

  async runOnce(): Promise<{ published: number; failed: number }> {
    const result = await this.store.processBatch(this.clock.now(), this.batchSize, (batch) => this.publisher.publishBatch(batch));
    if (result.published) this.metrics.outboxPublished(result.published);
    if (result.failed) {
      this.metrics.outboxPublishFailed(result.failed);
      this.logger.warn({ failed: result.failed }, 'outbox publish failures, scheduled for retry');
    }
    const now = this.clock.now();
    const [lagMs, pending] = await Promise.all([this.store.oldestPendingAgeMs(now), this.store.pendingCount()]);
    this.metrics.outboxLag(lagMs / 1000, pending);
    return result;
  }

  /** Esvazia a outbox (usado em testes e no shutdown). */
  async drain(maxRounds = 50): Promise<number> {
    let total = 0;
    for (let i = 0; i < maxRounds; i++) {
      const r = await this.runOnce();
      total += r.published;
      if (r.published === 0) break;
    }
    return total;
  }
}
