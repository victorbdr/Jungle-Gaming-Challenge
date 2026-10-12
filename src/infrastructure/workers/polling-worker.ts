import type { AppLogger } from '../../application/ports';

/** Loop de polling com parada graciosa: `stop()` espera a iteração em andamento terminar. */
export class PollingWorker {
  private running = false;
  private loop: Promise<void> | undefined;
  private wake: (() => void) | undefined;

  constructor(
    private readonly name: string,
    private readonly intervalMs: number,
    private readonly tick: () => Promise<unknown>,
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
  }

  async stop(): Promise<void> {
    this.running = false;
    this.wake?.();
    await this.loop;
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.tick();
      } catch (err) {
        this.logger.error({ worker: this.name, err: (err as Error).message }, 'worker iteration failed');
      }
      if (!this.running) break;
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, this.intervalMs);
        this.wake = () => {
          clearTimeout(t);
          resolve();
        };
      });
    }
  }
}
