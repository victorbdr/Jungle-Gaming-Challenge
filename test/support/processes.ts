import type { Subprocess } from 'bun';
import { resolve } from 'node:path';
import { AWS_ENDPOINT } from './env';

const ROOT = resolve(import.meta.dir, '../..');

export interface AppProcess {
  port: number;
  proc: Subprocess;
  baseUrl: string;
  logs: string[];
}

/** Sobe uma instância real da aplicação (processo separado) apontando para a infra de teste. */
export const spawnApp = async (opts: {
  port: number;
  databaseUrl: string;
  role?: string;
  queues?: { wagerQueue: string; wagerDlq: string; eventsQueue: string };
  env?: Record<string, string>;
  /** false: não espera o health (ex.: processo que deve morrer logo). */
  waitHealthy?: boolean;
}): Promise<AppProcess> => {
  const logs: string[] = [];
  const proc = Bun.spawn(['bun', 'src/main.ts'], {
    cwd: ROOT,
    env: {
      ...process.env,
      PORT: String(opts.port),
      ROLE: opts.role ?? 'api',
      DATABASE_URL: opts.databaseUrl,
      AWS_ENDPOINT,
      LOG_LEVEL: 'warn',
      DB_POOL_MAX: '10',
      OUTBOX_POLL_MS: '100',
      PENDING_REFERENCE_POLL_MS: '200',
      ...(opts.queues
        ? { SQS_WAGER_QUEUE: opts.queues.wagerQueue, SQS_WAGER_DLQ: opts.queues.wagerDlq, SQS_EVENTS_QUEUE: opts.queues.eventsQueue }
        : {}),
      ...opts.env,
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const collect = async (stream: ReadableStream<Uint8Array> | null) => {
    if (!stream) return;
    const decoder = new TextDecoder();
    for await (const chunk of stream) logs.push(decoder.decode(chunk));
  };
  void collect(proc.stdout as ReadableStream<Uint8Array>);
  void collect(proc.stderr as ReadableStream<Uint8Array>);

  const baseUrl = `http://127.0.0.1:${opts.port}`;
  if (opts.waitHealthy === false) return { port: opts.port, proc, baseUrl, logs };
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) throw new Error(`app on ${opts.port} exited early:\n${logs.join('')}`);
    try {
      const r = await fetch(`${baseUrl}/health/live`);
      if (r.ok) return { port: opts.port, proc, baseUrl, logs };
    } catch {
      /* ainda subindo */
    }
    await Bun.sleep(200);
  }
  proc.kill();
  throw new Error(`app on ${opts.port} did not start:\n${logs.join('')}`);
};

export const stopApp = async (app: AppProcess, signal: NodeJS.Signals = 'SIGTERM') => {
  if (app.proc.exitCode !== null) return;
  app.proc.kill(signal);
  await Promise.race([app.proc.exited, Bun.sleep(25_000)]);
  if (app.proc.exitCode === null) app.proc.kill('SIGKILL');
};

export const postJson = async (baseUrl: string, path: string, body: unknown, headers: Record<string, string> = {}) => {
  const r = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: r.status, body: (await r.json()) as Record<string, unknown> };
};
