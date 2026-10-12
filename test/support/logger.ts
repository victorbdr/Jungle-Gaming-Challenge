import type { AppLogger } from '../../src/application/ports';

export const silentLogger = (): AppLogger => {
  const noop = () => {};
  const l: AppLogger = { debug: noop, info: noop, warn: noop, error: noop, child: () => l };
  return l;
};
