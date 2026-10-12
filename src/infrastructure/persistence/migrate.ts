import { MikroORM } from '@mikro-orm/postgresql';
import { createOrmConfig } from './orm.config';

/** Uso: bun src/infrastructure/persistence/migrate.ts up|down [--to=0] */
export const runMigrations = async (direction: 'up' | 'down', clientUrl?: string, toZero = false): Promise<string[]> => {
  const orm = await MikroORM.init(createOrmConfig({ clientUrl, poolMax: 2 }));
  try {
    const result =
      direction === 'up' ? await orm.migrator.up() : await orm.migrator.down(toZero ? { to: 0 } : undefined);
    return result.map((m) => m.name);
  } finally {
    await orm.close(true);
  }
};

if (import.meta.main) {
  const direction = process.argv[2] === 'down' ? 'down' : 'up';
  const toZero = process.argv.includes('--to=0');
  runMigrations(direction, undefined, toZero)
    .then((names) => {
      console.log(JSON.stringify({ msg: `migrations ${direction}`, migrations: names }));
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
