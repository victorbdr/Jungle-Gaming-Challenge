import { Migrator } from '@mikro-orm/migrations';
import { defineConfig } from '@mikro-orm/postgresql';
import { ENTITIES } from './entities';
import { Migration20261010000000_initial } from './migrations/Migration20261010000000_initial';

export const createOrmConfig = (overrides: { clientUrl?: string; poolMax?: number } = {}) =>
  defineConfig({
    clientUrl: overrides.clientUrl ?? process.env.DATABASE_URL ?? 'postgres://wagering:wagering@localhost:5432/wagering',
    entities: ENTITIES,
    extensions: [Migrator],
    // descoberta explícita: sem varredura de arquivos (compatível com Bun e com o bundle)
    discovery: { warnWhenNoEntities: true },
    migrations: {
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
      // migrations escritas à mão: o snapshot do gerador de diffs não é usado
      snapshot: false,
      migrationsList: [{ name: 'Migration20261010000000_initial', class: Migration20261010000000_initial }],
    },
    pool: { min: 1, max: overrides.poolMax ?? Number(process.env.DB_POOL_MAX ?? 20) },
    // Sem Identity Map compartilhado entre requisições: cada use case usa um fork do EntityManager.
    allowGlobalContext: false,
    debug: process.env.ORM_DEBUG === 'true',
  });
