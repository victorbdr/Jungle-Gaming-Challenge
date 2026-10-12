import { Client } from 'pg';

/**
 * Infra real para testes: o Postgres e o LocalStack do docker-compose (`docker compose up -d postgres localstack`).
 * Cada suíte usa um banco próprio, recriado do zero e migrado com as migrations reais.
 */
export const ADMIN_URL = process.env.TEST_DATABASE_ADMIN_URL ?? 'postgres://wagering:wagering@localhost:5432/postgres';

export const dbUrl = (name: string) => {
  const u = new URL(ADMIN_URL);
  u.pathname = `/${name}`;
  return u.toString();
};

export const recreateDatabase = async (name: string): Promise<string> => {
  const admin = new Client({ connectionString: ADMIN_URL });
  await admin.connect();
  try {
    await admin.query(`drop database if exists "${name}" with (force)`);
    await admin.query(`create database "${name}"`);
  } finally {
    await admin.end();
  }
  return dbUrl(name);
};

export const AWS_ENDPOINT = process.env.AWS_ENDPOINT ?? 'http://localhost:4566';
