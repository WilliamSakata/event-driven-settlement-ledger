import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

const SCHEMA = 'authz';

async function migrate(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
  const pool = new Pool({ connectionString: databaseUrl, options: `-c search_path=${SCHEMA}` });

  await pool.query(`CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`);

  try {
    await pool.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
  } catch (error) {
    // Races when docker-compose starts every service's migrate job concurrently
    // against the same database -- the IF NOT EXISTS check isn't atomic across
    // sessions. If another service's migrate job already created it, treat this
    // as success. Isolated here, as its own statement, so a race on it can never
    // abort a migration file's own DDL (Postgres's simple-query protocol runs a
    // multi-statement string as one implicit transaction -- an error partway
    // through aborts every statement after it in that string).
    const code = error instanceof Error && 'code' in error ? (error as { code: string }).code : undefined;
    const message = error instanceof Error ? error.message : String(error);
    const isConcurrentPgcryptoRace = (code === '23505' || code === '42710') && message.includes('pg_extension');
    if (!isConcurrentPgcryptoRace) {
      throw error;
    }
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const migrationsDir = path.join(__dirname, 'migrations');
  const files = readdirSync(migrationsDir)
    .filter((file) => file.endsWith('.sql'))
    .sort();

  for (const file of files) {
    const alreadyApplied = await pool.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [file]);
    if (alreadyApplied.rows.length > 0) {
      continue;
    }

    const sql = readFileSync(path.join(migrationsDir, file), 'utf-8');
    await pool.query(sql);
    await pool.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
    console.log(`applied migration ${file}`);
  }

  await pool.end();
}

migrate().catch((error) => {
  console.error('migration failed', error);
  process.exit(1);
});
