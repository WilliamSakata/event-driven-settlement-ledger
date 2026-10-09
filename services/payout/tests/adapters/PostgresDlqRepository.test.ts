import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresDlqRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  repository = new PostgresDlqRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE dlq_events CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresDlqRepository', () => {
  it('adds, lists, and marks an entry as reprocessed', async () => {
    const id = await repository.add({ transferId: 't1', topic: 'settlement-posted', payload: {}, failureReason: 'boom', attempts: 3 });
    expect(await repository.list({ limit: 20, offset: 0 })).toHaveLength(1);
    await repository.markReprocessed(id);
    expect(await repository.list({ limit: 20, offset: 0 })).toHaveLength(0);
  });

  it('returns null for a non-UUID id', async () => {
    expect(await repository.get('not-a-uuid')).toBeNull();
  });
});
