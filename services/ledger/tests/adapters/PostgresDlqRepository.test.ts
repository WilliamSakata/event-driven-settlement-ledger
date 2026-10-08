import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresDlqRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=ledger' });
  repository = new PostgresDlqRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE dlq_events CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresDlqRepository', () => {
  it('adds and lists an entry', async () => {
    const id = await repository.add({
      transferId: 't1',
      topic: 'transfer-authorized',
      payload: { transferId: 't1' },
      failureReason: 'boom',
      attempts: 3,
    });

    const entries = await repository.list({ limit: 20, offset: 0 });
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ id, transferId: 't1', topic: 'transfer-authorized', failureReason: 'boom', attempts: 3 });
  });

  it('returns null for a non-UUID id instead of throwing', async () => {
    expect(await repository.get('not-a-uuid')).toBeNull();
  });

  it('marks an entry as reprocessed and excludes it from the default list', async () => {
    const id = await repository.add({
      transferId: 't2',
      topic: 'transfer-authorized',
      payload: {},
      failureReason: 'boom',
      attempts: 3,
    });

    await repository.markReprocessed(id);

    const entry = await repository.get(id);
    expect(entry?.reprocessedAt).not.toBeNull();

    const entries = await repository.list({ limit: 20, offset: 0 });
    expect(entries).toHaveLength(0);
  });
});
