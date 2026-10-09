import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresPayoutRepository } from '../../src/adapters/outbound/postgres/PostgresPayoutRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresPayoutRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  repository = new PostgresPayoutRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE payout_attempts, outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresPayoutRepository', () => {
  it('reports a transferId as unprocessed before any attempt is saved', async () => {
    expect(await repository.wasProcessed('t1')).toBe(false);
  });

  it('saves a successful attempt and publishes a payout-result outbox row', async () => {
    await repository.savePayoutAttempt({ transferId: 't1', toAccount: 'acc_2', amount: 500, status: 'sent', pspReference: 'ref-1' });

    expect(await repository.wasProcessed('t1')).toBe(true);
    const record = await repository.get('t1');
    expect(record).toMatchObject({ transferId: 't1', toAccount: 'acc_2', amount: 500, status: 'sent', pspReference: 'ref-1' });

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'payout-result'");
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].payload).toMatchObject({ transferId: 't1', outcome: 'succeeded', pspReference: 'ref-1' });
  });

  it('upserts on retry: a second save for the same transferId updates the row instead of duplicating it', async () => {
    await repository.savePayoutAttempt({ transferId: 't2', toAccount: 'acc_2', amount: 500, status: 'failed', pspReference: null });
    await repository.savePayoutAttempt({ transferId: 't2', toAccount: 'acc_2', amount: 500, status: 'sent', pspReference: 'ref-2' });

    const record = await repository.get('t2');
    expect(record?.status).toBe('sent');

    const all = await pool.query("SELECT * FROM payout_attempts WHERE transfer_id = 't2'");
    expect(all.rows).toHaveLength(1);
  });

  it('lists only failed attempts', async () => {
    await repository.savePayoutAttempt({ transferId: 't3', toAccount: 'acc_2', amount: 100, status: 'sent', pspReference: 'ref-3' });
    await repository.savePayoutAttempt({ transferId: 't4', toAccount: 'acc_psp_fail_demo', amount: 100, status: 'failed', pspReference: null });

    const failed = await repository.listFailed({ limit: 20, offset: 0 });
    expect(failed).toHaveLength(1);
    expect(failed[0].transferId).toBe('t4');
  });
});
