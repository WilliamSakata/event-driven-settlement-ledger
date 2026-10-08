import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresLedgerRepository } from '../../src/adapters/outbound/postgres/PostgresLedgerRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresLedgerRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=ledger' });
  repository = new PostgresLedgerRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE ledger_entries, processed_transfers, outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresLedgerRepository', () => {
  it('reports a transferId as not processed before it is posted', async () => {
    expect(await repository.wasProcessed('t1')).toBe(false);
  });

  it('posts a settlement atomically: two ledger entries, a processed_transfers row, and an outbox row', async () => {
    await repository.postSettlement({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(await repository.wasProcessed('t1')).toBe(true);

    const entries = await pool.query("SELECT * FROM ledger_entries WHERE transfer_id = 't1' ORDER BY direction");
    expect(entries.rows).toHaveLength(2);
    expect(entries.rows[0]).toMatchObject({ account_id: 'acc_2', direction: 'credit', amount: '500' });
    expect(entries.rows[1]).toMatchObject({ account_id: 'acc_1', direction: 'debit', amount: '500' });

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'settlement-posted'");
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].payload).toMatchObject({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
  });

  it('lists entries for an account in chronological order', async () => {
    await repository.postSettlement({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    await repository.postSettlement({ transferId: 't2', fromAccount: 'acc_2', toAccount: 'acc_1', amount: 100 });

    const entries = await repository.listEntriesForAccount('acc_1');
    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({ transferId: 't1', direction: 'debit', amount: 500 });
    expect(entries[1]).toMatchObject({ transferId: 't2', direction: 'credit', amount: 100 });
  });
});
