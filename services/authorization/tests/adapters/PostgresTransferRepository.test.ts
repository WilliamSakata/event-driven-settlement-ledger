import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresTransferRepository } from '../../src/adapters/outbound/postgres/PostgresTransferRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresTransferRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  repository = new PostgresTransferRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE transfers, reservations, outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresTransferRepository', () => {
  it('returns null when the transfer does not exist', async () => {
    expect(await repository.findById('missing')).toBeNull();
  });

  it('saves an approved transfer, its reservation, and the outbox row atomically', async () => {
    await repository.saveApproved(
      { id: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, status: 'approved' },
      { accountId: 'acc_1', amount: 500 },
      { transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 },
    );

    const transfer = await repository.findById('t1');
    expect(transfer).toEqual({ id: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, status: 'approved' });

    const reservation = await pool.query("SELECT * FROM reservations WHERE transfer_id = 't1'");
    expect(reservation.rows).toHaveLength(1);
    expect(reservation.rows[0].status).toBe('pending');

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'transfer-authorized'");
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].published_at).toBeNull();
  });

  it('saves a rejected transfer without a reservation or outbox row', async () => {
    await repository.saveRejected({ id: 't2', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 99999, status: 'rejected' });

    const transfer = await repository.findById('t2');
    expect(transfer?.status).toBe('rejected');

    const reservation = await pool.query("SELECT * FROM reservations WHERE transfer_id = 't2'");
    expect(reservation.rows).toHaveLength(0);
  });
});
