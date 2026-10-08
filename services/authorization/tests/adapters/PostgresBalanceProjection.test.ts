import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresBalanceProjection } from '../../src/adapters/outbound/postgres/PostgresBalanceProjection';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let balanceProjection: PostgresBalanceProjection;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  balanceProjection = new PostgresBalanceProjection(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE balance_projection, reservations, transfers CASCADE');
  await pool.query("INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ('acc_1', 1000)");
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresBalanceProjection', () => {
  it('returns the confirmed balance for a known account', async () => {
    expect(await balanceProjection.getConfirmedBalance('acc_1')).toBe(1000);
  });

  it('returns 0 for an unknown account', async () => {
    expect(await balanceProjection.getConfirmedBalance('acc_unknown')).toBe(0);
  });

  it('sums only pending reservations for the account', async () => {
    await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('t1', 'acc_1', 'acc_2', 100, 'approved')");
    await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('t2', 'acc_1', 'acc_2', 50, 'confirmed')");
    await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('t1', 'acc_1', 100, 'pending')");
    await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('t2', 'acc_1', 50, 'released')");

    expect(await balanceProjection.getPendingReservationsTotal('acc_1')).toBe(100);
  });
});
