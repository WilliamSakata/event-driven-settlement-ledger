import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresSettlementRepository } from '../../src/adapters/outbound/postgres/PostgresSettlementRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresSettlementRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  repository = new PostgresSettlementRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE transfers, reservations, balance_projection CASCADE');
  await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('t1', 'acc_1', 'acc_2', 500, 'approved')");
  await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('t1', 'acc_1', 500, 'pending')");
  await pool.query("INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ('acc_1', 1000)");
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresSettlementRepository', () => {
  it('reports a transfer as not confirmed before confirmSettlement runs', async () => {
    expect(await repository.wasConfirmed('t1')).toBe(false);
  });

  it('confirms the settlement: transfer status, reservation release, and both balances', async () => {
    await repository.confirmSettlement({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(await repository.wasConfirmed('t1')).toBe(true);

    const transfer = await pool.query("SELECT status FROM transfers WHERE id = 't1'");
    expect(transfer.rows[0].status).toBe('confirmed');

    const reservation = await pool.query("SELECT status FROM reservations WHERE transfer_id = 't1'");
    expect(reservation.rows[0].status).toBe('released');

    const fromBalance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_1'");
    expect(Number(fromBalance.rows[0].confirmed_balance)).toBe(500);

    const toBalance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_2'");
    expect(Number(toBalance.rows[0].confirmed_balance)).toBe(500);
  });

  it('is a no-op when confirmSettlement is called again for an already-confirmed transfer', async () => {
    const event = { transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 };

    await repository.confirmSettlement(event);
    await repository.confirmSettlement(event);

    const transfer = await pool.query("SELECT status FROM transfers WHERE id = 't1'");
    expect(transfer.rows[0].status).toBe('confirmed');

    const reservation = await pool.query("SELECT status FROM reservations WHERE transfer_id = 't1'");
    expect(reservation.rows[0].status).toBe('released');

    const fromBalance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_1'");
    expect(Number(fromBalance.rows[0].confirmed_balance)).toBe(500);

    const toBalance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_2'");
    expect(Number(toBalance.rows[0].confirmed_balance)).toBe(500);
  });
});
