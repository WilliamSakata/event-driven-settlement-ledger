import { Pool } from 'pg';
import { BalanceProjectionPort } from '../../../application/ports/BalanceProjectionPort';

export class PostgresBalanceProjection implements BalanceProjectionPort {
  constructor(private readonly pool: Pool) {}

  async getConfirmedBalance(accountId: string): Promise<number> {
    const result = await this.pool.query('SELECT confirmed_balance FROM balance_projection WHERE account_id = $1', [accountId]);
    if (result.rows.length === 0) {
      return 0;
    }
    return Number(result.rows[0].confirmed_balance);
  }

  async getPendingReservationsTotal(accountId: string): Promise<number> {
    const result = await this.pool.query(
      "SELECT COALESCE(SUM(amount), 0) AS total FROM reservations WHERE account_id = $1 AND status = 'pending'",
      [accountId],
    );
    return Number(result.rows[0].total);
  }
}
