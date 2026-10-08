import { Pool } from 'pg';
import { SettlementRepositoryPort, SettlementEvent } from '../../../application/ports/SettlementRepositoryPort';

export class PostgresSettlementRepository implements SettlementRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async wasConfirmed(transferId: string): Promise<boolean> {
    const result = await this.pool.query("SELECT 1 FROM transfers WHERE id = $1 AND status = 'confirmed'", [transferId]);
    return result.rows.length > 0;
  }

  async confirmSettlement(event: SettlementEvent): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("UPDATE transfers SET status = 'confirmed' WHERE id = $1 AND status = 'approved'", [event.transferId]);
      await client.query("UPDATE reservations SET status = 'released' WHERE transfer_id = $1 AND status = 'pending'", [event.transferId]);
      await client.query(
        `INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ($1, $2)
         ON CONFLICT (account_id) DO UPDATE SET confirmed_balance = balance_projection.confirmed_balance - $2`,
        [event.fromAccount, event.amount],
      );
      await client.query(
        `INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ($1, $2)
         ON CONFLICT (account_id) DO UPDATE SET confirmed_balance = balance_projection.confirmed_balance + $2`,
        [event.toAccount, event.amount],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}
