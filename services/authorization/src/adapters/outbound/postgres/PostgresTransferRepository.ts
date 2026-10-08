import { Pool } from 'pg';
import { TransferRepositoryPort, TransferRecord, ReservationInput } from '../../../application/ports/TransferRepositoryPort';

export class PostgresTransferRepository implements TransferRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async findById(id: string): Promise<TransferRecord | null> {
    const result = await this.pool.query(
      'SELECT id, from_account, to_account, amount, status FROM transfers WHERE id = $1',
      [id],
    );
    if (result.rows.length === 0) {
      return null;
    }
    const row = result.rows[0];
    return {
      id: row.id,
      fromAccount: row.from_account,
      toAccount: row.to_account,
      amount: Number(row.amount),
      status: row.status,
    };
  }

  async saveApproved(transfer: TransferRecord, reservation: ReservationInput, outboxPayload: unknown): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        'INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ($1, $2, $3, $4, $5)',
        [transfer.id, transfer.fromAccount, transfer.toAccount, transfer.amount, transfer.status],
      );
      await client.query(
        "INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ($1, $2, $3, 'pending')",
        [transfer.id, reservation.accountId, reservation.amount],
      );
      await client.query(
        "INSERT INTO outbox (topic, payload) VALUES ('transfer-authorized', $1)",
        [JSON.stringify(outboxPayload)],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async saveRejected(transfer: TransferRecord): Promise<void> {
    await this.pool.query(
      'INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ($1, $2, $3, $4, $5)',
      [transfer.id, transfer.fromAccount, transfer.toAccount, transfer.amount, transfer.status],
    );
  }
}
