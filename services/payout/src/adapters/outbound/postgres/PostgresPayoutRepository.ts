import { Pool } from 'pg';
import { PayoutRepositoryPort, PayoutAttemptInput, PayoutAttemptRecord } from '../../../application/ports/PayoutRepositoryPort';

export class PostgresPayoutRepository implements PayoutRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async wasProcessed(transferId: string): Promise<boolean> {
    const result = await this.pool.query('SELECT 1 FROM payout_attempts WHERE transfer_id = $1', [transferId]);
    return result.rows.length > 0;
  }

  async savePayoutAttempt(input: PayoutAttemptInput): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO payout_attempts (transfer_id, to_account, amount, status, psp_reference)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (transfer_id) DO UPDATE SET status = excluded.status, psp_reference = excluded.psp_reference`,
        [input.transferId, input.toAccount, input.amount, input.status, input.pspReference],
      );
      const outboxPayload = {
        transferId: input.transferId,
        outcome: input.status === 'sent' ? 'succeeded' : 'failed',
        pspReference: input.pspReference,
      };
      await client.query("INSERT INTO outbox (topic, payload) VALUES ('payout-result', $1)", [JSON.stringify(outboxPayload)]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async get(transferId: string): Promise<PayoutAttemptRecord | null> {
    const result = await this.pool.query('SELECT * FROM payout_attempts WHERE transfer_id = $1', [transferId]);
    if (result.rows.length === 0) {
      return null;
    }
    return this.toRecord(result.rows[0]);
  }

  async listFailed(options: { limit: number; offset: number }): Promise<PayoutAttemptRecord[]> {
    const result = await this.pool.query(
      "SELECT * FROM payout_attempts WHERE status = 'failed' ORDER BY created_at ASC LIMIT $1 OFFSET $2",
      [options.limit, options.offset],
    );
    return result.rows.map((row) => this.toRecord(row));
  }

  private toRecord(row: Record<string, unknown>): PayoutAttemptRecord {
    return {
      transferId: row.transfer_id as string,
      toAccount: row.to_account as string,
      amount: Number(row.amount),
      status: row.status as 'sent' | 'failed',
      pspReference: row.psp_reference as string | null,
      createdAt: row.created_at as Date,
    };
  }
}
