import { Pool } from 'pg';
import { LedgerRepositoryPort, LedgerEntryRecord } from '../../../application/ports/LedgerRepositoryPort';

export class PostgresLedgerRepository implements LedgerRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async wasProcessed(transferId: string): Promise<boolean> {
    const result = await this.pool.query('SELECT 1 FROM processed_transfers WHERE transfer_id = $1', [transferId]);
    return result.rows.length > 0;
  }

  async postSettlement(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        "INSERT INTO ledger_entries (transfer_id, account_id, direction, amount) VALUES ($1, $2, 'debit', $3)",
        [input.transferId, input.fromAccount, input.amount],
      );
      await client.query(
        "INSERT INTO ledger_entries (transfer_id, account_id, direction, amount) VALUES ($1, $2, 'credit', $3)",
        [input.transferId, input.toAccount, input.amount],
      );
      await client.query('INSERT INTO processed_transfers (transfer_id) VALUES ($1)', [input.transferId]);
      const outboxPayload = {
        transferId: input.transferId,
        fromAccount: input.fromAccount,
        toAccount: input.toAccount,
        amount: input.amount,
        postedAt: new Date().toISOString(),
      };
      await client.query("INSERT INTO outbox (topic, payload) VALUES ('settlement-posted', $1)", [JSON.stringify(outboxPayload)]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async listEntriesForAccount(accountId: string): Promise<LedgerEntryRecord[]> {
    const result = await this.pool.query(
      'SELECT id, transfer_id, account_id, direction, amount, created_at FROM ledger_entries WHERE account_id = $1 ORDER BY created_at ASC',
      [accountId],
    );
    return result.rows.map((row) => ({
      id: row.id,
      transferId: row.transfer_id,
      accountId: row.account_id,
      direction: row.direction,
      amount: Number(row.amount),
      createdAt: row.created_at,
    }));
  }
}
