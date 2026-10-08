import { Pool } from 'pg';
import { OutboxRepositoryPort, OutboxRow } from '../../../application/ports/OutboxRepositoryPort';

export class PostgresOutboxRepository implements OutboxRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async findUnpublished(limit: number): Promise<OutboxRow[]> {
    const result = await this.pool.query(
      'SELECT id, topic, payload FROM outbox WHERE published_at IS NULL ORDER BY created_at ASC LIMIT $1',
      [limit],
    );
    return result.rows.map((row) => ({ id: row.id, topic: row.topic, payload: row.payload }));
  }

  async markPublished(id: string): Promise<void> {
    await this.pool.query('UPDATE outbox SET published_at = now() WHERE id = $1', [id]);
  }
}
