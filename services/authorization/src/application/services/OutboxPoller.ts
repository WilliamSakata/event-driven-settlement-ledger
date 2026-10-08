import { OutboxRepositoryPort } from '../ports/OutboxRepositoryPort';
import { KafkaProducerPort } from '../ports/KafkaProducerPort';

export class OutboxPoller {
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly outboxRepository: OutboxRepositoryPort,
    private readonly producer: KafkaProducerPort,
    private readonly intervalMs: number = 500,
  ) {}

  start(): void {
    this.timer = setInterval(() => {
      this.pollOnce().catch((error) => console.error('outbox poll failed', error));
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async pollOnce(): Promise<void> {
    const rows = await this.outboxRepository.findUnpublished(20);
    for (const row of rows) {
      const key = this.extractKey(row);
      await this.producer.publish(row.topic, key, row.payload);
      await this.outboxRepository.markPublished(row.id);
    }
  }

  private extractKey(row: { id: string; payload: unknown }): string {
    if (typeof row.payload === 'object' && row.payload !== null && 'transferId' in row.payload) {
      const value = (row.payload as { transferId: unknown }).transferId;
      if (typeof value === 'string') {
        return value;
      }
    }
    return row.id;
  }
}
