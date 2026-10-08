import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';
import { KafkaProducerPort } from '../ports/KafkaProducerPort';

export class DlqEntryNotFoundError extends Error {}

export class ReprocessDlqEvent {
  constructor(
    private readonly dlqRepository: DlqRepositoryPort,
    private readonly producer: KafkaProducerPort,
  ) {}

  async execute(id: string): Promise<void> {
    const entry = await this.dlqRepository.get(id);
    if (!entry) {
      throw new DlqEntryNotFoundError(`no DLQ entry found with id "${id}"`);
    }
    await this.producer.publish(entry.topic, entry.transferId, entry.payload);
    await this.dlqRepository.markReprocessed(id);
  }
}
