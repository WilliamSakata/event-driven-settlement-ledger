import { Kafka, Producer } from 'kafkajs';
import { KafkaProducerPort } from '../../../application/ports/KafkaProducerPort';

export function createKafka(brokers: string[], clientId: string): Kafka {
  return new Kafka({ clientId, brokers });
}

export class KafkaProducerAdapter implements KafkaProducerPort {
  constructor(private readonly producer: Producer) {}

  async publish(topic: string, key: string, payload: unknown): Promise<void> {
    await this.producer.send({ topic, messages: [{ key, value: JSON.stringify(payload) }] });
  }
}
