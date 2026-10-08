export interface KafkaProducerPort {
  publish(topic: string, key: string, payload: unknown): Promise<void>;
}
