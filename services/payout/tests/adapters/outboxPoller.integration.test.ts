import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { OutboxPoller } from '../../src/application/services/OutboxPoller';
import { PostgresOutboxRepository } from '../../src/adapters/outbound/postgres/PostgresOutboxRepository';
import { KafkaProducerAdapter, createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');
const TOPIC = 'payout-result';

let pool: Pool;
let kafka: Kafka;
let poller: OutboxPoller;

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  kafka = createKafka(KAFKA_BROKERS, 'payout-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  poller = new OutboxPoller(new PostgresOutboxRepository(pool), new KafkaProducerAdapter(producer));
}, 30000);

beforeEach(async () => {
  await pool.query('TRUNCATE outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('OutboxPoller (integration)', () => {
  it('publishes an unpublished outbox row to Kafka and marks it published', async () => {
    await pool.query(
      "INSERT INTO outbox (topic, payload) VALUES ($1, $2)",
      [TOPIC, JSON.stringify({ transferId: 'ob1', outcome: 'succeeded', pspReference: 'ref-1' })],
    );

    const consumer = kafka.consumer({ groupId: `outbox-test-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    const received: unknown[] = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        received.push(JSON.parse(message.value?.toString() ?? '{}'));
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 5000));

    await poller.pollOnce();

    const start = Date.now();
    while (received.length === 0 && Date.now() - start < 10000) {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    expect(received).toEqual([{ transferId: 'ob1', outcome: 'succeeded', pspReference: 'ref-1' }]);

    const row = await pool.query("SELECT published_at FROM outbox WHERE topic = $1", [TOPIC]);
    expect(row.rows[0].published_at).not.toBeNull();

    await consumer.disconnect();
  }, 30000);
});
