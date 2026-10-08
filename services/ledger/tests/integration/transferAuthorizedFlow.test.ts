import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresLedgerRepository } from '../../src/adapters/outbound/postgres/PostgresLedgerRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { PostSettlement } from '../../src/application/use-cases/PostSettlement';
import { startTransferAuthorizedConsumer, TRANSFER_AUTHORIZED_TOPIC } from '../../src/adapters/inbound/kafka/transferAuthorizedConsumer';
import { createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 10000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=ledger' });
  await pool.query('TRUNCATE ledger_entries, processed_transfers, outbox, dlq_events CASCADE');

  const kafka = createKafka(KAFKA_BROKERS, 'ledger-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TRANSFER_AUTHORIZED_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  const ledgerRepository = new PostgresLedgerRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const postSettlement = new PostSettlement(ledgerRepository, dlqRepository);

  const consumer = kafka.consumer({ groupId: `ledger-flow-test-${Date.now()}` });
  await consumer.connect();
  await startTransferAuthorizedConsumer(consumer, postSettlement);

  // give the consumer group time to finish joining before any test publishes
  await new Promise((resolve) => setTimeout(resolve, 5000));

  (global as Record<string, unknown>).__producer = producer;
}, 30000);

afterAll(async () => {
  await pool.end();
});

describe('transfer-authorized consumption (integration)', () => {
  it('writes double-entry ledger entries when a transfer-authorized message is consumed', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: TRANSFER_AUTHORIZED_TOPIC,
      messages: [{ key: 'lt1', value: JSON.stringify({ transferId: 'lt1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 250 }) }],
    });

    const rows = await waitFor(async () => {
      const result = await pool.query("SELECT * FROM ledger_entries WHERE transfer_id = 'lt1'");
      return result.rows.length === 2 ? result.rows : null;
    });

    expect(rows).toHaveLength(2);

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'settlement-posted'");
    expect(outbox.rows).toHaveLength(1);
  }, 30000);
});
