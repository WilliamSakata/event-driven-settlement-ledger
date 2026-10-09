import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresSettlementRepository } from '../../src/adapters/outbound/postgres/PostgresSettlementRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { ApplySettlementToProjection } from '../../src/application/use-cases/ApplySettlementToProjection';
import { startSettlementPostedConsumer, SETTLEMENT_POSTED_TOPIC } from '../../src/adapters/inbound/kafka/settlementPostedConsumer';
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
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  await pool.query('TRUNCATE transfers, reservations, balance_projection, outbox, dlq_events CASCADE');
  await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('st1', 'acc_1', 'acc_2', 300, 'approved')");
  await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('st1', 'acc_1', 300, 'pending')");
  await pool.query("INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ('acc_1', 1000)");

  const kafka = createKafka(KAFKA_BROKERS, 'authorization-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: SETTLEMENT_POSTED_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  const settlementRepository = new PostgresSettlementRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const applySettlementToProjection = new ApplySettlementToProjection(settlementRepository, dlqRepository);

  const consumer = kafka.consumer({ groupId: `authorization-flow-test-${Date.now()}` });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, applySettlementToProjection);

  await new Promise((resolve) => setTimeout(resolve, 5000));

  (global as Record<string, unknown>).__producer = producer;
}, 30000);

afterAll(async () => {
  await pool.end();
});

describe('settlement-posted consumption (integration)', () => {
  it('confirms the transfer and updates both balances when the message is consumed', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: SETTLEMENT_POSTED_TOPIC,
      messages: [{
        key: 'st1',
        value: JSON.stringify({ transferId: 'st1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 300, postedAt: new Date().toISOString() }),
      }],
    });

    const transfer = await waitFor(async () => {
      const result = await pool.query("SELECT status FROM transfers WHERE id = 'st1' AND status = 'confirmed'");
      return result.rows[0] ?? null;
    });

    expect(transfer.status).toBe('confirmed');

    const balance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_2'");
    expect(Number(balance.rows[0].confirmed_balance)).toBe(300);
  }, 30000);
});
