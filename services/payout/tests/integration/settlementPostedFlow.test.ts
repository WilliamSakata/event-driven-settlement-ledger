import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { Server } from 'node:http';
import { createMockPayoutPspApp } from '../../../../mock-payout-psp/server';
import { PostgresPayoutRepository } from '../../src/adapters/outbound/postgres/PostgresPayoutRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { HttpPspClient } from '../../src/adapters/outbound/psp/HttpPspClient';
import { SendPayout } from '../../src/application/use-cases/SendPayout';
import { startSettlementPostedConsumer, SETTLEMENT_POSTED_TOPIC } from '../../src/adapters/inbound/kafka/settlementPostedConsumer';
import { createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;
let pspServer: Server;

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
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  await pool.query('TRUNCATE payout_attempts, outbox, dlq_events CASCADE');

  const pspApp = createMockPayoutPspApp();
  await new Promise<void>((resolve) => {
    pspServer = pspApp.listen(0, () => resolve());
  });
  const address = pspServer.address();
  const pspPort = typeof address === 'object' && address !== null ? address.port : 0;

  const kafka = createKafka(KAFKA_BROKERS, 'payout-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: SETTLEMENT_POSTED_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  const payoutRepository = new PostgresPayoutRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const pspClient = new HttpPspClient(`http://localhost:${pspPort}`);
  const sendPayout = new SendPayout(payoutRepository, pspClient, dlqRepository);

  const consumer = kafka.consumer({ groupId: `payout-flow-test-${Date.now()}` });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, sendPayout);

  await new Promise((resolve) => setTimeout(resolve, 5000));

  (global as Record<string, unknown>).__producer = producer;
}, 30000);

afterAll(async () => {
  await pool.end();
  pspServer.close();
});

describe('settlement-posted consumption in payout (integration)', () => {
  it('sends the payout and records a sent attempt for a normal account', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: SETTLEMENT_POSTED_TOPIC,
      messages: [{
        key: 'pt1',
        value: JSON.stringify({ transferId: 'pt1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 200, postedAt: new Date().toISOString() }),
      }],
    });

    const row = await waitFor(async () => {
      const result = await pool.query("SELECT * FROM payout_attempts WHERE transfer_id = 'pt1'");
      return result.rows[0] ?? null;
    });

    expect(row.status).toBe('sent');
  }, 30000);

  it('records a failed attempt for the magic failure account, without a DLQ entry', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: SETTLEMENT_POSTED_TOPIC,
      messages: [{
        key: 'pt2',
        value: JSON.stringify({ transferId: 'pt2', fromAccount: 'acc_1', toAccount: 'acc_psp_fail_demo', amount: 200, postedAt: new Date().toISOString() }),
      }],
    });

    const row = await waitFor(async () => {
      const result = await pool.query("SELECT * FROM payout_attempts WHERE transfer_id = 'pt2'");
      return result.rows[0] ?? null;
    });

    expect(row.status).toBe('failed');

    const dlq = await pool.query("SELECT * FROM dlq_events WHERE transfer_id = 'pt2'");
    expect(dlq.rows).toHaveLength(0);
  }, 30000);
});
