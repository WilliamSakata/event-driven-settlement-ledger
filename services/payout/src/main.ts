import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresPayoutRepository } from './adapters/outbound/postgres/PostgresPayoutRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { HttpPspClient } from './adapters/outbound/psp/HttpPspClient';
import { SendPayout } from './application/use-cases/SendPayout';
import { RetryPayout } from './application/use-cases/RetryPayout';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { startSettlementPostedConsumer } from './adapters/inbound/kafka/settlementPostedConsumer';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=payout' });

  const kafka = createKafka(env.kafkaBrokers, 'payout');
  const producer = kafka.producer();
  await producer.connect();
  const producerAdapter = new KafkaProducerAdapter(producer);

  const payoutRepository = new PostgresPayoutRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const pspClient = new HttpPspClient(env.pspBaseUrl);

  const sendPayout = new SendPayout(payoutRepository, pspClient, dlqRepository);
  const retryPayout = new RetryPayout(payoutRepository, pspClient);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, producerAdapter);

  const consumer = kafka.consumer({ groupId: 'payout' });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, sendPayout);

  const app = createApp({ payoutRepository, retryPayout, dlqRepository, reprocessDlqEvent });
  app.listen(env.port, () => {
    console.log(`payout listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
