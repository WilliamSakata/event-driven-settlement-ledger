import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresTransferRepository } from './adapters/outbound/postgres/PostgresTransferRepository';
import { PostgresBalanceProjection } from './adapters/outbound/postgres/PostgresBalanceProjection';
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { PostgresSettlementRepository } from './adapters/outbound/postgres/PostgresSettlementRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { RequestTransfer } from './application/use-cases/RequestTransfer';
import { ApplySettlementToProjection } from './application/use-cases/ApplySettlementToProjection';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { OutboxPoller } from './application/services/OutboxPoller';
import { startSettlementPostedConsumer } from './adapters/inbound/kafka/settlementPostedConsumer';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=authz' });

  const kafka = createKafka(env.kafkaBrokers, 'authorization');
  const producer = kafka.producer();
  await producer.connect();
  const producerAdapter = new KafkaProducerAdapter(producer);

  const transferRepository = new PostgresTransferRepository(pool);
  const balanceProjection = new PostgresBalanceProjection(pool);
  const outboxRepository = new PostgresOutboxRepository(pool);
  const settlementRepository = new PostgresSettlementRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);

  const requestTransfer = new RequestTransfer(transferRepository, balanceProjection);
  const applySettlementToProjection = new ApplySettlementToProjection(settlementRepository, dlqRepository);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, producerAdapter);

  const outboxPoller = new OutboxPoller(outboxRepository, producerAdapter);
  outboxPoller.start();

  const consumer = kafka.consumer({ groupId: 'authorization' });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, applySettlementToProjection);

  const app = createApp({ requestTransfer, transferRepository, dlqRepository, reprocessDlqEvent });
  app.listen(env.port, () => {
    console.log(`authorization listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
