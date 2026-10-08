import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresTransferRepository } from './adapters/outbound/postgres/PostgresTransferRepository';
import { PostgresBalanceProjection } from './adapters/outbound/postgres/PostgresBalanceProjection';
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { RequestTransfer } from './application/use-cases/RequestTransfer';
import { OutboxPoller } from './application/services/OutboxPoller';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=authz' });

  const kafka = createKafka(env.kafkaBrokers, 'authorization');
  const producer = kafka.producer();
  await producer.connect();

  const transferRepository = new PostgresTransferRepository(pool);
  const balanceProjection = new PostgresBalanceProjection(pool);
  const outboxRepository = new PostgresOutboxRepository(pool);
  const requestTransfer = new RequestTransfer(transferRepository, balanceProjection);

  const outboxPoller = new OutboxPoller(outboxRepository, new KafkaProducerAdapter(producer));
  outboxPoller.start();

  const app = createApp({ requestTransfer, transferRepository });
  app.listen(env.port, () => {
    console.log(`authorization listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
