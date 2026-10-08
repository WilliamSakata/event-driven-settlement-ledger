import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresLedgerRepository } from './adapters/outbound/postgres/PostgresLedgerRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { PostSettlement } from './application/use-cases/PostSettlement';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { OutboxPoller } from './application/services/OutboxPoller';
import { startTransferAuthorizedConsumer } from './adapters/inbound/kafka/transferAuthorizedConsumer';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=ledger' });

  const kafka = createKafka(env.kafkaBrokers, 'ledger');
  const producer = kafka.producer();
  await producer.connect();
  const producerAdapter = new KafkaProducerAdapter(producer);

  const ledgerRepository = new PostgresLedgerRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const postSettlement = new PostSettlement(ledgerRepository, dlqRepository);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, producerAdapter);

  const outboxRepository = new PostgresOutboxRepository(pool);
  const outboxPoller = new OutboxPoller(outboxRepository, producerAdapter);
  outboxPoller.start();

  const consumer = kafka.consumer({ groupId: 'ledger' });
  await consumer.connect();
  await startTransferAuthorizedConsumer(consumer, postSettlement);

  const app = createApp({ ledgerRepository, dlqRepository, reprocessDlqEvent });
  app.listen(env.port, () => {
    console.log(`ledger listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
