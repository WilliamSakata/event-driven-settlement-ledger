import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';

async function main(): Promise<void> {
  const env = loadEnv();

  const app = createApp({});
  app.listen(env.port, () => {
    console.log(`ledger listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
