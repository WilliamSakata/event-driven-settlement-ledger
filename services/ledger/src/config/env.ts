export interface AppEnv {
  port: number;
  databaseUrl: string;
  kafkaBrokers: string[];
}

export function loadEnv(): AppEnv {
  const rawPort = process.env.PORT;
  const port = rawPort === undefined ? 3002 : Number(rawPort);
  if (Number.isNaN(port)) {
    throw new Error(`Invalid PORT environment variable: "${rawPort}" is not a number`);
  }

  return {
    port,
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  };
}
