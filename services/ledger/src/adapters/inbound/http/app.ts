import express, { Express, Request, Response, NextFunction } from 'express';
import { LedgerRepositoryPort } from '../../../application/ports/LedgerRepositoryPort';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent } from '../../../application/use-cases/ReprocessDlqEvent';
import { createAccountsRouter } from './accountsRouter';
import { createDlqRouter } from './dlqRouter';

export interface AppDependencies {
  ledgerRepository?: LedgerRepositoryPort;
  dlqRepository?: DlqRepositoryPort;
  reprocessDlqEvent?: ReprocessDlqEvent;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  if (deps.ledgerRepository) {
    app.use(createAccountsRouter(deps.ledgerRepository));
  }
  if (deps.dlqRepository && deps.reprocessDlqEvent) {
    app.use(createDlqRouter(deps.dlqRepository, deps.reprocessDlqEvent));
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
