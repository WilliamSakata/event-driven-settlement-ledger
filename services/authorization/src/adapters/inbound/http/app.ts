import express, { Express, Request, Response, NextFunction } from 'express';
import { RequestTransfer } from '../../../application/use-cases/RequestTransfer';
import { TransferRepositoryPort } from '../../../application/ports/TransferRepositoryPort';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent } from '../../../application/use-cases/ReprocessDlqEvent';
import { createTransfersRouter } from './transfersRouter';
import { createDlqRouter } from './dlqRouter';

export interface AppDependencies {
  requestTransfer?: RequestTransfer;
  transferRepository?: TransferRepositoryPort;
  dlqRepository?: DlqRepositoryPort;
  reprocessDlqEvent?: ReprocessDlqEvent;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  if (deps.requestTransfer && deps.transferRepository) {
    app.use(createTransfersRouter(deps.requestTransfer, deps.transferRepository));
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
