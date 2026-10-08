import { Router, Request, Response, NextFunction } from 'express';
import { LedgerRepositoryPort } from '../../../application/ports/LedgerRepositoryPort';

export function createAccountsRouter(ledgerRepository: LedgerRepositoryPort): Router {
  const router = Router();

  router.get('/accounts/:accountId/entries', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const entries = await ledgerRepository.listEntriesForAccount(req.params.accountId);
      res.status(200).json({ entries });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
