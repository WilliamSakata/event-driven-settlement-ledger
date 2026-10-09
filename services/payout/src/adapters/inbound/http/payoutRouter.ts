import { Router, Request, Response, NextFunction } from 'express';
import { PayoutRepositoryPort } from '../../../application/ports/PayoutRepositoryPort';
import { RetryPayout, PayoutAttemptNotFoundError } from '../../../application/use-cases/RetryPayout';

export function createPayoutRouter(payoutRepository: PayoutRepositoryPort, retryPayout: RetryPayout): Router {
  const router = Router();

  router.get('/payout/failed', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 20;
      const offset = typeof req.query.offset === 'string' ? Number(req.query.offset) : 0;
      if (!Number.isFinite(limit) || limit < 0 || !Number.isFinite(offset) || offset < 0) {
        res.status(400).json({ error: 'invalid limit or offset' });
        return;
      }
      const attempts = await payoutRepository.listFailed({ limit, offset });
      res.status(200).json({ attempts });
    } catch (error) {
      next(error);
    }
  });

  router.post('/payout/:transferId/retry', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const result = await retryPayout.execute(req.params.transferId);
      res.status(200).json(result);
    } catch (error) {
      if (error instanceof PayoutAttemptNotFoundError) {
        res.status(404).json({ error: 'payout attempt not found' });
        return;
      }
      next(error);
    }
  });

  return router;
}
