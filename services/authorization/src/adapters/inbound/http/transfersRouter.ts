import { Router, Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import { RequestTransfer } from '../../../application/use-cases/RequestTransfer';
import { TransferRepositoryPort } from '../../../application/ports/TransferRepositoryPort';

const requestTransferSchema = z.object({
  transferId: z.string().min(1),
  fromAccount: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
});

export function createTransfersRouter(
  requestTransfer: RequestTransfer,
  transferRepository: TransferRepositoryPort,
): Router {
  const router = Router();

  router.post('/transfers', async (req: Request, res: Response, next: NextFunction) => {
    const parseResult = requestTransferSchema.safeParse(req.body);
    if (!parseResult.success) {
      res.status(400).json({ error: 'invalid request body', details: parseResult.error.issues });
      return;
    }
    try {
      const result = await requestTransfer.execute(parseResult.data);
      res.status(201).json(result);
    } catch (error) {
      next(error);
    }
  });

  router.get('/transfers/:transferId', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const transfer = await transferRepository.findById(req.params.transferId);
      if (transfer === null) {
        res.status(404).json({ error: 'transfer not found' });
        return;
      }
      res.status(200).json(transfer);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
