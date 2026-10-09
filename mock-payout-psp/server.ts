import express, { Express, Request, Response } from 'express';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';

const sendSchema = z.object({
  transferId: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
});

const FAILING_ACCOUNT = 'acc_psp_fail_demo';

export function createMockPayoutPspApp(): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  app.post('/psp/send', (req: Request, res: Response) => {
    const parseResult = sendSchema.safeParse(req.body);
    if (!parseResult.success) {
      res.status(400).json({ error: 'invalid request body' });
      return;
    }

    if (parseResult.data.toAccount === FAILING_ACCOUNT) {
      res.status(200).json({ accepted: false, reason: 'destination account rejected the payment' });
      return;
    }

    res.status(200).json({ accepted: true, pspReference: randomUUID() });
  });

  return app;
}

if (require.main === module) {
  const app = createMockPayoutPspApp();
  const port = process.env.PORT === undefined ? 4003 : Number(process.env.PORT);
  app.listen(port, () => {
    console.log(`mock-payout-psp listening on port ${port}`);
  });
}
