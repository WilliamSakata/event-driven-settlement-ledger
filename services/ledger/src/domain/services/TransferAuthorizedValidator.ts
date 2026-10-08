import { z } from 'zod';

const transferAuthorizedSchema = z.object({
  transferId: z.string().min(1),
  fromAccount: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
});

export type TransferAuthorizedEvent = z.infer<typeof transferAuthorizedSchema>;

export function validateTransferAuthorized(raw: unknown): TransferAuthorizedEvent {
  return transferAuthorizedSchema.parse(raw);
}
