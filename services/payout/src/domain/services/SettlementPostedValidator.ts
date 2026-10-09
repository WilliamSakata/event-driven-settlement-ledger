import { z } from 'zod';

const settlementPostedSchema = z.object({
  transferId: z.string().min(1),
  fromAccount: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
  postedAt: z.string().min(1),
});

export type SettlementPostedEvent = z.infer<typeof settlementPostedSchema>;

export function validateSettlementPosted(raw: unknown): SettlementPostedEvent {
  return settlementPostedSchema.parse(raw);
}
