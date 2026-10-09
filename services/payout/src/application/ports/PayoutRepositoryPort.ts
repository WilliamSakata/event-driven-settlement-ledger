export type PayoutStatus = 'sent' | 'failed';

export interface PayoutAttemptInput {
  transferId: string;
  toAccount: string;
  amount: number;
  status: PayoutStatus;
  pspReference: string | null;
}

export interface PayoutAttemptRecord extends PayoutAttemptInput {
  createdAt: Date;
}

export interface PayoutRepositoryPort {
  wasProcessed(transferId: string): Promise<boolean>;
  savePayoutAttempt(input: PayoutAttemptInput): Promise<void>;
  get(transferId: string): Promise<PayoutAttemptRecord | null>;
  listFailed(options: { limit: number; offset: number }): Promise<PayoutAttemptRecord[]>;
}
