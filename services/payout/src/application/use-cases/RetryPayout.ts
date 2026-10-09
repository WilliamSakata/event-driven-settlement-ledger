import { PayoutRepositoryPort, PayoutAttemptRecord } from '../ports/PayoutRepositoryPort';
import { PspClientPort } from '../ports/PspClientPort';

export class PayoutAttemptNotFoundError extends Error {}

export class RetryPayout {
  constructor(
    private readonly payoutRepository: PayoutRepositoryPort,
    private readonly pspClient: PspClientPort,
  ) {}

  async execute(transferId: string): Promise<PayoutAttemptRecord> {
    const existing = await this.payoutRepository.get(transferId);
    if (existing === null) {
      throw new PayoutAttemptNotFoundError(`no payout attempt found for transferId "${transferId}"`);
    }

    const result = await this.pspClient.send({ transferId, toAccount: existing.toAccount, amount: existing.amount });
    await this.payoutRepository.savePayoutAttempt({
      transferId,
      toAccount: existing.toAccount,
      amount: existing.amount,
      status: result.succeeded ? 'sent' : 'failed',
      pspReference: result.pspReference,
    });

    const updated = await this.payoutRepository.get(transferId);
    return updated as PayoutAttemptRecord;
  }
}
