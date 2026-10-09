import { PayoutRepositoryPort, PayoutAttemptInput, PayoutAttemptRecord } from '../../src/application/ports/PayoutRepositoryPort';

export class FakePayoutRepository implements PayoutRepositoryPort {
  private readonly attempts = new Map<string, PayoutAttemptRecord>();
  public saveCalls = 0;

  async wasProcessed(transferId: string): Promise<boolean> {
    return this.attempts.has(transferId);
  }

  async savePayoutAttempt(input: PayoutAttemptInput): Promise<void> {
    this.saveCalls += 1;
    this.attempts.set(input.transferId, { ...input, createdAt: new Date() });
  }

  async get(transferId: string): Promise<PayoutAttemptRecord | null> {
    return this.attempts.get(transferId) ?? null;
  }

  async listFailed(): Promise<PayoutAttemptRecord[]> {
    return [...this.attempts.values()].filter((attempt) => attempt.status === 'failed');
  }
}
