import { validateSettlementPosted } from '../../domain/services/SettlementPostedValidator';
import { PspClientPort } from '../ports/PspClientPort';
import { PayoutRepositoryPort } from '../ports/PayoutRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';

export interface SendPayoutOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class SendPayout {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;

  constructor(
    private readonly payoutRepository: PayoutRepositoryPort,
    private readonly pspClient: PspClientPort,
    private readonly dlqRepository: DlqRepositoryPort,
    options: SendPayoutOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  }

  async execute(rawPayload: unknown): Promise<void> {
    const transferId = this.extractTransferId(rawPayload);
    if (transferId !== null && (await this.payoutRepository.wasProcessed(transferId))) {
      return;
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validateSettlementPosted(rawPayload);
        const result = await this.pspClient.send({ transferId: event.transferId, toAccount: event.toAccount, amount: event.amount });
        await this.payoutRepository.savePayoutAttempt({
          transferId: event.transferId,
          toAccount: event.toAccount,
          amount: event.amount,
          status: result.succeeded ? 'sent' : 'failed',
          pspReference: result.pspReference,
        });
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.maxAttempts) {
          await this.delay(this.backoffMs(attempt));
        }
      }
    }

    await this.dlqRepository.add({
      transferId: transferId ?? 'unknown',
      topic: 'settlement-posted',
      payload: rawPayload,
      failureReason: lastError.message,
      attempts: this.maxAttempts,
    });
  }

  private extractTransferId(rawPayload: unknown): string | null {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'transferId' in rawPayload) {
      const value = (rawPayload as { transferId: unknown }).transferId;
      return typeof value === 'string' ? value : null;
    }
    return null;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
