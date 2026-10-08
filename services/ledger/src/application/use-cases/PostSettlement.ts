import { validateTransferAuthorized } from '../../domain/services/TransferAuthorizedValidator';
import { buildDoubleEntryLines, isBalanced } from '../../domain/services/DoubleEntry';
import { LedgerRepositoryPort } from '../ports/LedgerRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';

export interface PostSettlementOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class PostSettlement {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;

  constructor(
    private readonly ledgerRepository: LedgerRepositoryPort,
    private readonly dlqRepository: DlqRepositoryPort,
    options: PostSettlementOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  }

  async execute(rawPayload: unknown): Promise<void> {
    const transferId = this.extractTransferId(rawPayload);
    if (transferId !== null && (await this.ledgerRepository.wasProcessed(transferId))) {
      return;
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validateTransferAuthorized(rawPayload);
        const lines = buildDoubleEntryLines(event);
        if (!isBalanced(lines)) {
          throw new Error('double-entry lines are not balanced');
        }
        await this.ledgerRepository.postSettlement(event);
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
      topic: 'transfer-authorized',
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
