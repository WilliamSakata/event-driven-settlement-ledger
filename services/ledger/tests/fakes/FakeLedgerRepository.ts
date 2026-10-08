import { LedgerRepositoryPort, LedgerEntryRecord } from '../../src/application/ports/LedgerRepositoryPort';

export class FakeLedgerRepository implements LedgerRepositoryPort {
  public readonly processed = new Set<string>();
  public postSettlementCalls = 0;

  async wasProcessed(transferId: string): Promise<boolean> {
    return this.processed.has(transferId);
  }

  async postSettlement(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): Promise<void> {
    this.postSettlementCalls += 1;
    this.processed.add(input.transferId);
  }

  async listEntriesForAccount(_accountId: string): Promise<LedgerEntryRecord[]> {
    return [];
  }
}
