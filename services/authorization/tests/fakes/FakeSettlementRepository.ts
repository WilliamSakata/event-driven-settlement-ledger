import { SettlementRepositoryPort, SettlementEvent } from '../../src/application/ports/SettlementRepositoryPort';

export class FakeSettlementRepository implements SettlementRepositoryPort {
  public readonly confirmed = new Set<string>();
  public confirmSettlementCalls = 0;

  async wasConfirmed(transferId: string): Promise<boolean> {
    return this.confirmed.has(transferId);
  }

  async confirmSettlement(event: SettlementEvent): Promise<void> {
    this.confirmSettlementCalls += 1;
    this.confirmed.add(event.transferId);
  }
}
