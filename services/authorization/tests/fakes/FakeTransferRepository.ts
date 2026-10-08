import {
  TransferRepositoryPort,
  TransferRecord,
  ReservationInput,
} from '../../src/application/ports/TransferRepositoryPort';

export class FakeTransferRepository implements TransferRepositoryPort {
  private readonly transfers = new Map<string, TransferRecord>();
  public savedApprovedCalls = 0;
  public savedRejectedCalls = 0;

  async findById(id: string): Promise<TransferRecord | null> {
    return this.transfers.get(id) ?? null;
  }

  async saveApproved(transfer: TransferRecord, _reservation: ReservationInput, _outboxPayload: unknown): Promise<void> {
    this.transfers.set(transfer.id, transfer);
    this.savedApprovedCalls += 1;
  }

  async saveRejected(transfer: TransferRecord): Promise<void> {
    this.transfers.set(transfer.id, transfer);
    this.savedRejectedCalls += 1;
  }
}
