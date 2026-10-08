export type TransferStatus = 'approved' | 'rejected' | 'confirmed' | 'released';

export interface TransferRecord {
  id: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
  status: TransferStatus;
}

export interface ReservationInput {
  accountId: string;
  amount: number;
}

export interface TransferRepositoryPort {
  findById(id: string): Promise<TransferRecord | null>;
  saveApproved(transfer: TransferRecord, reservation: ReservationInput, outboxPayload: unknown): Promise<void>;
  saveRejected(transfer: TransferRecord): Promise<void>;
}
