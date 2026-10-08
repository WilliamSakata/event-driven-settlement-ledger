export interface DlqEntry {
  id: string;
  transferId: string;
  topic: string;
  payload: unknown;
  failureReason: string;
  attempts: number;
  createdAt: Date;
  reprocessedAt: Date | null;
}

export interface DlqRepositoryPort {
  add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string>;
  list(options: { limit: number; offset: number }): Promise<DlqEntry[]>;
  get(id: string): Promise<DlqEntry | null>;
  markReprocessed(id: string): Promise<void>;
}
