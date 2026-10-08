export interface OutboxRow {
  id: string;
  topic: string;
  payload: unknown;
}

export interface OutboxRepositoryPort {
  findUnpublished(limit: number): Promise<OutboxRow[]>;
  markPublished(id: string): Promise<void>;
}
