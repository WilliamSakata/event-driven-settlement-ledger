import { DlqRepositoryPort, DlqEntry } from '../../src/application/ports/DlqRepositoryPort';

export class FakeDlqRepository implements DlqRepositoryPort {
  public readonly entries: DlqEntry[] = [];

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const id = `dlq-${this.entries.length + 1}`;
    this.entries.push({ id, ...entry, createdAt: new Date(), reprocessedAt: null });
    return id;
  }

  async list(): Promise<DlqEntry[]> {
    return this.entries;
  }

  async get(id: string): Promise<DlqEntry | null> {
    return this.entries.find((entry) => entry.id === id) ?? null;
  }

  async markReprocessed(id: string): Promise<void> {
    const entry = this.entries.find((e) => e.id === id);
    if (entry) {
      entry.reprocessedAt = new Date();
    }
  }
}
