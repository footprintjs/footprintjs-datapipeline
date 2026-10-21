import { RepositoryRetentionError } from './contracts.js';
export const MAX_RETAINED_REQUESTS = 32;
/** Bounded per-instance ordering; SQLite still arbitrates independent processes. */
export class RepositorySerial {
  pending = 0;
  private tail: Promise<void> = Promise.resolve();
  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.pending >= MAX_RETAINED_REQUESTS) throw new RepositoryRetentionError('REPOSITORY_BUSY');
    this.pending++;
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>(resolve => { release = resolve; });
    await previous;
    try { return await fn(); }
    finally { this.pending--; release(); }
  }
}
