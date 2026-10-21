import type { RecordRef, StoredRecord } from '../../contracts.js';
import { MAX_REPOSITORY_READ_BATCH } from '../../ports.js';
import { validRef } from '../filesystemRecords.js';
import { RepositoryRetentionError } from './contracts.js';
type Request = { ref: RecordRef; resolve: (value: StoredRecord | undefined) => void; reject: (error: unknown) => void };
/** Share bounded I/O only; every reference retains its own failure and result. No cache. */
export class RepositoryReadBatcher {
  private readonly pending: Request[] = [];
  busy = false;
  constructor(private readonly load: (refs: readonly RecordRef[]) => Promise<readonly (StoredRecord | undefined)[]>) {}
  read(ref: RecordRef): Promise<StoredRecord | undefined> {
    if (!validRef(ref)) return Promise.reject(new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS'));
    if (this.pending.length >= 8192) return Promise.reject(new RepositoryRetentionError('REPOSITORY_BUSY'));
    const result = new Promise<StoredRecord | undefined>((resolve, reject) => this.pending.push({ref, resolve, reject}));
    if (!this.busy) { this.busy = true; queueMicrotask(() => { void this.flush(); }); }
    return result;
  }
  private async flush(): Promise<void> {
    try {
      while (this.pending.length) {
        const chunk = this.pending.splice(0, MAX_REPOSITORY_READ_BATCH);
        try { const rows = await this.load(chunk.map(row=>row.ref)); chunk.forEach((row,i)=>row.resolve(rows[i])); }
        catch {
          // Bulk refusal must not mark unrelated claims expired/corrupt.
          for (const request of chunk) {
            try { request.resolve((await this.load([request.ref]))[0]); }
            catch (error) { request.reject(error); }
          }
        }
      }
    } finally { this.busy = false; }
  }
}
