import { lstatSync, statfsSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError, type RepositoryMetadataCapacity, type RepositoryMetadataLimits } from './contracts.js';
const PAGE_BYTES = 4096;
// Reserve fixed fence files/sidecars, journal headers and supported allocation rounding.
const CONTROL_RESERVE_BYTES = 1024 * 1024;
const CONTROL_FILES = ['retention.sqlite', 'retention.sqlite-journal', 'retention.sqlite-wal', 'retention.sqlite-shm'];
export function checkedMetadataLimits(limits: RepositoryMetadataLimits): void {
  if (!limits || Object.keys(limits).some(key => !['maxBytes', 'maxOperations', 'maxReferences'].includes(key)) ||
    !Number.isSafeInteger(limits.maxBytes) || limits.maxBytes < 2 * CONTROL_RESERVE_BYTES || limits.maxBytes > 2 ** 40 ||
    !Number.isSafeInteger(limits.maxOperations) || limits.maxOperations < 1 ||
    !Number.isSafeInteger(limits.maxReferences) || limits.maxReferences < 1) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
}
/** Only call at a ledger-owned SQLite write boundary, never for publication errors. */
export function metadataWriteFailure(error: unknown): never {
  const code: unknown = error && typeof error === 'object' ? Object.getOwnPropertyDescriptor(error, 'errcode')?.value : undefined;
  if (typeof code === 'number' && (code & 255) === 13) throw new RepositoryRetentionError('REPOSITORY_METADATA_LIMIT');
  throw error;
}
/** Fixed control files only; payloads/traces and other processes need their own disk guard. */
export class MetadataBudget {
  readonly pageLimit: number;
  private readonly blockBytes: number;
  constructor(private readonly db: DatabaseSync, private readonly directory: string,
    private readonly limits: RepositoryMetadataLimits,
    private readonly ledgerFile = 'retention-v2.sqlite', private readonly reclaiming = false) {
    this.blockBytes = Number(statfsSync(directory, { bigint: true }).bsize);
    if (!Number.isSafeInteger(this.blockBytes) || this.blockBytes < 1 || this.blockBytes > 65536) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    if (Number(db.prepare('PRAGMA page_size').get()!.page_size) !== PAGE_BYTES) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    this.pageLimit = Math.floor((limits.maxBytes - CONTROL_RESERVE_BYTES) / (3 * (PAGE_BYTES + 8)));
    // DELETE journaling prevents an indefinitely pinned WAL from bypassing a main-file cap.
    // The page allowance reserves room for a full rollback image as well as the database.
    db.exec('PRAGMA cache_spill=OFF');
    const actual = db.prepare(`PRAGMA max_page_count=${this.pageLimit}`).get()!.max_page_count;
    if (Number(actual) !== this.pageLimit) throw new RepositoryRetentionError('REPOSITORY_METADATA_LIMIT');
    this.checkFiles();
  }
  checkFiles(): number {
    if (this.db.prepare('PRAGMA journal_mode').get()!.journal_mode !== 'delete' ||
      this.db.prepare('PRAGMA cache_spill').get()!.cache_spill !== 0) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    let bytes = 0;
    for (const name of [...CONTROL_FILES, this.ledgerFile, this.ledgerFile + '-journal']) {
      try {
        const info = lstatSync(join(this.directory, name));
        if (!info.isFile() || info.isSymbolicLink() || info.nlink !== 1) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
        bytes += Math.max(info.blocks * 512, Math.ceil(info.size / this.blockBytes) * this.blockBytes);
      } catch (error) {
        if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error;
        if (name === 'retention.sqlite' || name === this.ledgerFile) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      }
    }
    if (!Number.isSafeInteger(bytes) || bytes > this.limits.maxBytes) throw new RepositoryRetentionError('REPOSITORY_METADATA_LIMIT');
    return bytes;
  }
  capacity(request: { operations: number; references: number }): RepositoryMetadataCapacity {
    if (![request.operations, request.references].every(value => Number.isSafeInteger(value) && value >= 0)) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    // Legacy tables are append-only. The opt-in retirement format reuses rows,
    // so max(rowid) must never be interpreted as its live membership count.
    const count = this.reclaiming ? 'count(*)' : 'coalesce(max(rowid),0)';
    const operations = Number(this.db.prepare(`SELECT ${count} AS n FROM operations`).get()!.n);
    const references = Number(this.db.prepare(`SELECT ${count} AS n FROM refs`).get()!.n);
    if (![operations, references].every(value => Number.isSafeInteger(value) && value >= 0) ||
      operations > this.limits.maxOperations || references > this.limits.maxReferences) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    const pages = Number(this.db.prepare('PRAGMA page_count').get()!.page_count);
    const freePages = Number(this.db.prepare('PRAGMA freelist_count').get()!.freelist_count);
    return { available: request.operations <= this.limits.maxOperations - operations &&
      request.references <= this.limits.maxReferences - references && (pages < this.pageLimit || freePages > 0),
    operations, references, remainingOperations: this.limits.maxOperations - operations,
    remainingReferences: this.limits.maxReferences - references, controlFileBytes: this.checkFiles(),
    databasePages: pages, databasePageLimit: this.pageLimit, limits: this.limits };
  }
  admit(references: number): void {
    if (!this.capacity({ operations: 1, references }).available) throw new RepositoryRetentionError('REPOSITORY_METADATA_LIMIT');
  }
}
