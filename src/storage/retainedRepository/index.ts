import type { CommitWrite, Repository } from '../../ports.js';
import type { RecordRef } from '../../contracts.js';
import { FilesystemRepositoryError } from '../filesystemError.js';
import { RepositoryRetentionError, type RetainedRepositoryOptions, type RepositoryRetentionReceipt } from './contracts.js';
import { RepositoryReadBatcher } from './readBatcher.js';
import { RepositorySerial } from './serial.js';
import { RepositoryLedger, type Row } from './ledger.js';
import { checkOptions, readyDirectory, time } from './lifecycle.js';
import type { IndexedData } from './indexedData.js';
import { readOwned } from './readOwned.js';
import { prepareIntent, snapshotWrite, operationHash } from './commit.js';
import { removeOperation } from './cleanup.js';
import { admitTransaction } from './admission.js';
import { setImmediate as yieldTurn } from 'node:timers/promises';
import { storageFailure } from './failure.js';
export * from './contracts.js';
/** Explicit, packed, managed namespace. No timer, automatic resend or legacy adoption. */
export class RetainedFilesystemRepository implements Repository {
  private ledger?: RepositoryLedger;
  private data?: IndexedData;
  private path?: string;
  private readonly serial = new RepositorySerial();
  private readonly reads = new RepositoryReadBatcher(refs => this.readMany(refs));
  private publishing?: string;
  private cleanupRunning = false;
  constructor(private readonly options: RetainedRepositoryOptions) {
    checkOptions(options);
    this.options = Object.freeze({ ...options, ...(options.metadataLimits ? { metadataLimits: Object.freeze({
      maxBytes: options.metadataLimits.maxBytes, maxOperations: options.metadataLimits.maxOperations, maxReferences: options.metadataLimits.maxReferences,
    }) } : {}) });
  }
  private fresh = (row: Row): void => {
    const now = time(this.options);
    if (this.ledger!.expiredAt(row, now)) {
      this.ledger!.expired(row.operationHash);
      throw new RepositoryRetentionError('REPOSITORY_EXPIRED');
    }
    if (this.ledger!.wallBased() && now < row.acceptedAt) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  };
  private async ready(): Promise<void> {
    if (this.ledger) return;
    this.path = await readyDirectory(this.options);
    this.ledger = new RepositoryLedger(this.path, this.options.retentionMs, this.options);
    const { IndexedData } = await import('./indexedData.js');
    this.data = new IndexedData(this.path, refs => readOwned(refs, this.ledger!, this.data!, this.fresh, this.publishing));
  }
  private async exclusive<T>(fn: () => Promise<T>): Promise<T> {
    try { return await this.serial.run(() => this.transaction(fn)); }
    catch (error) { return storageFailure(this.options, error); }
  }
  private async transaction<T>(fn: () => Promise<T>): Promise<T> {
    let begun = false;
    try {
      await admitTransaction(async () => { await this.ready(); this.ledger!.begin(); }); begun = true;
      const result = await fn(); this.ledger!.commit(); begun = false; return result;
    } catch (error) {
      let failure = error;
      if (begun) {
        try {
          if (error instanceof RepositoryRetentionError && error.code === 'REPOSITORY_EXPIRED') this.ledger!.commit();
          else this.ledger!.rollback();
        } catch { failure = new RepositoryRetentionError('REPOSITORY_UNCONFIRMED'); }
      }
      const safe = failure instanceof RepositoryRetentionError || failure instanceof FilesystemRepositoryError ? failure : new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      throw safe;
    }
  }
  read(ref: RecordRef) { return this.reads.read(ref); }
  /** A snapshot for host preflight, not a reservation or publication promise. */
  metadataCapacity(request = { operations: 1, references: 1 }) {
    return this.exclusive(async () => this.ledger!.metadataCapacity(request));
  }
  readMany(refs: readonly RecordRef[]) { return this.exclusive(() => this.data!.readMany(refs)); }
  findOperation(id: string) {
    return this.exclusive(async () => {
      const row = this.ledger!.row(operationHash(id)); if (!row) return undefined;
      this.fresh(row); const result = await this.data!.findOperation(id);
      if (!result && row.status === 'stored') throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      if (result) this.ledger!.stored(row.operationHash);
      return result;
    });
  }
  async commit(input: CommitWrite) {
    const write = snapshotWrite(input);
    // The pending intent commits before native publication. A crash never loses its identity/expiry.
    await this.exclusive(async () => prepareIntent(write, this.ledger!, this.fresh, time(this.options), this.options.retentionMs));
    return this.exclusive(async () => {
      const hash = operationHash(write.operationId); this.fresh(this.ledger!.row(hash)!); this.publishing = hash;
      try { const receipt = await this.data!.commit(write); this.fresh(this.ledger!.row(hash)!); if (receipt.status === 'committed') this.ledger!.stored(hash); return receipt; }
      finally { delete this.publishing; }
    });
  }
  receipt(id: string): Promise<RepositoryRetentionReceipt | undefined> {
    return this.exclusive(async () => {
      const row = this.ledger!.row(operationHash(id)); if (!row) return undefined;
      if (this.ledger!.expiredAt(row, time(this.options))) this.ledger!.expired(row.operationHash);
      return { operationHash: row.operationHash, acceptedAt: row.acceptedAt, expiresAt: row.expiresAt, status: this.ledger!.row(row.operationHash)!.status, publication: row.publication, dataDeleted: row.dataDeleted,
        ...(row.expiryClock ? { expiryClock: row.expiryClock, acceptedAgeMs: row.acceptedAgeMs!, expiresAgeMs: row.expiresAgeMs! } : {}) };
    });
  }
  async sweep(options: { limit: number }): Promise<{ deleted: number }> {
    if (!Number.isSafeInteger(options.limit) || options.limit < 1 || options.limit > 100) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    if (this.cleanupRunning) return storageFailure(this.options, new RepositoryRetentionError('REPOSITORY_BUSY'));
    this.cleanupRunning = true;
    try {
      const rows = await this.exclusive(async () => this.ledger!.candidates(time(this.options), options.limit));
      let deleted = 0;
      for (const row of rows) {
        await this.exclusive(async () => this.ledger!.expired(row.operationHash)); // durable before unlink
        // Expired identities cannot publish again. Unlink/fsync needs no database
        // lock; a crash here leaves dataDeleted=false for explicit reconciliation.
        try { await removeOperation(this.path!, row.operationHash); }
        catch (error) { return storageFailure(this.options, error); }
        await this.exclusive(async () => this.ledger!.deleted(row.operationHash));
        deleted++;
        await yieldTurn();
      }
      return { deleted };
    } finally { this.cleanupRunning = false; }
  }
  close(): void { if (this.cleanupRunning || this.serial.pending || this.reads.busy) throw new RepositoryRetentionError('REPOSITORY_BUSY'); this.ledger?.close(); delete this.ledger; delete this.data; }
}
export function createRetainedFilesystemRepository(options: RetainedRepositoryOptions): RetainedFilesystemRepository | undefined {
  if (options.enabled !== true) return undefined;
  return new RetainedFilesystemRepository(options);
}
