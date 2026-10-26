import { createRequire } from 'node:module';
import { chmodSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError, type RepositoryRetentionReceipt, type RetainedRepositoryOptions } from './contracts.js';
import { ElapsedAge } from './elapsedAge.js';
import { clockFence } from './clockFence.js';
import { MetadataBudget, metadataWriteFailure } from './metadataBudget.js';
import { admitTransaction } from './admission.js';
export interface Row extends RepositoryRetentionReceipt { fingerprint: string; writeHash: string }
function isBusy(error: unknown): boolean {
  if ((typeof error !== 'object' && typeof error !== 'function') || error === null) return false;
  const code: unknown = Object.getOwnPropertyDescriptor(error, 'errcode')?.value;
  return typeof code === 'number' && ((code & 255) === 5 || (code & 255) === 6);
}
/** Content-free durable ownership, pending intents and expiry tombstones. */
export class RepositoryLedger {
  private readonly db: DatabaseSync;
  private readonly age?: ElapsedAge;
  private readonly budget?: MetadataBudget;
  constructor(directory: string, private readonly retentionMs: number, options?: RetainedRepositoryOptions) {
    const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
    const guarded = options?.expiryClock === 'verified-uptime';
    if (guarded) {
      try { clockFence(DatabaseSync, directory, retentionMs); }
      catch (error) { if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY'); throw error; }
    }
    const path = join(directory, guarded ? 'retention-v2.sqlite' : 'retention.sqlite');
    this.db = new DatabaseSync(path);
    try {
      chmodSync(path, 0o600);
      if (guarded) {
        this.db.exec(`PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA; PRAGMA busy_timeout=0; BEGIN IMMEDIATE;
          CREATE TABLE IF NOT EXISTS policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL,settings TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS operations(operationHash TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,writeHash TEXT NOT NULL,acceptedAt INTEGER NOT NULL,expiresAt INTEGER NOT NULL,status TEXT NOT NULL,dataDeleted INTEGER NOT NULL DEFAULT 0,confirmed INTEGER NOT NULL DEFAULT 0,acceptedAgeMs INTEGER NOT NULL,expiresAgeMs INTEGER NOT NULL);
          CREATE INDEX IF NOT EXISTS expiry ON operations(expiresAgeMs) WHERE dataDeleted=0;
          CREATE TABLE IF NOT EXISTS refs(refHash TEXT PRIMARY KEY,operationHash TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS elapsed_clock(id INTEGER PRIMARY KEY CHECK(id=1),ageMs INTEGER NOT NULL);`);
        const settings = JSON.stringify({ expiryClock: 'verified-uptime', ...(options.metadataLimits ? { metadataLimits: options.metadataLimits } : {}) });
        this.db.prepare('INSERT OR IGNORE INTO policy VALUES(1,2,?,?)').run(retentionMs, settings);
        const policy = this.db.prepare('SELECT * FROM policy WHERE id=1').get();
        if (policy?.version !== 2 || policy.retentionMs !== retentionMs || policy.settings !== settings) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
        this.db.exec('INSERT OR IGNORE INTO elapsed_clock VALUES(1,0); COMMIT');
        if (options.metadataLimits) this.budget = new MetadataBudget(this.db, directory, options.metadataLimits);
        this.age = new ElapsedAge(this.db, options.monotonicClock ?? (() => performance.now()));
        return;
      }
      this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0;
        CREATE TABLE IF NOT EXISTS policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS operations(operationHash TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,writeHash TEXT NOT NULL,acceptedAt INTEGER NOT NULL,expiresAt INTEGER NOT NULL,status TEXT NOT NULL,dataDeleted INTEGER NOT NULL DEFAULT 0,confirmed INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS expiry ON operations(expiresAt) WHERE dataDeleted=0;
        CREATE TABLE IF NOT EXISTS refs(refHash TEXT PRIMARY KEY,operationHash TEXT NOT NULL);`);
      this.db.prepare('INSERT OR IGNORE INTO policy VALUES(1,1,?)').run(retentionMs);
      const policy = this.db.prepare('SELECT version,retentionMs FROM policy WHERE id=1').get()!;
      if (policy.version !== 1 || policy.retentionMs !== retentionMs) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    } catch (error) {
      if (this.db.isTransaction) this.db.exec('ROLLBACK');
      this.db.close();
      if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY');
      throw error;
    }
  }
  begin(): void {
    try { this.db.exec('BEGIN IMMEDIATE'); this.budget?.checkFiles(); this.age?.begin(); }
    catch (error) {
      if (this.db.isTransaction) this.db.exec('ROLLBACK');
      if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY');
      throw error;
    }
  }
  /** Translate only this ledger's own writes; external publication errors stay unconfirmed. */
  private write<T>(operation: () => T): T {
    try { return operation(); }
    catch (error) { if (this.budget) metadataWriteFailure(error); throw error; }
  }
  async commit(): Promise<void> {
    this.write(() => this.age?.checkpoint());
    // DELETE-journal readers can block COMMIT after BEGIN IMMEDIATE succeeded.
    // SQLITE_BUSY leaves this transaction open. Retry only COMMIT, once the
    // checkpoint and all native publication/cleanup effects have already run.
    await admitTransaction(() => {
      try { this.write(() => this.db.exec('COMMIT')); }
      catch (error) {
        if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY');
        throw error;
      }
    });
  }
  rollback(): void { if (this.db.isTransaction) this.db.exec('ROLLBACK'); }
  row(hash: string): Row | undefined {
    const row = this.db.prepare('SELECT * FROM operations WHERE operationHash=?').get(hash);
    if (!row) return undefined;
    if (['operationHash', 'fingerprint', 'writeHash'].some(key => typeof row[key] !== 'string' || !/^[a-f0-9]{64}$/.test(row[key] as string)) ||
      !Number.isSafeInteger(row.acceptedAt) || !Number.isSafeInteger(row.expiresAt) || Number(row.acceptedAt) < 0 ||
      Number(row.expiresAt) - Number(row.acceptedAt) !== this.retentionMs || !['pending', 'stored', 'expired'].includes(String(row.status)) ||
      ![0, 1].includes(Number(row.confirmed)) || (row.status === 'stored' && row.confirmed !== 1) ||
      ![0, 1].includes(Number(row.dataDeleted)) || (row.dataDeleted === 1 && row.status !== 'expired')) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    if (this.age && (!Number.isSafeInteger(row.acceptedAgeMs) || Number(row.acceptedAgeMs) < 0 ||
      !Number.isSafeInteger(row.expiresAgeMs) || Number(row.expiresAgeMs) - Number(row.acceptedAgeMs) !== this.retentionMs)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    return { ...row, publication: row.confirmed === 1 ? 'committed' : 'unconfirmed', dataDeleted: row.dataDeleted === 1,
      ...(this.age ? { expiryClock: 'verified-uptime' } : {}) } as unknown as Row;
  }
  owner(refHash: string): Row | undefined {
    const row = this.db.prepare('SELECT operationHash FROM refs WHERE refHash=?').get(refHash);
    return row ? this.row(String(row.operationHash)) : undefined;
  }
  intent(hash: string, fingerprint: string, writeHash: string, refs: readonly string[], now: number, retentionMs: number): void {
    this.budget?.admit(refs.length);
    this.write(() => this.insertIntent(hash, fingerprint, writeHash, refs, now, retentionMs));
  }
  private insertIntent(hash: string, fingerprint: string, writeHash: string, refs: readonly string[], now: number, retentionMs: number): void {
    if (this.age) {
      const age = this.age.now();
      if (!Number.isSafeInteger(age + retentionMs)) throw new RepositoryRetentionError('REPOSITORY_CLOCK_UNAVAILABLE');
      this.db.prepare("INSERT INTO operations VALUES(?,?,?,?,?,'pending',0,0,?,?)").run(hash, fingerprint, writeHash, now, now + retentionMs, age, age + retentionMs);
    } else this.db.prepare("INSERT INTO operations VALUES(?,?,?,?,?,'pending',0,0)").run(hash, fingerprint, writeHash, now, now + retentionMs);
    const insert = this.db.prepare('INSERT INTO refs VALUES(?,?)');
    for (const ref of refs) insert.run(ref, hash);
  }
  stored(hash: string): void { this.write(() => this.db.prepare("UPDATE operations SET status='stored',confirmed=1 WHERE operationHash=?").run(hash)); }
  expired(hash: string): void { this.write(() => this.db.prepare("UPDATE operations SET status='expired' WHERE operationHash=?").run(hash)); }
  deleted(hash: string): void { this.write(() => this.db.prepare('UPDATE operations SET dataDeleted=1 WHERE operationHash=?').run(hash)); }
  expiredAt(row: Row, wall: number): boolean { return row.status === 'expired' || (this.age ? row.expiresAgeMs! <= this.age.now() : row.expiresAt <= wall); }
  wallBased(): boolean { return !this.age; }
  metadataCapacity(request: { operations: number; references: number }) {
    if (!this.budget) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    return this.budget.capacity(request);
  }
  candidates(now: number, limit: number): Row[] {
    const field = this.age ? 'expiresAgeMs' : 'expiresAt';
    return this.db.prepare(`SELECT operationHash FROM operations WHERE (${field}<=? OR status='expired') AND dataDeleted=0 ORDER BY ${field},operationHash LIMIT ?`)
      .all(this.age ? this.age.now() : now, limit).map(row => this.row(String(row.operationHash))!);
  }
  close(): void { this.db.close(); }
}
