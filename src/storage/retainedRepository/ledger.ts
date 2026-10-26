import { createRequire } from 'node:module';
import { chmodSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError, type RepositoryRetentionReceipt, type RetainedRepositoryOptions, type RepositoryHistoryPolicy } from './contracts.js';
import { ElapsedAge } from './elapsedAge.js';
import { clockFence } from './clockFence.js';
import { MetadataBudget, metadataWriteFailure } from './metadataBudget.js';
import { admitTransaction } from './admission.js';
import { HistoryLedger } from './historyLedger.js';
import { validateActiveHistory } from './historyFence.js';
import { validateOwnership } from './historyIdentity.js';
import type { CommitWrite } from '../../ports.js';
import type { RecordRef } from '../../contracts.js';
import { inTransaction } from './transactionState.js';
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
  private readonly history?: HistoryLedger;
  private readonly historyPolicy?: RepositoryHistoryPolicy;
  constructor(directory: string, private readonly retentionMs: number, options?: RetainedRepositoryOptions) {
    const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
    const guarded = options?.expiryClock === 'verified-uptime';
    const history = options?.historyRetirement;
    if (history) this.historyPolicy = history;
    if (guarded) {
      let authority: ReturnType<typeof clockFence> | undefined;
      let db: DatabaseSync | undefined;
      try {
        const settings = JSON.stringify({ expiryClock: 'verified-uptime', ...(options.metadataLimits ? { metadataLimits: options.metadataLimits } : {}),
          ...(history ? { historyRetirement: { profile: history.profile, keepDays: history.keepDays, namespaceId: history.namespaceId, maxOperationBytes: history.maxOperationBytes } } : {}) });
        const ledgerFile = history ? 'retention-v4.sqlite' : 'retention-v2.sqlite';
        authority = clockFence(DatabaseSync, directory, retentionMs, { settings, ledgerFile, version: history ? 6 : 5,
          ...(history ? { requiredTables: ['retired_history', 'history_counts', 'history_scan'] } : {}) });
        const path = join(directory, ledgerFile);
        this.db = db = new DatabaseSync(path);
        chmodSync(path, 0o600);
        db.exec('PRAGMA foreign_keys=ON; PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA; PRAGMA busy_timeout=0; BEGIN IMMEDIATE');
        const fresh = authority.requireAuthority(db);
        if (fresh) {
          db.exec(`CREATE TABLE policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL,settings TEXT NOT NULL);
            CREATE TABLE operations(operationHash TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,writeHash TEXT NOT NULL,acceptedAt INTEGER NOT NULL,expiresAt INTEGER NOT NULL,status TEXT NOT NULL,dataDeleted INTEGER NOT NULL DEFAULT 0,confirmed INTEGER NOT NULL DEFAULT 0,acceptedAgeMs INTEGER NOT NULL,expiresAgeMs INTEGER NOT NULL${history ? ',period INTEGER NOT NULL' : ''});
            CREATE INDEX expiry ON operations(expiresAgeMs) WHERE dataDeleted=0;
            CREATE TABLE refs(refHash TEXT PRIMARY KEY,operationHash TEXT NOT NULL${history ? ' REFERENCES operations(operationHash)' : ''});
            CREATE TABLE elapsed_clock(id INTEGER PRIMARY KEY CHECK(id=1),ageMs INTEGER NOT NULL,revision INTEGER NOT NULL);`);
          db.prepare(history ? 'INSERT INTO policy VALUES(1,6,?,?)' : 'INSERT INTO policy VALUES(1,5,?,?)').run(retentionMs, settings);
          db.exec('INSERT INTO elapsed_clock VALUES(1,0,0)');
        } else if (history) validateActiveHistory(db);
        this.age = new ElapsedAge(db, options.monotonicClock ?? (() => performance.now()));
        if (history) this.history = new HistoryLedger(db, history, () => this.age!.now(), hash => this.row(hash)!);
        if (fresh) authority.bind(db);
        if (options.metadataLimits) this.budget = new MetadataBudget(db, directory, options.metadataLimits, ledgerFile, !!history);
        db.exec('COMMIT');
        authority.activate();
        return;
      } catch (error) {
        if (db) { try { if (inTransaction(db)) db.exec('ROLLBACK'); } finally { db.close(); } }
        if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY');
        // Startup may fail before a MetadataBudget instance exists.
        if (options.metadataLimits) metadataWriteFailure(error);
        throw error;
      } finally { authority?.close(); }
    }
    const path = join(directory, 'retention.sqlite');
    this.db = new DatabaseSync(path);
    try {
      chmodSync(path, 0o600);
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
      // Startup writes can fail before the budget has been constructed.
      if (options?.metadataLimits) metadataWriteFailure(error);
      throw error;
    }
  }
  begin(): void {
    try { this.db.exec('BEGIN IMMEDIATE'); this.budget?.checkFiles(); this.age?.begin(); }
    catch (error) {
      if (this.age ? inTransaction(this.db) : this.db.isTransaction) this.db.exec('ROLLBACK');
      this.age?.rolledBack();
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
    this.age?.committed();
  }
  rollback(): void {
    try { if (this.age ? inTransaction(this.db) : this.db.isTransaction) this.db.exec('ROLLBACK'); }
    finally { this.age?.rolledBack(); }
  }
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
    if (this.historyPolicy && (!Number.isSafeInteger(row.period) || Number(row.period) < 0 ||
      Math.floor(Number(row.acceptedAgeMs) / 86400_000) !== row.period)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    return { ...row, publication: row.confirmed === 1 ? 'committed' : 'unconfirmed', dataDeleted: row.dataDeleted === 1,
      ...(this.age ? { expiryClock: 'verified-uptime' } : {}) } as unknown as Row;
  }
  owner(refHash: string): Row | undefined {
    const row = this.db.prepare('SELECT operationHash FROM refs WHERE refHash=?').get(refHash);
    return row ? this.row(String(row.operationHash)) : undefined;
  }
  intent(hash: string, fingerprint: string, writeHash: string, refs: readonly string[], now: number, retentionMs: number, period?: number): void {
    this.budget?.admit(refs.length);
    this.write(() => this.insertIntent(hash, fingerprint, writeHash, refs, now, retentionMs, period));
  }
  private insertIntent(hash: string, fingerprint: string, writeHash: string, refs: readonly string[], now: number, retentionMs: number, period?: number): void {
    if (this.age) {
      const age = this.age.acceptedNow();
      if (this.history && Math.floor(age / 86400_000) !== period) throw new RepositoryRetentionError('REPOSITORY_PERIOD_CLOSED');
      if (!Number.isSafeInteger(age + retentionMs)) throw new RepositoryRetentionError('REPOSITORY_CLOCK_UNAVAILABLE');
      const values = [hash, fingerprint, writeHash, now, now + retentionMs, age, age + retentionMs];
      if (this.history) values.push(period!);
      this.db.prepare(`INSERT INTO operations VALUES(?,?,?,?,?,'pending',0,0,?,?${this.history ? ',?' : ''})`).run(...values);
    } else this.db.prepare("INSERT INTO operations VALUES(?,?,?,?,?,'pending',0,0)").run(hash, fingerprint, writeHash, now, now + retentionMs);
    const insert = this.db.prepare('INSERT INTO refs VALUES(?,?)');
    for (const ref of refs) insert.run(ref, hash);
  }
  stored(hash: string): void { this.write(() => this.db.prepare("UPDATE operations SET status='stored',confirmed=1 WHERE operationHash=?").run(hash)); }
  expired(hash: string): void { this.write(() => this.db.prepare("UPDATE operations SET status='expired' WHERE operationHash=?").run(hash)); }
  deleted(hash: string): void { this.write(() => this.db.prepare('UPDATE operations SET dataDeleted=1 WHERE operationHash=?').run(hash)); }
  expiredAt(row: Row, wall: number): boolean { return row.status === 'expired' || (this.age ? row.expiresAgeMs! <= this.age.now() : row.expiresAt <= wall); }
  wallBased(): boolean { return !this.age; }
  checkOperation(id: string, intent: 'lookup' | 'admit' = 'lookup'): number | undefined { return this.history?.check(id, 'o', intent); }
  checkRefs(refs: readonly RecordRef[]): void { this.history?.checkRefs(refs); }
  validatePeriodWrite(write: CommitWrite): number | undefined {
    if (!this.history) return undefined;
    this.history.check(write.operationId, 'o', 'admit');
    return validateOwnership(write, this.historyPolicy!);
  }
  historyStatus() {
    if (!this.history) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    return this.history.status();
  }
  retireHistory(limit: number) {
    if (!this.history) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    return this.write(() => this.history!.retire(limit));
  }
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
