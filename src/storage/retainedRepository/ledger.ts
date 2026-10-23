import { createRequire } from 'node:module';
import { chmodSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError, type RepositoryRetentionReceipt } from './contracts.js';
export interface Row extends RepositoryRetentionReceipt { fingerprint: string; writeHash: string }
function isBusy(error: unknown): boolean {
  if ((typeof error !== 'object' && typeof error !== 'function') || error === null) return false;
  const code: unknown = Object.getOwnPropertyDescriptor(error, 'errcode')?.value;
  return typeof code === 'number' && ((code & 255) === 5 || (code & 255) === 6);
}
/** Content-free durable ownership, pending intents and expiry tombstones. */
export class RepositoryLedger {
  private readonly db: DatabaseSync;
  constructor(directory: string, private readonly retentionMs: number) {
    const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
    this.db = new DatabaseSync(join(directory, 'retention.sqlite'));
    try {
      chmodSync(join(directory, 'retention.sqlite'), 0o600);
      this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0;
        CREATE TABLE IF NOT EXISTS policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS operations(operationHash TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,writeHash TEXT NOT NULL,acceptedAt INTEGER NOT NULL,expiresAt INTEGER NOT NULL,status TEXT NOT NULL,dataDeleted INTEGER NOT NULL DEFAULT 0,confirmed INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS expiry ON operations(expiresAt) WHERE dataDeleted=0;
        CREATE TABLE IF NOT EXISTS refs(refHash TEXT PRIMARY KEY,operationHash TEXT NOT NULL);`);
      this.db.prepare('INSERT OR IGNORE INTO policy VALUES(1,1,?)').run(retentionMs);
      const policy = this.db.prepare('SELECT version,retentionMs FROM policy WHERE id=1').get()!;
      if (policy.version !== 1 || policy.retentionMs !== retentionMs) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    } catch (error) {
      this.db.close();
      if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY');
      throw error;
    }
  }
  begin(): void {
    try { this.db.exec('BEGIN IMMEDIATE'); }
    catch (error) { if (isBusy(error)) throw new RepositoryRetentionError('REPOSITORY_BUSY'); throw error; }
  }
  commit(): void { this.db.exec('COMMIT'); }
  rollback(): void { this.db.exec('ROLLBACK'); }
  row(hash: string): Row | undefined {
    const row = this.db.prepare('SELECT * FROM operations WHERE operationHash=?').get(hash);
    if (!row) return undefined;
    if (['operationHash', 'fingerprint', 'writeHash'].some(key => typeof row[key] !== 'string' || !/^[a-f0-9]{64}$/.test(row[key] as string)) ||
      !Number.isSafeInteger(row.acceptedAt) || !Number.isSafeInteger(row.expiresAt) || Number(row.acceptedAt) < 0 ||
      Number(row.expiresAt) - Number(row.acceptedAt) !== this.retentionMs || !['pending', 'stored', 'expired'].includes(String(row.status)) ||
      ![0, 1].includes(Number(row.confirmed)) || (row.status === 'stored' && row.confirmed !== 1) ||
      ![0, 1].includes(Number(row.dataDeleted)) || (row.dataDeleted === 1 && row.status !== 'expired')) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    return { ...row, publication: row.confirmed === 1 ? 'committed' : 'unconfirmed', dataDeleted: row.dataDeleted === 1 } as unknown as Row;
  }
  owner(refHash: string): Row | undefined {
    const row = this.db.prepare('SELECT operationHash FROM refs WHERE refHash=?').get(refHash);
    return row ? this.row(String(row.operationHash)) : undefined;
  }
  intent(hash: string, fingerprint: string, writeHash: string, refs: readonly string[], now: number, retentionMs: number): void {
    this.db.prepare("INSERT INTO operations VALUES(?,?,?,?,?,'pending',0,0)").run(hash, fingerprint, writeHash, now, now + retentionMs);
    const insert = this.db.prepare('INSERT INTO refs VALUES(?,?)');
    for (const ref of refs) insert.run(ref, hash);
  }
  stored(hash: string): void { this.db.prepare("UPDATE operations SET status='stored',confirmed=1 WHERE operationHash=?").run(hash); }
  expired(hash: string): void { this.db.prepare("UPDATE operations SET status='expired' WHERE operationHash=?").run(hash); }
  deleted(hash: string): void { this.db.prepare('UPDATE operations SET dataDeleted=1 WHERE operationHash=?').run(hash); }
  candidates(now: number, limit: number): Row[] { return (this.db.prepare("SELECT operationHash FROM operations WHERE (expiresAt<=? OR status='expired') AND dataDeleted=0 ORDER BY expiresAt,operationHash LIMIT ?").all(now, limit)).map(row => this.row(String(row.operationHash))!); }
  close(): void { this.db.close(); }
}
