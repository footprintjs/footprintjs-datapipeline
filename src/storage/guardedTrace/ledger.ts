import type { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import { chmodSync } from 'node:fs';
import { join } from 'node:path';
import { TRACE_RETENTION_MS, TraceStorageError, type TraceStorageReceipt, type TraceStorageGapReceipt, type GuardedTraceOptions, type TraceStorageCode } from './contracts.js';
/** Metadata contains hashes, times, counts and fixed reason codes, never trace values. */
export class TraceLedger {
 private readonly db: DatabaseSync;
 constructor(path: string) {
  // Load SQLite only for explicitly enabled managed stores.
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  this.db = new DatabaseSync(join(path, 'retention.sqlite'));
  try {
  chmodSync(join(path, 'retention.sqlite'), 0o600);
  this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0;
   CREATE TABLE IF NOT EXISTS traces(id TEXT PRIMARY KEY, acceptedAt INTEGER NOT NULL, expiresAt INTEGER NOT NULL, bytes INTEGER NOT NULL, status TEXT NOT NULL);
   CREATE INDEX IF NOT EXISTS expiry ON traces(expiresAt) WHERE bytes>0;
   CREATE INDEX IF NOT EXISTS pending ON traces(bytes) WHERE status='pending';
   CREATE TABLE IF NOT EXISTS gaps(id INTEGER PRIMARY KEY, traceId TEXT NOT NULL, at INTEGER NOT NULL, code TEXT NOT NULL);
   CREATE INDEX IF NOT EXISTS gap_lookup ON gaps(traceId,id DESC);
   CREATE TABLE IF NOT EXISTS totals(id INTEGER PRIMARY KEY CHECK(id=1), bytes INTEGER NOT NULL);
   INSERT OR IGNORE INTO totals VALUES(1,0);`);
  } catch (error) { this.db.close(); throw error; }
 }
 bindPolicy(value: string): void {
  this.db.exec('CREATE TABLE IF NOT EXISTS policy(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)');
  this.db.prepare('INSERT OR IGNORE INTO policy VALUES(1,?)').run(value);
  if (this.db.prepare('SELECT value FROM policy WHERE id=1').get()!.value !== value) throw new TraceStorageError('TRACE_INVALID_OPTIONS');
 }
 lastGap(id: string): TraceStorageGapReceipt | undefined {
  const row = this.db.prepare('SELECT traceId AS id,at,code FROM gaps WHERE traceId=? ORDER BY id DESC LIMIT 1').get(id);
  if (!row) return undefined;
  if (!['TRACE_DISK_PRESSURE', 'TRACE_BUDGET_EXCEEDED', 'TRACE_STORAGE_UNCONFIRMED'].includes(String(row.code))) throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED');
  return { id: String(row.id), at: Number(row.at), code: row.code as TraceStorageCode, status: 'not-saved' };
 }
 gap(id: string, at: number): void { this.db.prepare('INSERT INTO gaps(traceId,at,code) VALUES(?,?,?)').run(id, at, 'TRACE_STORAGE_UNCONFIRMED'); }
 close(): void { this.db.close(); }
 row(id: string): TraceStorageReceipt | undefined { return this.db.prepare('SELECT * FROM traces WHERE id=?').get(id) as unknown as TraceStorageReceipt | undefined; }
 traceBytes(): number { return Number(this.db.prepare('SELECT bytes FROM totals WHERE id=1').get()!.bytes); }
 usage() { return { traceBytes: this.traceBytes(), gaps: Number(this.db.prepare('SELECT COUNT(*) AS n FROM gaps').get()!.n) }; }
 private transaction<T>(fn: () => T): T {
  try { this.db.exec('BEGIN IMMEDIATE'); } catch { throw new TraceStorageError('TRACE_STORE_BUSY'); }
  try { const result = fn(); this.db.exec('COMMIT'); return result; }
  catch (error) { this.db.exec('ROLLBACK'); throw error; }
 }
 reserve(id: string, bytes: number, now: number, free: bigint, o: GuardedTraceOptions): { existing?: TraceStorageReceipt; refusal?: TraceStorageCode } {
  return this.transaction(() => {
   const existing = this.row(id); if (existing) return { existing };
   const pending = Number(this.db.prepare("SELECT COALESCE(SUM(bytes),0) AS n FROM traces WHERE status='pending'").get()!.n);
   const overBudget = this.traceBytes() + bytes > o.maxTraceBytesOnDisk;
   const lowDisk = free < BigInt(o.minFreeBytes) + BigInt(o.receiptReserveBytes) + BigInt(pending) + BigInt(bytes);
   if (overBudget || lowDisk) {
    const refusal = overBudget ? 'TRACE_BUDGET_EXCEEDED' : 'TRACE_DISK_PRESSURE';
    this.db.prepare('INSERT INTO gaps(traceId,at,code) VALUES(?,?,?)').run(id, now, refusal);
    return { refusal };
   }
   this.db.prepare("INSERT INTO traces VALUES(?,?,?,?,'pending')").run(id, now, now + TRACE_RETENTION_MS, bytes);
   this.db.prepare('UPDATE totals SET bytes=bytes+? WHERE id=1').run(bytes);
   return {};
  });
 }
 stored(id: string, bytes: number): void {
  this.transaction(() => {
   const row = this.row(id)!;
   this.db.prepare("UPDATE traces SET status='stored',bytes=? WHERE id=?").run(bytes, id);
   this.db.prepare('UPDATE totals SET bytes=bytes+? WHERE id=1').run(bytes - row.bytes);
  });
 }
 expired(now: number, limit: number): TraceStorageReceipt[] { return this.db.prepare('SELECT * FROM traces WHERE expiresAt<=? AND bytes>0 ORDER BY expiresAt,id LIMIT ?').all(now, limit) as unknown as TraceStorageReceipt[]; }
 markExpired(id: string): void { this.db.prepare("UPDATE traces SET status='expired' WHERE id=?").run(id); }
 released(id: string): void {
  this.transaction(() => { const row = this.row(id)!; this.db.prepare('UPDATE totals SET bytes=bytes-? WHERE id=1').run(row.bytes); this.db.prepare('UPDATE traces SET bytes=0 WHERE id=?').run(id); });
 }
}
