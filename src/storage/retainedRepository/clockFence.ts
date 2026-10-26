import { chmodSync, lstatSync, readdirSync, type Stats } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError } from './contracts.js';

interface AuthorityOptions {
  readonly version?: number;
  readonly ledgerFile?: string;
  readonly settings?: string;
  readonly requiredTables?: readonly string[];
}
const unconfirmed = (): never => { throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED'); };
function file(path: string): Stats | undefined {
  try {
    const info = lstatSync(path);
    if (!info.isFile() || info.isSymbolicLink()) return unconfirmed();
    return info;
  } catch (error) { if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') return undefined; throw error; }
}
function tables(db: DatabaseSync): Set<string> {
  return new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all().map(row => String(row.name)));
}
function emptyOperations(directory: string): boolean {
  try { return readdirSync(join(directory, 'operations')).length === 0; }
  catch (error) { if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') return true; throw error; }
}
function single(db: DatabaseSync, table: string, fields: string): Record<string, unknown> {
  const rows = db.prepare(`SELECT ${fields} FROM ${table}`).all();
  if (rows.length !== 1 || rows[0]?.id !== 1) return unconfirmed();
  return rows[0]!;
}

/** The initializing marker commits before ledger creation; activation commits after it. */
export class ClockAuthority {
  private ledger?: DatabaseSync;
  private ledgerFile: Stats | undefined;
  private needsBinding = false;
  private prepared = false;
  private closed = false;
  constructor(private readonly db: DatabaseSync, private readonly directory: string,
    private readonly retentionMs: number, private readonly version: number,
    private readonly ledgerName: string, private readonly settings: string,
    private readonly requiredTables: readonly string[], private readonly token: string,
    private readonly initializing: boolean) {}

  /** True permits first schema creation. Existing authority is never repaired in place. */
  requireAuthority(db: DatabaseSync): boolean {
    if (this.closed || this.ledger || !db.isTransaction) return unconfirmed();
    this.ledger = db;
    this.ledgerFile = file(join(this.directory, this.ledgerName));
    if (!this.ledgerFile) return unconfirmed();
    if (tables(db).size === 0) {
      if (!this.initializing || !emptyOperations(this.directory)) return unconfirmed();
      return this.needsBinding = true;
    }
    this.validate(db);
    // An initializing namespace cannot have admitted any operation before activation.
    if (this.initializing && (!emptyOperations(this.directory) ||
      db.prepare('SELECT 1 FROM operations LIMIT 1').get() || db.prepare('SELECT 1 FROM refs LIMIT 1').get())) return unconfirmed();
    this.prepared = true;
    return false;
  }

  /** Called only for a new schema, in the same transaction as its policy and clock row. */
  bind(db: DatabaseSync): void {
    if (this.closed || this.ledger !== db || !db.isTransaction || !this.needsBinding) return unconfirmed();
    db.exec('CREATE TABLE authority(id INTEGER PRIMARY KEY CHECK(id=1),token TEXT NOT NULL)');
    db.prepare('INSERT INTO authority VALUES(1,?)').run(this.token);
    this.validate(db);
    this.needsBinding = false;
    this.prepared = true;
  }

  private validate(db: DatabaseSync): void {
    const present = tables(db);
    for (const name of ['policy', 'operations', 'refs', 'elapsed_clock', 'authority', ...this.requiredTables]) {
      if (!present.has(name)) return unconfirmed();
    }
    const policy = single(db, 'policy', 'id,version,retentionMs,settings');
    if (policy.version !== this.version || policy.retentionMs !== this.retentionMs || policy.settings !== this.settings) return unconfirmed();
    if (single(db, 'authority', 'id,token').token !== this.token) return unconfirmed();
    const clock = single(db, 'elapsed_clock', 'id,ageMs,revision');
    if (!Number.isSafeInteger(clock.ageMs) || Number(clock.ageMs) < 0 ||
      !Number.isSafeInteger(clock.revision) || Number(clock.revision) < 0) return unconfirmed();
    // Preparing these projections also rejects a missing critical column without mutating it.
    db.prepare('SELECT operationHash,fingerprint,writeHash,acceptedAt,expiresAt,status,dataDeleted,confirmed,acceptedAgeMs,expiresAgeMs FROM operations LIMIT 0').all();
    db.prepare('SELECT refHash,operationHash FROM refs LIMIT 0').all();
  }

  activate(): void {
    if (this.closed || !this.prepared || !this.ledger || this.ledger.isTransaction) return unconfirmed();
    const current = file(join(this.directory, this.ledgerName));
    if (!current || current.dev !== this.ledgerFile?.dev || current.ino !== this.ledgerFile.ino) return unconfirmed();
    if (this.initializing) {
      const changed = this.db.prepare("UPDATE clock_authority SET state='active' WHERE id=1 AND token=? AND state='initializing'").run(this.token);
      if (changed.changes !== 1) return unconfirmed();
    }
    this.db.exec('COMMIT');
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    try { if (this.db.isTransaction) this.db.exec('ROLLBACK'); }
    finally { this.db.close(); }
  }
}

/** Legacy constructors see a fixed format fence; this writer lock serializes activation. */
export function clockFence(Database: typeof DatabaseSync, directory: string, retentionMs: number,
  options: AuthorityOptions = {}): ClockAuthority {
  const version = options.version ?? 5, ledgerName = options.ledgerFile ?? 'retention-v2.sqlite';
  const path = join(directory, 'retention.sqlite'), ledgerPath = join(directory, ledgerName);
  const previous = file(path);
  if (!previous && file(ledgerPath) && !file(path)) return unconfirmed();
  const db = new Database(path);
  try {
    chmodSync(path, 0o600);
    db.exec('PRAGMA busy_timeout=0; PRAGMA synchronous=EXTRA; BEGIN IMMEDIATE');
    const present = tables(db);
    if (present.size === 0) {
      // A zero-length file can remain after SQLite was opened but setup never committed.
      if ((previous && previous.size !== 0) || file(ledgerPath) || !emptyOperations(directory)) return unconfirmed();
      db.exec('CREATE TABLE policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL); CREATE TABLE clock_authority(id INTEGER PRIMARY KEY CHECK(id=1),state TEXT NOT NULL,token TEXT NOT NULL)');
      db.prepare('INSERT INTO policy VALUES(1,?,?)').run(version, retentionMs);
      db.prepare("INSERT INTO clock_authority VALUES(1,'initializing',?)").run(randomUUID());
      // Persist the identity before a ledger can exist. Reacquire and reread because
      // a simultaneous initializer may complete the namespace between transactions.
      db.exec('COMMIT; BEGIN IMMEDIATE');
    }
    if (!tables(db).has('policy')) return unconfirmed();
    const policy = single(db, 'policy', 'id,version,retentionMs');
    if (policy.version !== version || policy.retentionMs !== retentionMs) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    if (!tables(db).has('clock_authority')) return unconfirmed();
    const authority = single(db, 'clock_authority', 'id,state,token');
    if (!['initializing', 'active'].includes(String(authority.state)) || typeof authority.token !== 'string' ||
      !/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/.test(authority.token)) return unconfirmed();
    if (authority.state === 'active' && !file(ledgerPath)) return unconfirmed();
    return new ClockAuthority(db, directory, retentionMs, version, ledgerName,
      options.settings ?? '{"expiryClock":"verified-uptime"}', options.requiredTables ?? [],
      authority.token, authority.state === 'initializing');
  } catch (error) {
    try { if (db.isTransaction) db.exec('ROLLBACK'); }
    finally { db.close(); }
    throw error;
  }
}
