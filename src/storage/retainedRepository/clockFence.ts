import { chmodSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError } from './contracts.js';
/** Legacy constructors see only a fixed format fence, never the elapsed-age ledger. */
export function clockFence(Database: typeof DatabaseSync, directory: string, retentionMs: number): void {
  const path = join(directory, 'retention.sqlite'), db = new Database(path);
  try {
    chmodSync(path, 0o600);
    db.exec('PRAGMA busy_timeout=0; PRAGMA synchronous=FULL; BEGIN IMMEDIATE');
    db.exec('CREATE TABLE IF NOT EXISTS policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL)');
    db.prepare('INSERT OR IGNORE INTO policy VALUES(1,2,?)').run(retentionMs);
    const policy = db.prepare('SELECT version,retentionMs FROM policy WHERE id=1').get();
    if (policy?.version !== 2 || policy.retentionMs !== retentionMs) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    db.exec('COMMIT');
  } catch (error) {
    if (db.isTransaction) db.exec('ROLLBACK');
    throw error;
  } finally { db.close(); }
}
