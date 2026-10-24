import { existsSync } from 'node:fs';
import { join } from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError } from './contracts.js';
/** Called inside the legacy-format fence transaction. Pending initialization
 * may resume, but an active fence never recreates a lost history ledger. */
export function historyFence(db: DatabaseSync, directory: string, namespaceId: string, fresh: boolean): boolean {
  if (fresh) {
    db.exec("CREATE TABLE history_namespace(id INTEGER PRIMARY KEY CHECK(id=1),namespaceId TEXT NOT NULL,state TEXT NOT NULL)");
    db.prepare("INSERT INTO history_namespace VALUES(1,?,'initializing')").run(namespaceId);
  }
  const row = db.prepare('SELECT * FROM history_namespace WHERE id=1').get();
  if (!row || !['initializing', 'active'].includes(String(row.state))) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  if (row.namespaceId !== namespaceId) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  if (row.state === 'active' && !existsSync(join(directory, 'retention-v4.sqlite'))) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  return row.state === 'active';
}
/** Active authority is validated before any CREATE/INSERT OR IGNORE, so damaged
 * checkpoints, clocks or ownership cannot be silently reinitialized as empty. */
export function validateActiveHistory(db: DatabaseSync): void {
  for (const table of ['policy', 'operations', 'refs', 'elapsed_clock', 'retired_history', 'history_counts', 'history_scan']) {
    if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  }
  for (const name of ['history_operation_insert', 'history_operation_delete', 'history_ref_insert', 'history_ref_delete']) {
    if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?").get(name)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  }
  for (const table of ['policy', 'elapsed_clock', 'retired_history', 'history_counts', 'history_scan']) {
    if (!db.prepare(`SELECT 1 FROM ${table} WHERE id=1`).get()) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  }
}
export function activateHistoryFence(Database: typeof DatabaseSync, directory: string, namespaceId: string): void {
  const db = new Database(join(directory, 'retention.sqlite'));
  try {
    db.exec('PRAGMA busy_timeout=0; PRAGMA synchronous=FULL; BEGIN IMMEDIATE');
    const row = db.prepare('SELECT namespaceId FROM history_namespace WHERE id=1').get();
    if (row?.namespaceId !== namespaceId) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    db.exec("UPDATE history_namespace SET state='active' WHERE id=1; COMMIT");
  } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  finally { db.close(); }
}
