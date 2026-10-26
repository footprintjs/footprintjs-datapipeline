import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError } from './contracts.js';
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
