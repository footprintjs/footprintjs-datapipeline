import type { DatabaseSync } from 'node:sqlite';

export function inTransaction(db: DatabaseSync): boolean {
  // isTransaction was added after the supported Node 22.13 minimum. A nested
  // BEGIN is rejected without changing the existing transaction; if it starts
  // instead, roll back only that empty probe transaction before returning.
  if (typeof db.isTransaction === 'boolean') return db.isTransaction;
  try { db.exec('BEGIN DEFERRED'); }
  catch (error) {
    if (error instanceof Error && Object.getOwnPropertyDescriptor(error, 'code')?.value === 'ERR_SQLITE_ERROR' &&
      Object.getOwnPropertyDescriptor(error, 'errcode')?.value === 1 &&
      error.message === 'cannot start a transaction within a transaction') return true;
    throw error;
  }
  db.exec('ROLLBACK');
  return false;
}
