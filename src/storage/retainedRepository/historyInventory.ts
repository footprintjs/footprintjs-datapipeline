import { createHash } from 'node:crypto';
import type { DatabaseSync } from 'node:sqlite';
import { canonical } from '../../validation.js';
import { RepositoryRetentionError, type RepositoryHistoryStatus } from './contracts.js';
import type { Row } from './ledger.js';
/** Inspect the complete period in the same control transaction. Terminal proof
 * is required for every member; a selected subset can never authorize retirement. */
export function historyInventory(db: DatabaseSync, state: RepositoryHistoryStatus, period: number | undefined,
  next: number, now: number, validate: (hash: string) => Row) {
  if (db.prepare('SELECT 1 FROM refs LEFT JOIN operations USING(operationHash) WHERE operations.operationHash IS NULL LIMIT 1').get()) {
    throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  }
  const hash = createHash('sha256').update(state.checkpointSha256).update(canonical({ from: state.retireBefore, to: next }));
  let operations = 0, refs = 0;
  if (period !== undefined) for (const item of db.prepare('SELECT operationHash FROM operations WHERE period=? ORDER BY operationHash').iterate(period)) {
    const row = validate(String(item.operationHash));
    if (row.status !== 'expired' || row.publication !== 'committed' || !row.dataDeleted) return undefined;
    if (row.expiresAgeMs! > now) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    hash.update(canonical(row)); operations++;
    for (const ref of db.prepare('SELECT * FROM refs WHERE operationHash=? ORDER BY refHash').iterate(item.operationHash!)) {
      if (typeof ref.refHash !== 'string' || !/^[a-f0-9]{64}$/.test(ref.refHash)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      hash.update(canonical(ref)); refs++;
    }
  }
  return { operations, refs, sha256: hash.digest('hex') };
}
