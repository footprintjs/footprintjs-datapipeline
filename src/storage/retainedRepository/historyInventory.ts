import { createHash } from 'node:crypto';
import type { DatabaseSync } from 'node:sqlite';
import { canonical } from '../../validation.js';
import { RepositoryRetentionError, type RepositoryHistoryStatus } from './contracts.js';
import type { Row } from './ledger.js';
interface Scan { floor: number; period: number; cursorOp: string; cursorRef: string; phase: string;
  operations: number; refs: number; unconfirmed: number; sha256: string }
const invalid = (): never => { throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED'); };
const hash = (previous: string, value: unknown) => createHash('sha256').update(previous).update(canonical(value)).digest('hex');
/** One persisted hash chain; at most limit operations and 16 × limit references per transaction.
 * Closed periods admit no new work. Verified expired/deleted members cannot revive. */
export function historyInventory(db: DatabaseSync, state: RepositoryHistoryStatus, period: number,
  now: number, limit: number, validate: (hash: string) => Row) {
  const scan = db.prepare('SELECT * FROM history_scan WHERE id=1').get() as unknown as Scan | undefined;
  if (!scan || ![scan.floor, scan.operations, scan.refs, scan.unconfirmed].every(n => Number.isSafeInteger(n) && n >= 0) ||
    !Number.isSafeInteger(scan.period) || scan.period < -1 || !['operation', 'references'].includes(scan.phase) ||
    !/^[a-f0-9]{64}$/.test(scan.sha256) || !/^(?:[a-f0-9]{64})?$/.test(scan.cursorOp) ||
    !/^(?:[a-f0-9]{64})?$/.test(scan.cursorRef) || scan.unconfirmed > scan.operations) return invalid();
  if (scan.period === -1) Object.assign(scan, { floor: state.retireBefore, period,
    sha256: hash(state.checkpointSha256, { from: state.retireBefore, to: period + 1 }) });
  if (scan.floor !== state.retireBefore || scan.period !== period) return invalid();
  let verifiedItems = 0, verifiedOperations = 0, verifiedReferences = 0, blocked = false, complete = false;
  const until = performance.now() + 10;
  while (performance.now() < until) {
    if (scan.phase === 'operation') {
      if (verifiedOperations >= limit) break;
      const next = db.prepare('SELECT operationHash FROM operations WHERE period=? AND operationHash>? ORDER BY operationHash LIMIT 1').get(period, scan.cursorOp);
      if (!next) { complete = true; break; }
      const row = validate(String(next.operationHash));
      if (!row || row.status !== 'expired' || !row.dataDeleted) { blocked = true; break; }
      if (row.expiresAgeMs! > now) return invalid();
      scan.sha256 = hash(scan.sha256, { operation: row }); scan.operations++;
      if (row.publication === 'unconfirmed') scan.unconfirmed++;
      scan.cursorOp = row.operationHash; scan.cursorRef = ''; scan.phase = 'references'; verifiedItems++; verifiedOperations++;
    } else {
      if (verifiedReferences >= 16 * limit) break;
      const refs = db.prepare('SELECT * FROM refs WHERE operationHash=? AND refHash>? ORDER BY refHash LIMIT ?')
        .all(scan.cursorOp, scan.cursorRef, Math.min(16 * limit - verifiedReferences, 16));
      if (!refs.length) { scan.phase = 'operation'; continue; }
      for (const ref of refs) {
        if (typeof ref.refHash !== 'string' || !/^[a-f0-9]{64}$/.test(ref.refHash) || ref.operationHash !== scan.cursorOp) return invalid();
        scan.sha256 = hash(scan.sha256, { reference: ref }); scan.refs++; scan.cursorRef = ref.refHash; verifiedItems++; verifiedReferences++;
      }
    }
  }
  db.prepare('UPDATE history_scan SET floor=?,period=?,cursorOp=?,cursorRef=?,phase=?,operations=?,refs=?,unconfirmed=?,sha256=? WHERE id=1')
    .run(scan.floor, scan.period, scan.cursorOp, scan.cursorRef, scan.phase, scan.operations, scan.refs, scan.unconfirmed, scan.sha256);
  return { ...scan, verifiedItems, verifiedOperations, verifiedReferences, blocked, complete };
}
export function resetHistoryInventory(db: DatabaseSync): void {
  db.prepare("UPDATE history_scan SET floor=0,period=-1,cursorOp='',cursorRef='',phase='operation',operations=0,refs=0,unconfirmed=0,sha256=? WHERE id=1").run('0'.repeat(64));
}
