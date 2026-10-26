import { createHash } from 'node:crypto';
import type { DatabaseSync } from 'node:sqlite';
import type { RecordRef } from '../../contracts.js';
import { MAX_REPOSITORY_READ_BATCH } from '../../ports.js';
import { canonical } from '../../validation.js';
import { validRef } from '../filesystemRecords.js';
import { operationHash } from './commit.js';
import { address, HISTORY_PERIOD_MS } from './historyIdentity.js';
import { historyInventory, resetHistoryInventory } from './historyInventory.js';
import { createHistorySchema } from './historySchema.js';
import { reclaimHistory } from './historyReclaim.js';
import type { Row } from './ledger.js';
import { RepositoryRetentionError, type RepositoryHistoryPolicy, type RepositoryHistoryStatus } from './contracts.js';
/** Same control ledger, one cumulative checkpoint and one bounded verification cursor. */
export class HistoryLedger {
  constructor(private readonly db: DatabaseSync, private readonly policy: RepositoryHistoryPolicy,
    private readonly age: () => number, private readonly validate: (hash: string) => Row) { createHistorySchema(db); }
  status(): RepositoryHistoryStatus {
    const row = this.db.prepare('SELECT * FROM retired_history WHERE id=1').get();
    const currentPeriod = Math.floor(this.age() / HISTORY_PERIOD_MS);
    if (!row || ![row.floor, row.generation, row.operations, row.refs, row.unconfirmed].every(n => Number.isSafeInteger(n) && Number(n) >= 0)
      || Number(row.unconfirmed) > Number(row.operations) || Number(row.floor) > Math.max(0, currentPeriod - this.policy.keepDays)
      || typeof row.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(row.sha256)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    return { currentPeriod, retireBefore: Number(row.floor), generation: Number(row.generation), retiredOperations: Number(row.operations),
      retiredReferences: Number(row.refs), retiredUnconfirmedOperations: Number(row.unconfirmed), checkpointSha256: row.sha256 };
  }
  check(id: string, kind: string, intent: 'lookup' | 'admit' = 'lookup'): number {
    const parsed = address(id, this.policy, kind), state = this.status();
    if (parsed.period < state.retireBefore) throw new RepositoryRetentionError('REPOSITORY_HISTORY_UNAVAILABLE');
    if (parsed.period > state.currentPeriod || (intent === 'admit' && parsed.period !== state.currentPeriod &&
      !this.db.prepare('SELECT 1 FROM operations WHERE operationHash=?').get(operationHash(id)))) throw new RepositoryRetentionError('REPOSITORY_PERIOD_CLOSED');
    return parsed.period;
  }
  checkRefs(refs: readonly RecordRef[]): void {
    if (!Array.isArray(refs) || refs.length > MAX_REPOSITORY_READ_BATCH || refs.some(ref => !validRef(ref))) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    for (const ref of refs) this.check(ref.id, ref.kind);
  }
  retire(limit: number) {
    const state = this.status(), cutoff = Math.max(0, state.currentPeriod - this.policy.keepDays);
    const result = { retireBefore: state.retireBefore, reclaimedOperations: 0, reclaimedReferences: 0, verifiedItems: 0, verifiedOperations: 0, verifiedReferences: 0, blocked: false, pending: false };
    // Reclaim previously fenced rows first. No full reference-table scan or large DELETE.
    if (this.db.prepare('SELECT 1 FROM operations WHERE period<? LIMIT 1').get(state.retireBefore)) {
      const garbage = reclaimHistory(this.db, state.retireBefore, limit);
      return { ...result, ...garbage, pending: garbage.pending || state.retireBefore < cutoff };
    }
    if (state.retireBefore >= cutoff) return result;
    const oldest = this.db.prepare('SELECT period FROM operations WHERE period>=? AND period<? ORDER BY period,operationHash LIMIT 1').get(state.retireBefore, cutoff);
    const period = oldest ? Number(oldest.period) : undefined, next = period === undefined ? cutoff : period + 1;
    const inventory = period === undefined ? { operations: 0, refs: 0, unconfirmed: 0, verifiedItems: 0, verifiedOperations: 0, verifiedReferences: 0, blocked: false, complete: true,
      sha256: createHash('sha256').update(state.checkpointSha256).update(canonical({ from: state.retireBefore, to: next })).digest('hex') }
      : historyInventory(this.db, state, period, this.age(), limit, this.validate);
    if (!inventory.complete) return { ...result, verifiedItems: inventory.verifiedItems, verifiedOperations: inventory.verifiedOperations, verifiedReferences: inventory.verifiedReferences, blocked: inventory.blocked, pending: true };
    const operations = state.retiredOperations + inventory.operations, refs = state.retiredReferences + inventory.refs,
      unconfirmed = state.retiredUnconfirmedOperations + inventory.unconfirmed;
    if (![operations, refs, unconfirmed, state.generation + 1].every(Number.isSafeInteger)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    this.db.prepare('UPDATE retired_history SET floor=?,generation=?,operations=?,refs=?,unconfirmed=?,sha256=? WHERE id=1')
      .run(next, state.generation + 1, operations, refs, unconfirmed, inventory.sha256);
    resetHistoryInventory(this.db);
    const garbage = reclaimHistory(this.db, next, limit - inventory.verifiedOperations, 16 * limit - inventory.verifiedReferences);
    return { ...result, ...garbage, retireBefore: next, verifiedItems: inventory.verifiedItems, verifiedOperations: inventory.verifiedOperations, verifiedReferences: inventory.verifiedReferences, pending: garbage.pending || next < cutoff };
  }
}
