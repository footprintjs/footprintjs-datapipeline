import type { DatabaseSync } from 'node:sqlite';
import type { RecordRef } from '../../contracts.js';
import { MAX_REPOSITORY_READ_BATCH } from '../../ports.js';
import { validRef } from '../filesystemRecords.js';
import { operationHash } from './commit.js';
import { address, HISTORY_PERIOD_MS } from './historyIdentity.js';
import { historyInventory } from './historyInventory.js';
import type { Row } from './ledger.js';
import { RepositoryRetentionError, type RepositoryHistoryPolicy, type RepositoryHistoryStatus } from './contracts.js';

/** Same control ledger, one cumulative checkpoint; no per-retirement journal. */
export class HistoryLedger {
  constructor(private readonly db: DatabaseSync, private readonly policy: RepositoryHistoryPolicy,
    private readonly age: () => number, private readonly validate: (hash: string) => Row) {
    db.exec(`CREATE TABLE IF NOT EXISTS retired_history(id INTEGER PRIMARY KEY CHECK(id=1),
      floor INTEGER NOT NULL,generation INTEGER NOT NULL,operations INTEGER NOT NULL,
      refs INTEGER NOT NULL,sha256 TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS period_members ON operations(period,operationHash);
      CREATE INDEX IF NOT EXISTS reference_owners ON refs(operationHash);`);
    db.prepare('INSERT OR IGNORE INTO retired_history VALUES(1,0,0,0,0,?)').run('0'.repeat(64));
  }
  status(): RepositoryHistoryStatus {
    const row = this.db.prepare('SELECT * FROM retired_history WHERE id=1').get();
    const currentPeriod = Math.floor(this.age() / HISTORY_PERIOD_MS);
    if (!row || ![row.floor, row.generation, row.operations, row.refs].every(n => Number.isSafeInteger(n) && Number(n) >= 0)
      || Number(row.floor) > Math.max(0, currentPeriod - this.policy.keepDays)
      || typeof row.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(row.sha256)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    return { currentPeriod, retireBefore: Number(row.floor), generation: Number(row.generation),
      retiredOperations: Number(row.operations), retiredReferences: Number(row.refs), checkpointSha256: row.sha256 };
  }
  check(id: string, kind: string, intent: 'lookup' | 'admit' = 'lookup'): number {
    const parsed = address(id, this.policy, kind), state = this.status();
    if (parsed.period < state.retireBefore) throw new RepositoryRetentionError('REPOSITORY_HISTORY_UNAVAILABLE');
    if (parsed.period > state.currentPeriod) throw new RepositoryRetentionError('REPOSITORY_PERIOD_CLOSED');
    if (intent === 'admit' && !this.db.prepare('SELECT 1 FROM operations WHERE operationHash=?').get(operationHash(id)) &&
      (parsed.period !== state.currentPeriod || parsed.period !== this.policy.period)) throw new RepositoryRetentionError('REPOSITORY_PERIOD_CLOSED');
    return parsed.period;
  }
  checkRefs(refs: readonly RecordRef[]): void {
    if (!Array.isArray(refs) || refs.length > MAX_REPOSITORY_READ_BATCH || refs.some(ref => !validRef(ref))) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    for (const ref of refs) this.check(ref.id, ref.kind);
  }
  /** One synchronous control transaction; file deletion was confirmed earlier.
   * Inventory work is bounded by the namespace's hard operation/reference caps.
   * Physical row reclamation is additionally bounded by the caller's batch size. */
  retire(limit: number): { retireBefore: number; reclaimedOperations: number; blocked: boolean } {
    let state = this.status();
    const cutoff = Math.max(0, state.currentPeriod - this.policy.keepDays);
    const oldest = this.db.prepare('SELECT min(period) AS p FROM operations WHERE period>=? AND period<?')
      .get(state.retireBefore, cutoff)!.p;
    let blocked = false;
    if (state.retireBefore < cutoff) {
      const next = oldest === null ? cutoff : Number(oldest) + 1;
      const period = oldest === null ? undefined : Number(oldest);
      const inventory = historyInventory(this.db, state, period, next, this.age(), this.validate);
      blocked = !inventory;
      if (inventory) {
        const operations = state.retiredOperations + inventory.operations, refs = state.retiredReferences + inventory.refs;
        if (![operations, refs, state.generation + 1].every(Number.isSafeInteger)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
        this.db.prepare('UPDATE retired_history SET floor=?,generation=?,operations=?,refs=?,sha256=? WHERE id=1')
          .run(next, state.generation + 1, operations, refs, inventory.sha256);
        state = this.status();
      }
    }
    // The prefix and these deletes commit atomically. A crash leaves either the
    // old exact inventory or the new fence; remaining garbage is selected by period.
    const garbage = this.db.prepare('SELECT operationHash FROM operations WHERE period<? ORDER BY period,operationHash LIMIT ?')
      .all(state.retireBefore, limit);
    for (const row of garbage) {
      this.db.prepare('DELETE FROM refs WHERE operationHash=?').run(row.operationHash!);
      this.db.prepare('DELETE FROM operations WHERE operationHash=?').run(row.operationHash!);
    }
    return { retireBefore: state.retireBefore, reclaimedOperations: garbage.length, blocked };
  }
}
