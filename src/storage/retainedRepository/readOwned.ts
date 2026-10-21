import type { RecordRef, StoredRecord } from '../../contracts.js';
import { MAX_REPOSITORY_READ_BATCH } from '../../ports.js';
import { digest } from '../../validation.js';
import { validRef } from '../filesystemRecords.js';
import { RepositoryRetentionError } from './contracts.js';
import type { RepositoryLedger, Row } from './ledger.js';
import type { IndexedData } from './indexedData.js';
export async function readOwned(refs: readonly RecordRef[], ledger: RepositoryLedger, data: IndexedData, fresh: (row: Row) => void, publishing?: string): Promise<readonly (StoredRecord | undefined)[]> {
  if (!Array.isArray(refs) || refs.length > MAX_REPOSITORY_READ_BATCH || refs.some(ref => !validRef(ref))) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  const results: (StoredRecord | undefined)[] = Array.from({ length: refs.length });
  const groups = new Map<string, { refs: RecordRef[]; positions: number[] }>();
  for (const [i, ref] of refs.entries()) {
    const row = ledger.owner(digest(ref));
    if (!row) continue;
    fresh(row);
    if (row.status === 'pending') {
      if (publishing === row.operationHash) continue;
      if (!await data.reconcileHash(row.operationHash)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      ledger.stored(row.operationHash);
    }
    const group = groups.get(row.operationHash) ?? { refs: [], positions: [] };
    group.refs.push(ref); group.positions.push(i); groups.set(row.operationHash, group);
  }
  for (const [hash, group] of groups) {
    const rows = await data.readAt(hash, group.refs);
    for (const [i, row] of rows.entries()) {
      if (!row) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      results[group.positions[i]!] = row;
    }
  }
  return results;
}
