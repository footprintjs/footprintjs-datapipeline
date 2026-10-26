import type { DatabaseSync } from 'node:sqlite';
/** Indexed, resumable reclamation. Both operation and reference work have independent hard batch bounds. */
export function reclaimHistory(db: DatabaseSync, floor: number, limit: number, referenceLimit = 16 * limit) {
  let reclaimedOperations = 0, reclaimedReferences = 0;
  const until = performance.now() + 10;
  while (reclaimedOperations < limit && reclaimedReferences < referenceLimit && performance.now() < until) {
    const row = db.prepare('SELECT operationHash FROM operations WHERE period<? ORDER BY period,operationHash LIMIT 1').get(floor);
    if (!row) break;
    const refs = db.prepare('SELECT refHash FROM refs WHERE operationHash=? ORDER BY refHash LIMIT ?').all(row.operationHash!, Math.min(referenceLimit - reclaimedReferences, 16));
    if (refs.length) {
      const remove = db.prepare('DELETE FROM refs WHERE refHash=? AND operationHash=?');
      for (const ref of refs) remove.run(ref.refHash!, row.operationHash!);
      reclaimedReferences += refs.length;
    } else {
      db.prepare('DELETE FROM operations WHERE operationHash=?').run(row.operationHash!); reclaimedOperations++;
    }
  }
  return { reclaimedOperations, reclaimedReferences,
    pending: !!db.prepare('SELECT 1 FROM operations WHERE period<? LIMIT 1').get(floor) };
}
