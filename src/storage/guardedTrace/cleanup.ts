import { lstat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { traceId, TraceStorageError } from './contracts.js';
import type { TraceLedger } from './ledger.js';
/** Tombstones precede deletion; interrupted work remains charged until retried. */
export async function sweepExpired(path: string, ledger: TraceLedger, now: number, limit: number): Promise<{ deleted: number }> {
 if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new TraceStorageError('TRACE_INVALID_OPTIONS');
 const rows = ledger.expired(now, limit); let deleted = 0;
 for (const row of rows) {
  traceId({ kind: 'trace', id: row.id });
  ledger.markExpired(row.id);
  const file = join(path, 'traces', `${row.id}.json.gz`);
  try {
   const info = await lstat(file);
   if (!info.isFile() || info.isSymbolicLink()) throw new TraceStorageError('TRACE_UNSAFE_FILESYSTEM');
   await unlink(file);
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  ledger.released(row.id); deleted++;
 }
 return { deleted };
}
