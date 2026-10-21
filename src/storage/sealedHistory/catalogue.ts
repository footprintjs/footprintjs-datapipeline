import { join } from 'node:path';
import type { SealedHistoryPublication } from './contracts.js';
import { decodeHistory, type DecodedHistory } from './decode.js';
import { missing, privateDirectory, readBounded } from './files.js';
import { fail, hash, isHash } from './layout.js';

export interface Catalogue {
  readonly version: 1;
  readonly retentionMs: number;
  readonly snapshot: Omit<SealedHistoryPublication, 'status'>;
}
export async function readCatalogue(directory: string, retentionMs: number, limit: number): Promise<{ catalogue: Catalogue; decoded: DecodedHistory } | undefined> {
  let bytes: Buffer;
  try { bytes = await readBounded(join(directory, 'catalogue.json'), 2048); }
  catch (error) { if (missing(error)) return undefined; throw error; }
  let value: Catalogue;
  try { value = JSON.parse(bytes.toString('utf8')) as Catalogue; } catch { return fail('SEALED_HISTORY_CORRUPT'); }
  if (!value || value.version !== 1) fail('SEALED_HISTORY_UNSUPPORTED_FORMAT');
  const snapshot = value.snapshot;
  if (value.retentionMs !== retentionMs || !snapshot || !isHash(snapshot.sha256) ||
      !Number.isSafeInteger(snapshot.bytes) || snapshot.bytes < 96 ||
      !Number.isSafeInteger(snapshot.operations) || snapshot.operations < 0 ||
      !Number.isSafeInteger(snapshot.references) || snapshot.references < 0) fail('SEALED_HISTORY_CORRUPT');
  if (snapshot.bytes > limit) fail('SEALED_HISTORY_LIMIT');
  let data: Buffer;
  try {
    await privateDirectory(join(directory, 'snapshots'));
    data = await readBounded(join(directory, 'snapshots', snapshot.sha256 + '.bin'), limit);
  } catch (error) { if (missing(error)) fail('SEALED_HISTORY_CORRUPT'); throw error; }
  if (data.length !== snapshot.bytes || hash(data) !== snapshot.sha256) fail('SEALED_HISTORY_CORRUPT');
  const decoded = decodeHistory(data, retentionMs, limit);
  if (!decoded.terminal || decoded.counts.operations !== snapshot.operations || decoded.counts.references !== snapshot.references) fail('SEALED_HISTORY_CORRUPT');
  return { catalogue: value, decoded };
}
