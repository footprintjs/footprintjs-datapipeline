import type { RecordRef } from '../../contracts.js';
import { digest } from '../../validation.js';
import { validRef } from '../filesystemRecords.js';
import { operationHash } from '../retainedRepository/commit.js';
import { readCatalogue } from './catalogue.js';
import { MAX_SEALED_HISTORY_BYTES, SealedHistoryError, fail, type SealedFilesystemHistory, type SealedHistoryLookup, type SealedHistoryOperation, type SealedHistoryOptions, type SealedHistorySnapshot, type SealedHistoryView } from './contracts.js';
import { encodeHistory } from './encode.js';
import { decodeHistory, type DecodedHistory } from './decode.js';
import { accessDenied, rootDirectory } from './files.js';
import { checkNamespace, initializeNamespace } from './namespace.js';
import { publishSnapshot } from './publication.js';

export { MAX_SEALED_HISTORY_BYTES, SealedHistoryError } from './contracts.js';
export type { SealedFilesystemHistory, SealedHistoryOptions, SealedHistoryOperation, SealedHistorySnapshot, SealedHistoryPublication, SealedHistoryView, SealedHistoryLookup, SealedHistoryReceiptLookup, SealedHistoryCode } from './contracts.js';
function checked<T>(task: () => T): T {
  try { return task(); }
  catch (error) { if (error instanceof SealedHistoryError) throw error; return fail('SEALED_HISTORY_INVALID_OPTIONS'); }
}
function view(decoded: DecodedHistory): SealedHistoryView {
  const absent = Object.freeze({ state: 'not-in-snapshot' as const });
  const result = (operation: SealedHistoryOperation | undefined): SealedHistoryLookup =>
    operation ? Object.freeze({ state: 'found', operation }) : absent;
  const lookupOperation = (id: string) => checked(() => {
    if (typeof id !== 'string' || !id) fail('SEALED_HISTORY_INVALID_OPTIONS');
    return result(decoded.row(operationHash(id)));
  });
  return Object.freeze({ counts: decoded.counts, lookupOperation,
    lookupReference(ref: RecordRef) { return checked(() => {
      if (!validRef(ref)) fail('SEALED_HISTORY_INVALID_OPTIONS');
      return result(decoded.owner(digest(ref)));
    }); },
    receipt(id: string) {
      const found = lookupOperation(id);
      if (found.state === 'not-in-snapshot') return found;
      const { fingerprint: _fingerprint, writeHash: _writeHash, ...receipt } = found.operation;
      return Object.freeze({ state: 'found' as const, receipt: Object.freeze(receipt) });
    },
  });
}
/** Metadata-only opt-in; does not replace a Repository or remove hot rows. */
export function createSealedFilesystemHistory(options: SealedHistoryOptions): SealedFilesystemHistory | undefined {
  if (options.enabled !== true) return undefined;
  const limit = options.maxSnapshotBytes ?? MAX_SEALED_HISTORY_BYTES;
  if (typeof options.directory !== 'string' || !options.directory || !Number.isSafeInteger(options.retentionMs) || options.retentionMs < 1 ||
      !Number.isSafeInteger(limit) || limit < 96 || limit > MAX_SEALED_HISTORY_BYTES ||
      (options.requireSeparateFilesystem !== undefined && typeof options.requireSeparateFilesystem !== 'boolean')) fail('SEALED_HISTORY_INVALID_OPTIONS');
  const config = { ...options };
  return Object.freeze({
    async publish(snapshot: SealedHistorySnapshot) {
      try {
        const bytes = encodeHistory(snapshot, config.retentionMs, limit);
        if (!decodeHistory(bytes, config.retentionMs, limit).terminal) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
        const directory = await rootDirectory(config);
        await initializeNamespace(directory, config.retentionMs);
        return await publishSnapshot(directory, bytes, config.retentionMs, limit);
      } catch (error) {
        if (error instanceof SealedHistoryError) throw error;
        return fail('SEALED_HISTORY_UNCONFIRMED');
      }
    },
    async open() {
      try {
        const directory = await rootDirectory(config);
        if (!await checkNamespace(directory, config.retentionMs)) fail('SEALED_HISTORY_UNAVAILABLE');
        const loaded = await readCatalogue(directory, config.retentionMs, limit);
        if (!loaded) fail('SEALED_HISTORY_UNAVAILABLE');
        return view(loaded.decoded);
      } catch (error) {
        if (error instanceof SealedHistoryError) throw error;
        return fail(accessDenied(error) ? 'SEALED_HISTORY_UNMANAGED' : 'SEALED_HISTORY_UNAVAILABLE');
      }
    },
  });
}
