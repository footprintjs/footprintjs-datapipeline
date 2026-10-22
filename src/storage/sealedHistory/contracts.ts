import type { RecordRef } from '../../contracts.js';
import type { RepositoryRetentionReceipt } from '../retainedRepository/contracts.js';

export type SealedHistoryCode =
  | 'SEALED_HISTORY_INVALID_OPTIONS' | 'SEALED_HISTORY_INVALID_SNAPSHOT'
  | 'SEALED_HISTORY_UNMANAGED' | 'SEALED_HISTORY_UNSUPPORTED_FORMAT'
  | 'SEALED_HISTORY_CORRUPT' | 'SEALED_HISTORY_LIMIT'
  | 'SEALED_HISTORY_UNCONFIRMED' | 'SEALED_HISTORY_CONFLICT'
  | 'SEALED_HISTORY_UNAVAILABLE';
export class SealedHistoryError extends Error {
  constructor(readonly code: SealedHistoryCode) {
    super('Sealed history operation was refused.');
    this.name = 'SealedHistoryError';
  }
}
export function fail(code: SealedHistoryCode): never { throw new SealedHistoryError(code); }
export const MAX_SEALED_HISTORY_BYTES = 16 * 1024 * 1024;
export interface SealedHistoryOptions {
  readonly enabled?: boolean;
  /**
   * An existing, empty, private directory on a local POSIX filesystem.
   * All publishers of one namespace must share one host and PID namespace.
   */
  readonly directory: string;
  /** Original payload expiry interval; this is not a history retention period. */
  readonly retentionMs: number;
  /** May lower the 16 MiB complete snapshot bound. */
  readonly maxSnapshotBytes?: number;
  readonly requireSeparateFilesystem?: boolean;
}
/** Exact retained-ledger metadata, not source values or a claim of authenticity. */
export interface SealedHistoryOperation extends RepositoryRetentionReceipt {
  readonly fingerprint: string;
  readonly writeHash: string;
}
export interface SealedHistorySnapshot {
  readonly operations: readonly SealedHistoryOperation[];
  readonly references: readonly {
    readonly refHash: string;
    readonly operationHash: string;
  }[];
}
export interface SealedHistoryPublication {
  readonly status: 'stored';
  readonly sha256: string;
  readonly bytes: number;
  readonly operations: number;
  readonly references: number;
}
export type SealedHistoryLookup =
  | { readonly state: 'found'; readonly operation: SealedHistoryOperation }
  | { readonly state: 'not-in-snapshot' };
export type SealedHistoryReceiptLookup =
  | { readonly state: 'found'; readonly receipt: RepositoryRetentionReceipt }
  | { readonly state: 'not-in-snapshot' };
export interface SealedHistoryView {
  readonly counts: { readonly operations: number; readonly references: number };
  /** A miss names only this selected snapshot; it cannot authorize admission. */
  lookupOperation(operationId: string): SealedHistoryLookup;
  lookupReference(ref: RecordRef): SealedHistoryLookup;
  receipt(operationId: string): SealedHistoryReceiptLookup;
}
export interface SealedFilesystemHistory {
  /** One immutable selected snapshot; not migration, compaction or admission. */
  publish(snapshot: SealedHistorySnapshot): Promise<SealedHistoryPublication>;
  /**
   * Revalidates saved files without reclaiming stages; the view is detached.
   * SQLite may create sidecars for a WAL marker; read-only mounts are not guaranteed.
   */
  open(): Promise<SealedHistoryView>;
}
