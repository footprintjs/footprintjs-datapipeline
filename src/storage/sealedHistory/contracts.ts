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
export const MAX_SEALED_HISTORY_BYTES = 16 * 1024 * 1024;
export interface SealedHistoryOptions {
  readonly enabled?: boolean;
  /** An existing, empty, private directory on a local POSIX filesystem. */
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
export interface SealedHistoryView {
  readonly counts: { readonly operations: number; readonly references: number };
  /** Absence means only "not in this selected snapshot". */
  lookupOperation(operationId: string): SealedHistoryOperation | undefined;
  lookupReference(ref: RecordRef): SealedHistoryOperation | undefined;
  receipt(operationId: string): RepositoryRetentionReceipt | undefined;
}
export interface SealedFilesystemHistory {
  /** One immutable selected snapshot; not migration, compaction or admission. */
  publish(snapshot: SealedHistorySnapshot): Promise<SealedHistoryPublication>;
  /** Revalidates the saved files; the returned metadata view is detached. */
  open(): Promise<SealedHistoryView>;
}
