export const RETAINED_REPOSITORY_SAFETY_VERSION = 1;
export const REPOSITORY_HISTORY_VERSION = 2;
export type RepositoryRetentionCode = 'REPOSITORY_INVALID_OPTIONS' | 'REPOSITORY_UNMANAGED' | 'REPOSITORY_BUSY' | 'REPOSITORY_EXPIRED' | 'REPOSITORY_UNCONFIRMED' | 'REPOSITORY_CLOCK_UNAVAILABLE' | 'REPOSITORY_METADATA_LIMIT' | 'REPOSITORY_HISTORY_UNAVAILABLE' | 'REPOSITORY_PERIOD_CLOSED' | 'REPOSITORY_HISTORY_BLOCKED';
export interface RepositoryHistoryPolicy {
  readonly profile: 'daily-periods-v1';
  /** Required finite host policy. The laptop example explicitly chooses seven. */
  readonly keepDays: number;
  /** A permanent, host-owned namespace instance: 32 lowercase hex characters. */
  readonly namespaceId: string;
  /** Explicit scope for NEW work. Rebinding to a later period is not a retry. */
  readonly period: number;
  /** Bound the native packed content of each accepted operation. */
  readonly maxOperationBytes: number;
}
export interface RepositoryHistoryStatus {
  readonly currentPeriod: number;
  readonly retireBefore: number;
  readonly generation: number;
  readonly retiredOperations: number;
  readonly retiredReferences: number;
  /** Publication was uncertain; only physical deletion is confirmed. */
  readonly retiredUnconfirmedOperations: number;
  readonly checkpointSha256: string;
}
export interface RepositoryMetadataLimits {
  readonly maxBytes: number;
  readonly maxOperations: number;
  readonly maxReferences: number;
}
export interface RepositoryMetadataCapacity {
  readonly available: boolean;
  readonly operations: number;
  readonly references: number;
  readonly remainingOperations: number;
  readonly remainingReferences: number;
  readonly controlFileBytes: number;
  readonly databasePages: number;
  readonly databasePageLimit: number;
  readonly limits: RepositoryMetadataLimits;
}
export class RepositoryRetentionError extends Error {
  constructor(readonly code: RepositoryRetentionCode) { super(code === 'REPOSITORY_HISTORY_UNAVAILABLE'
    ? 'Retained history is no longer kept.' : 'Retained repository access was refused.'); this.name = 'RepositoryRetentionError'; }
}
export interface RetainedRepositoryOptions {
  readonly enabled?: boolean;
  readonly directory: string;
  /** Explicit host policy; no default or inferred age for existing records. */
  readonly retentionMs: number;
  readonly requireSeparateFilesystem?: boolean;
  readonly clock?: () => number;
  /** New namespaces only. Downtime is not credited to expiry without elapsed-time evidence. */
  readonly expiryClock?: 'verified-uptime';
  /** Trusted host/test elapsed-time port; defaults to performance.now. */
  readonly monotonicClock?: () => number;
  /** New verified-uptime namespaces only; retained identities are never removed at the limit. */
  readonly metadataLimits?: RepositoryMetadataLimits;
  /** New format only. Never adopts or truncates an existing namespace. */
  readonly historyRetirement?: RepositoryHistoryPolicy;
  readonly onHealth: (event: { readonly code: RepositoryRetentionCode }) => void;
}
export interface RepositoryRetentionReceipt {
  readonly operationHash: string;
  readonly acceptedAt: number;
  readonly expiresAt: number;
  readonly status: 'pending' | 'stored' | 'expired';
  readonly publication: 'unconfirmed' | 'committed';
  readonly dataDeleted: boolean;
  /** In this profile expiresAt is a nominal wall timestamp, not the deletion clock. */
  readonly expiryClock?: 'verified-uptime';
  readonly acceptedAgeMs?: number;
  readonly expiresAgeMs?: number;
}
