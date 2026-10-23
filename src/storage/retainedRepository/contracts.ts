export const RETAINED_REPOSITORY_SAFETY_VERSION = 1;
export type RepositoryRetentionCode = 'REPOSITORY_INVALID_OPTIONS' | 'REPOSITORY_UNMANAGED' | 'REPOSITORY_BUSY' | 'REPOSITORY_EXPIRED' | 'REPOSITORY_UNCONFIRMED' | 'REPOSITORY_CLOCK_UNAVAILABLE' | 'REPOSITORY_METADATA_LIMIT';
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
  constructor(readonly code: RepositoryRetentionCode) { super('Retained repository access was refused.'); this.name = 'RepositoryRetentionError'; }
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
