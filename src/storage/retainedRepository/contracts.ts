export type RepositoryRetentionCode = 'REPOSITORY_INVALID_OPTIONS' | 'REPOSITORY_UNMANAGED' | 'REPOSITORY_BUSY' | 'REPOSITORY_EXPIRED' | 'REPOSITORY_UNCONFIRMED';
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
  readonly onHealth: (event: { readonly code: RepositoryRetentionCode }) => void;
}
export interface RepositoryRetentionReceipt {
  readonly operationHash: string;
  readonly acceptedAt: number;
  readonly expiresAt: number;
  readonly status: 'pending' | 'stored' | 'expired';
  readonly publication: 'unconfirmed' | 'committed';
  readonly dataDeleted: boolean;
}
