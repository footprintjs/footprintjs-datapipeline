import type {
  Batch,
  BatchInput,
  CandidateBatch,
  CommitReceipt,
  DeriveRequest,
  Evidence,
  JsonValue,
  Observation,
  RecordRef,
  Source,
  StoredRecord,
  TraceRecord,
  TraceRef,
  VersionedRef,
} from './contracts.js';

export interface OperationContext {
  readonly signal: AbortSignal;
  /** Stable logical operation identity; never an execution-attempt identifier. */
  readonly operationId: string;
  readonly startedAt: string;
  readonly now: () => string;
}

/** Registries must resolve the exact requested revision, without latest fallback. */
export interface Registry<T extends { readonly ref: VersionedRef }> {
  resolve(ref: VersionedRef): T | undefined;
}

export interface SourceAdapter {
  readonly ref: VersionedRef;
  /** Return one finite candidate. IDs and durable publication belong to core. */
  collect(source: Source, request: JsonValue, context: OperationContext): Promise<CandidateBatch>;
}

export interface Transform {
  readonly ref: VersionedRef;
  /** Committed batches, with observations selected on the request's explicit clock/window. */
  apply(
    inputs: readonly BatchInput[],
    request: DeriveRequest,
    context: OperationContext,
  ): Promise<CandidateBatch>;
}

export interface CommitWrite {
  readonly operationId: string;
  /** Canonical request/config digest used to reject idempotency-key conflicts. */
  readonly fingerprint: string;
  readonly batch: Batch;
  readonly observations: readonly Observation[];
  readonly evidence: Evidence;
}

export interface OperationCommit {
  readonly fingerprint: string;
  readonly receipt: CommitReceipt;
}

export interface Repository {
  /** Reconcile a retry or an acknowledgement whose commit outcome was unknown. */
  findOperation(operationId: string): Promise<OperationCommit | undefined>;
  /**
   * Publish data + structured evidence together, preserving core-supplied refs.
   * The same operation/fingerprint returns its existing receipt; a changed
   * fingerprint is a conflict. Verify referenced external artifacts before
   * acknowledging publication. An ambiguous durable outcome is `unknown`.
   */
  commit(write: CommitWrite): Promise<CommitReceipt>;
  /** Only published records may be read; missing records return undefined. */
  read(ref: RecordRef): Promise<StoredRecord | undefined>;
}

/** Called after the data outcome is known; trace failure cannot undo that outcome. */
export interface TraceStore {
  store(trace: TraceRecord): Promise<TraceRef>;
}
