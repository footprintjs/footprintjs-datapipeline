/** Values crossing an adapter, repository or trace boundary must be JSON safe. */
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[];
export interface JsonObject {
  readonly [key: string]: JsonValue;
}

/** An immutable implementation, schema or source configuration revision. */
export interface VersionedRef {
  readonly id: string;
  readonly version: string;
}

export interface Source {
  readonly ref: VersionedRef;
  readonly adapterRef: VersionedRef;
  /** Public configuration only. Resolve credentials outside persisted records. */
  readonly config?: JsonObject;
}

export interface BatchRef {
  readonly kind: 'batch';
  readonly id: string;
}
export interface ObservationRef {
  readonly kind: 'observation';
  readonly id: string;
}
export interface EvidenceRef {
  readonly kind: 'evidence';
  readonly id: string;
}
export interface TraceRef {
  readonly kind: 'trace';
  readonly id: string;
}
export type InputRef = BatchRef | ObservationRef;
export type RecordRef = InputRef | EvidenceRef;

/** The repository must verify these bytes before acknowledging their record. */
export interface ExternalArtifactRef {
  readonly uri: string;
  readonly sha256: string;
  readonly sizeBytes: number;
  readonly mediaType?: string;
}

export type ObservationPayload =
  | { readonly payload: JsonValue; readonly externalArtifactRef?: never }
  | { readonly payload?: never; readonly externalArtifactRef: ExternalArtifactRef };

export type TimeBasis = 'source' | 'collection';
/** Collection time never silently stands in for a source event/window time. */
export type ObservationTime =
  | { readonly kind: 'point'; readonly at: string; readonly basis: TimeBasis }
  | {
      readonly kind: 'interval';
      readonly start: string;
      readonly end: string;
      readonly basis: TimeBasis;
    }
  | { readonly kind: 'source-time-unknown'; readonly reason?: string };

export interface Coverage {
  readonly status: 'complete' | 'partial' | 'unknown';
  readonly time: ObservationTime;
  /** Adapter-defined population/filter description; not an inferred universe. */
  readonly selection?: JsonValue;
  readonly reason?: string;
}

export interface PipelineError {
  readonly code: string;
  readonly message: string;
  readonly details?: JsonValue;
}

/** Keys identify logical items within a candidate, not execution attempts. */
export type CandidateObservation = ObservationPayload & {
  readonly key: string;
  readonly subject: JsonObject;
  readonly schemaRef: VersionedRef;
  readonly time: ObservationTime;
  /** Local capture time, retained independently of source time availability. */
  readonly collectedAt?: string;
  /** Exact support for this derived observation, when the transform knows it. */
  readonly inputRefs?: readonly InputRef[];
};

export type BatchOutcome = 'complete' | 'partial' | 'failed';
/** One bounded, finite result. The host enforces its observation/payload limits. */
export interface CandidateBatch {
  readonly key: string;
  readonly observations: readonly CandidateObservation[];
  readonly coverage: Coverage;
  readonly outcome: BatchOutcome;
  readonly errors?: readonly PipelineError[];
  /** Capture receipts, selection details or calculation semantics. */
  readonly details?: JsonObject;
}

export type ObservationSupport =
  | { readonly kind: 'source'; readonly sourceRef: VersionedRef }
  | { readonly kind: 'inputs'; readonly inputRefs: readonly InputRef[] }
  | { readonly kind: 'batches'; readonly inputBatchRefs: readonly BatchRef[] };

export type Observation = CandidateObservation & {
  readonly kind: 'observation';
  readonly ref: ObservationRef;
  readonly batchRef: BatchRef;
  readonly evidenceRef: EvidenceRef;
  /** Batch support explicitly means finer observation lineage was not supplied. */
  readonly support: ObservationSupport;
};

export interface Batch {
  readonly kind: 'batch';
  readonly ref: BatchRef;
  readonly key: string;
  readonly operationId: string;
  readonly observationRefs: readonly ObservationRef[];
  readonly evidenceRef: EvidenceRef;
  readonly coverage: Coverage;
  readonly outcome: BatchOutcome;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly sourceRef?: VersionedRef;
  readonly methodRef?: VersionedRef;
  readonly errors?: readonly PipelineError[];
}

/** The interval is half open [start, end), on the explicitly named clock. */
export interface DeriveWindow {
  readonly basis: TimeBasis;
  readonly start: string;
  readonly end: string;
}

export interface CollectRequest {
  readonly sourceRef: VersionedRef;
  /** Reuse for retries of one operation; do not reuse for different requests. */
  readonly idempotencyKey: string;
  readonly request: JsonValue;
}

export interface DeriveRequest {
  readonly methodRef: VersionedRef;
  readonly idempotencyKey: string;
  readonly inputBatchRefs: readonly BatchRef[];
  readonly window: DeriveWindow;
  /** Optional transform parameters, included in the operation fingerprint. */
  readonly request?: JsonValue;
}

export type EvidenceOrigin =
  | {
      readonly kind: 'collect';
      readonly sourceRef: VersionedRef;
      readonly adapterRef: VersionedRef;
      readonly sourceConfigFingerprint: string;
      readonly request: JsonValue;
    }
  | {
      readonly kind: 'derive';
      readonly methodRef: VersionedRef;
      readonly inputBatchRefs: readonly BatchRef[];
      readonly window: DeriveWindow;
      readonly request?: JsonValue;
    };

/** Durable structured provenance; a full FootPrint trace is stored separately. */
export interface Evidence {
  readonly kind: 'evidence';
  readonly ref: EvidenceRef;
  readonly operationId: string;
  /** Canonical operation/request + source configuration digest. */
  readonly fingerprint: string;
  readonly origin: EvidenceOrigin;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly outcome: BatchOutcome;
  readonly outputRefs: readonly InputRef[];
  readonly errors?: readonly PipelineError[];
  readonly details?: JsonObject;
}

export type StoredRecord = Observation | Batch | Evidence;
export interface BatchInput {
  readonly batch: Batch;
  readonly observations: readonly Observation[];
}

/** `partial` acknowledges only part of storage publication, never source coverage. */
export type CommitStatus = 'committed' | 'partial' | 'failed' | 'unknown';
export interface CommitReceipt {
  readonly operationId: string;
  /** Storage acknowledgement only; incomplete source coverage is independent. */
  readonly status: CommitStatus;
  readonly batchRef?: BatchRef;
  readonly evidenceRef?: EvidenceRef;
  readonly observationRefs?: readonly ObservationRef[];
  readonly committedAt?: string;
  readonly error?: PipelineError;
}

export type TraceOutcome =
  | { readonly status: 'stored'; readonly ref: TraceRef }
  | { readonly status: 'unavailable'; readonly error: PipelineError }
  | { readonly status: 'disabled' };

export interface PipelineResult {
  readonly commit: CommitReceipt;
  readonly trace: TraceOutcome;
}

export interface TraceRecord {
  readonly operationId: string;
  readonly recordedAt: string;
  readonly value: JsonValue;
}
