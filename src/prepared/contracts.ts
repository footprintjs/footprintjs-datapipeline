import type { Batch, BatchInput, Coverage, JsonObject, JsonValue, Observation, PipelineResult, RecordRef, VersionedRef } from '../contracts.js';
import type { Pipeline } from '../createPipeline.js';

/** Host-owned acquisition identity. Never take a namespace from an untrusted query. */
export interface CollectionSchedule {
  readonly ref: VersionedRef;
  readonly namespace: string;
  readonly sourceRef: VersionedRef;
  readonly methodRef: VersionedRef;
  readonly summaryRef: VersionedRef;
  readonly startAt: string;
  readonly intervalMs: number;
  readonly request: JsonValue;
  /** A late tick may collect the current slot; it never fabricates a past capture. */
  readonly maxLatenessMs?: number;
  readonly timeoutMs?: number;
}
export interface PreparedSummaryCandidate {
  readonly key: string;
  readonly subject: JsonObject;
  /** Actual collection receipt time, not scheduled slot time or derivation time. */
  readonly collectedAt: string;
  /** Domain-defined sufficient statistics and bounded source-reference metadata. */
  readonly payload: JsonObject;
}
export interface SummaryProjector {
  readonly ref: VersionedRef;
  /** Optional trusted ingestion hook. Author an ordinary Footprint flow here,
   * once per committed poll, and return compact references for project().
   * Never return raw rows or the complete execution record in this metadata.
   * Change summaryRef when enabling it; old history is not retroactively traced. */
  prepareWindow?(context: {
    readonly schedule: CollectionSchedule;
    readonly slotAt: string;
    readonly collection: BatchInput;
    readonly derivation: BatchInput;
    readonly read: Pipeline['read'];
    readonly signal: AbortSignal;
  }): Promise<JsonObject>;
  project(observation: Observation, context: {
    readonly schedule: CollectionSchedule;
    readonly collectionBatch: Batch;
    readonly collection: PipelineResult;
    readonly derivation: PipelineResult;
    /** Collection-time only. Queries never invoke the projector or this read port. */
    readonly read: Pipeline['read'];
    readonly signal: AbortSignal;
    /** Immutable metadata from this poll's prepareWindow(), if configured. */
    readonly window?: JsonObject;
  }): Promise<PreparedSummaryCandidate | undefined>;
}
export interface PreparedSummary extends PreparedSummaryCandidate {
  readonly id: string;
  readonly namespace: string;
  readonly scheduleRef: VersionedRef;
  readonly sourceRef: VersionedRef;
  readonly methodRef: VersionedRef;
  readonly summaryRef: VersionedRef;
  readonly slotAt: string;
  readonly observationRef: Observation['ref'];
  readonly batchRef: Observation['batchRef'];
  readonly evidenceRef: Observation['evidenceRef'];
  readonly collectionBatchRef: Batch['ref'];
  readonly collectionEvidenceRef: Batch['evidenceRef'];
  readonly inputRefs: readonly RecordRef[];
}
export type PollStatus = 'collected' | 'empty' | 'failed' | 'missed';
export interface PreparedPoll {
  readonly namespace: string;
  readonly scheduleRef: VersionedRef;
  readonly slotAt: string;
  readonly status: PollStatus;
  readonly attemptedAt?: string;
  readonly finishedAt?: string;
  readonly collection?: PipelineResult;
  readonly derivation?: PipelineResult;
  readonly coverage?: Coverage;
  /** A bounded public reason, never a source response or credential-bearing error. */
  readonly reason?: string;
  readonly summaries: readonly PreparedSummary[];
}
export interface PreparedQuery {
  readonly schedule: CollectionSchedule;
  /** Half-open range over summary.collectedAt, independently of scheduled slots. */
  readonly start: string;
  readonly end: string;
  readonly limit?: number;
}
export interface PreparedQueryResult {
  readonly summaries: readonly PreparedSummary[];
  readonly polls: readonly Omit<PreparedPoll, 'summaries'>[];
  readonly coverage: {
    readonly expectedPolls: number;
    readonly collectedPolls: number;
    readonly emptyPolls: number;
    readonly failedPolls: number;
    readonly missedPolls: number;
    readonly pendingPolls: number;
    readonly additionalReceiptPolls: number;
    readonly meaning: 'scheduled-slots-and-collection-receipts';
  };
  readonly truncated: boolean;
}
/** An implementation must publish poll status and its summaries atomically. */
export interface SummaryStore {
  register(schedule: CollectionSchedule): Promise<void>;
  readPoll(schedule: CollectionSchedule, slotAt: string): Promise<PreparedPoll | undefined>;
  publish(schedule: CollectionSchedule, poll: PreparedPoll): Promise<void>;
  latestSlot(schedule: CollectionSchedule): Promise<string | undefined>;
  query(request: PreparedQuery): Promise<PreparedQueryResult>;
  readSummary(schedule: CollectionSchedule, id: string): Promise<PreparedSummary | undefined>;
  close?(): void | Promise<void>;
}
