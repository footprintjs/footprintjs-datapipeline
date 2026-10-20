import type { JsonObject, Observation } from '../contracts.js';
import type { NumericMetricDefinition } from '../statistics/numericStatistics.js';
import type { TraceStore } from '../ports.js';

export const RUN_RECEIPT_SCHEMA = Object.freeze({ id: 'datapipeline.run-receipt', version: '1' });
export const RUN_CONTRIBUTION_SCHEMA = Object.freeze({ id: 'datapipeline.run-contribution', version: '1' });
export const MAX_RUN_WINDOW_SLOTS = 64;
export const MAX_RUN_WINDOW_INPUTS = 8192;
export interface RunWindowEvidenceOptions {
  /** Construction is inert unless the host explicitly opts in. */
  readonly enabled?: boolean;
  /** One declared series per window; partition dimensions in the host. */
  readonly metric: NumericMetricDefinition;
  readonly anchorAt: string;
  readonly intervalMs: number;
  readonly traceStore?: TraceStore;
}
export type RunReceiptStatus = 'complete' | 'partial' | 'failed' | 'empty';
export interface RunReceiptPayload extends JsonObject {
  readonly slotAt: string;
  readonly status: RunReceiptStatus;
  readonly rawDisposition: 'retained' | 'not-retained' | 'expired';
  readonly retentionPolicyRef: { readonly id: string; readonly version: string };
}
export type RunContributionPayload = {
  readonly receiptKey: string;
  readonly metric: NumericMetricDefinition;
} & ({ readonly status: 'valid'; readonly value: number }
  | { readonly status: 'missing' | 'invalid'; readonly value?: never });
export interface RunSlot {
  readonly at: string;
  readonly receipt?: Observation;
  readonly contribution?: Observation;
}
export class RunWindowEvidenceError extends Error {
  readonly code = 'RUN_WINDOW_INVALID';
  constructor() { super('Saved run/window evidence was refused.'); }
}
