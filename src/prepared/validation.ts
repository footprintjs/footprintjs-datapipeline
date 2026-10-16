import type { PipelineResult, RecordRef, VersionedRef } from '../contracts.js';
import { assertTime, assertTimestamp, canonical, digest } from '../validation.js';
import type { CollectionSchedule, PreparedPoll, PreparedSummary } from './contracts.js';

/** Bounded, public errors: contract failures never include supplied data. */
export class PreparedContractError extends Error {
  constructor() { super('Invalid prepared collection contract.'); this.name = 'PreparedContractError'; }
  readonly code = 'invalid_prepared_contract';
}
export const PREPARED_MAX_SUMMARY_BYTES = 256 * 1024;
export const PREPARED_MAX_POLL_BYTES = 8 * 1024 * 1024;
export const PREPARED_MAX_SUMMARIES = 10_000;
export const PREPARED_MAX_QUERY_POLLS = 20_000;
export const PREPARED_MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

function check(condition: unknown): asserts condition { if (!condition) throw new PreparedContractError(); }
function encoded(value: unknown, maxBytes: number): string {
  try { const text = canonical(value); check(Buffer.byteLength(text) <= maxBytes); return text; }
  catch { throw new PreparedContractError(); }
}
function object(value: unknown): asserts value is Record<string, unknown> {
  check(value !== null && typeof value === 'object' && !Array.isArray(value));
}
function text(value: unknown, max = 1024): asserts value is string {
  check(typeof value === 'string' && value.trim().length > 0 && Buffer.byteLength(value) <= max);
}
function keys(value: Record<string, unknown>, names: readonly string[]): void {
  check(Object.keys(value).every(key => names.includes(key)));
}
function ref(value: unknown): asserts value is VersionedRef {
  object(value); keys(value, ['id', 'version']); text(value.id, 512); text(value.version, 512);
}
function recordRef(value: unknown, kind?: RecordRef['kind']): asserts value is RecordRef {
  object(value); keys(value, ['kind', 'id']);
  check(kind ? value.kind === kind : ['batch', 'observation', 'evidence'].includes(String(value.kind)));
  text(value.id);
}
function same(a: unknown, b: unknown): boolean {
  try { return canonical(a) === canonical(b); } catch { return false; }
}
function timestamp(value: unknown): asserts value is string {
  try { assertTimestamp(value); check(Number.isSafeInteger(Date.parse(value))); }
  catch { throw new PreparedContractError(); }
}

export function preparedDigest(value: unknown): string {
  try { return digest(value); } catch { throw new PreparedContractError(); }
}
export function scheduleFingerprint(schedule: CollectionSchedule): string { return preparedDigest(validateSchedule(schedule)); }
export function preparedSummaryId(summary: Omit<PreparedSummary, 'id'> | PreparedSummary): string {
  const clone = JSON.parse(encoded(summary, PREPARED_MAX_SUMMARY_BYTES)) as Record<string, unknown>;
  object(clone);
  delete clone.id;
  return preparedDigest(clone);
}

/** Validation returns a JSON snapshot so later caller mutation cannot change identity. */
export function validateSchedule(value: unknown): CollectionSchedule {
  const clone = JSON.parse(encoded(value, 128 * 1024)) as CollectionSchedule;
  object(clone); keys(clone as unknown as Record<string, unknown>, ['ref', 'namespace', 'sourceRef', 'methodRef', 'summaryRef', 'startAt', 'intervalMs', 'request', 'maxLatenessMs', 'timeoutMs']);
  ref(clone.ref); text(clone.namespace, 512); ref(clone.sourceRef); ref(clone.methodRef); ref(clone.summaryRef);
  timestamp(clone.startAt); check(Number.isSafeInteger(clone.intervalMs) && clone.intervalMs >= 1000);
  check(Object.hasOwn(clone, 'request')); encoded(clone.request, 64 * 1024);
  if (clone.maxLatenessMs !== undefined) check(Number.isSafeInteger(clone.maxLatenessMs) && clone.maxLatenessMs >= 0 && clone.maxLatenessMs <= clone.intervalMs);
  if (clone.timeoutMs !== undefined) check(Number.isSafeInteger(clone.timeoutMs) && clone.timeoutMs > 0 && clone.timeoutMs <= 2_147_483_647);
  return clone;
}

export function slotMilliseconds(schedule: CollectionSchedule, slotAt: string): number {
  timestamp(slotAt);
  const slot = Date.parse(slotAt); const start = Date.parse(schedule.startAt);
  check(slot >= start && (slot - start) % schedule.intervalMs === 0);
  return slot;
}

function result(value: unknown): asserts value is PipelineResult {
  object(value); keys(value, ['commit', 'trace']); object(value.commit); object(value.trace);
  const commit = value.commit; text(commit.operationId);
  keys(commit, ['operationId', 'status', 'batchRef', 'evidenceRef', 'observationRefs', 'committedAt', 'error']);
  check(['committed', 'partial', 'failed', 'unknown'].includes(String(commit.status)));
  if (commit.batchRef !== undefined) recordRef(commit.batchRef, 'batch');
  if (commit.evidenceRef !== undefined) recordRef(commit.evidenceRef, 'evidence');
  if (commit.committedAt !== undefined) timestamp(commit.committedAt);
  if (commit.error !== undefined) { object(commit.error); text(commit.error.code); text(commit.error.message, 4096); keys(commit.error, ['code', 'message', 'details']); }
  if (commit.status === 'committed') check(commit.batchRef && commit.evidenceRef);
  if (commit.observationRefs !== undefined) {
    check(Array.isArray(commit.observationRefs) && commit.observationRefs.length <= PREPARED_MAX_SUMMARIES);
    for (const item of commit.observationRefs) recordRef(item, 'observation');
    check(new Set(commit.observationRefs.map(item => canonical(item))).size === commit.observationRefs.length);
  }
  check(['stored', 'unavailable', 'disabled'].includes(String(value.trace.status)));
  if (value.trace.status === 'stored') { keys(value.trace, ['status', 'ref']); object(value.trace.ref); keys(value.trace.ref, ['kind', 'id']); check(value.trace.ref.kind === 'trace'); text(value.trace.ref.id); }
  else if (value.trace.status === 'unavailable') { keys(value.trace, ['status', 'error']); object(value.trace.error); text(value.trace.error.code); text(value.trace.error.message, 4096); keys(value.trace.error, ['code', 'message', 'details']); }
  else keys(value.trace, ['status']);
}
function committed(value: PipelineResult | undefined): asserts value is PipelineResult {
  check(value?.commit.status === 'committed' && value.commit.batchRef && value.commit.evidenceRef);
}

export function validatePreparedSummary(schedule: CollectionSchedule, value: unknown): PreparedSummary {
  const clone = JSON.parse(encoded(value, PREPARED_MAX_SUMMARY_BYTES)) as PreparedSummary;
  object(clone); keys(clone as unknown as Record<string, unknown>, ['id', 'key', 'subject', 'payload', 'collectedAt', 'namespace', 'scheduleRef', 'sourceRef', 'methodRef', 'summaryRef', 'slotAt', 'observationRef', 'batchRef', 'evidenceRef', 'collectionBatchRef', 'collectionEvidenceRef', 'inputRefs']);
  text(clone.key); object(clone.subject); object(clone.payload); timestamp(clone.collectedAt);
  check(clone.namespace === schedule.namespace);
  for (const name of ['scheduleRef', 'sourceRef', 'methodRef', 'summaryRef'] as const) {
    ref(clone[name]); check(same(clone[name], name === 'scheduleRef' ? schedule.ref : schedule[name]));
  }
  slotMilliseconds(schedule, clone.slotAt);
  recordRef(clone.observationRef, 'observation'); recordRef(clone.batchRef, 'batch'); recordRef(clone.evidenceRef, 'evidence');
  recordRef(clone.collectionBatchRef, 'batch'); recordRef(clone.collectionEvidenceRef, 'evidence');
  check(Array.isArray(clone.inputRefs) && clone.inputRefs.length > 0 && clone.inputRefs.length <= PREPARED_MAX_SUMMARIES);
  for (const input of clone.inputRefs) recordRef(input);
  check(typeof clone.id === 'string' && /^[a-f0-9]{64}$/.test(clone.id) && clone.id === preparedSummaryId(clone));
  return clone;
}

function pollMetadata(checkedSchedule: CollectionSchedule, clone: Omit<PreparedPoll, 'summaries'>): void {
  object(clone);
  check(clone.namespace === checkedSchedule.namespace && same(clone.scheduleRef, checkedSchedule.ref));
  slotMilliseconds(checkedSchedule, clone.slotAt);
  check(['collected', 'empty', 'failed', 'missed'].includes(clone.status));
  if (clone.attemptedAt !== undefined) timestamp(clone.attemptedAt);
  if (clone.finishedAt !== undefined) timestamp(clone.finishedAt);
  if (clone.reason !== undefined) text(clone.reason, 1024);
  if (clone.collection !== undefined) result(clone.collection);
  if (clone.derivation !== undefined) result(clone.derivation);
  if (clone.coverage !== undefined) {
    object(clone.coverage); keys(clone.coverage as unknown as Record<string, unknown>, ['status', 'time', 'selection', 'reason']);
    check(['complete', 'partial', 'unknown'].includes(clone.coverage.status));
    if (clone.coverage.reason !== undefined) text(clone.coverage.reason, 4096);
    try { assertTime(clone.coverage.time); } catch { throw new PreparedContractError(); }
  }
  if (clone.status === 'collected') { committed(clone.collection); committed(clone.derivation); }
  if (clone.status === 'empty') { committed(clone.collection); if (clone.derivation !== undefined) committed(clone.derivation); }
  if (clone.status === 'missed') check(clone.collection === undefined && clone.derivation === undefined);
}

export function validatePollMetadata(schedule: CollectionSchedule, value: unknown): Omit<PreparedPoll, 'summaries'> {
  const clone = JSON.parse(encoded(value, PREPARED_MAX_POLL_BYTES)) as Omit<PreparedPoll, 'summaries'>;
  object(clone); keys(clone as unknown as Record<string, unknown>, ['namespace', 'scheduleRef', 'slotAt', 'status', 'attemptedAt', 'finishedAt', 'collection', 'derivation', 'coverage', 'reason']);
  pollMetadata(validateSchedule(schedule), clone);
  return clone;
}

export function validatePoll(schedule: CollectionSchedule, value: unknown): PreparedPoll {
  const checkedSchedule = validateSchedule(schedule);
  const clone = JSON.parse(encoded(value, PREPARED_MAX_POLL_BYTES)) as PreparedPoll;
  object(clone); keys(clone as unknown as Record<string, unknown>, ['namespace', 'scheduleRef', 'slotAt', 'status', 'attemptedAt', 'finishedAt', 'collection', 'derivation', 'coverage', 'reason', 'summaries']);
  pollMetadata(checkedSchedule, clone);
  const { summaries: _summaries, ...metadata } = clone;
  validatePollMetadata(checkedSchedule, metadata);
  check(Array.isArray(clone.summaries) && clone.summaries.length <= PREPARED_MAX_SUMMARIES);
  if (clone.status === 'collected') check(clone.summaries.length > 0);
  else check(clone.summaries.length === 0);
  const identifiers = new Set<string>(); const observationIds = new Set<string>(); const candidateKeys = new Set<string>();
  for (const summary of clone.summaries) {
    validatePreparedSummary(checkedSchedule, summary);
    check(Date.parse(summary.slotAt) === Date.parse(clone.slotAt));
    check(!identifiers.has(summary.id) && !observationIds.has(summary.observationRef.id) && !candidateKeys.has(summary.key));
    identifiers.add(summary.id); observationIds.add(summary.observationRef.id); candidateKeys.add(summary.key);
    check(same(summary.collectionBatchRef, clone.collection!.commit.batchRef) && same(summary.collectionEvidenceRef, clone.collection!.commit.evidenceRef));
    check(same(summary.batchRef, clone.derivation!.commit.batchRef) && same(summary.evidenceRef, clone.derivation!.commit.evidenceRef));
    check(clone.derivation!.commit.observationRefs?.some(item => same(item, summary.observationRef)));
  }
  return clone;
}

export function validatePreparedRange(schedule: CollectionSchedule, start: unknown, end: unknown, requestedLimit?: number): {
  startMs: number; endMs: number; limit: number; expectedPolls: number;
} {
  timestamp(start); timestamp(end);
  const startMs = Date.parse(start); const endMs = Date.parse(end);
  check(startMs < endMs && endMs - startMs <= PREPARED_MAX_RANGE_MS);
  const limit = requestedLimit ?? 10_000;
  check(Number.isSafeInteger(limit) && limit > 0 && limit <= 50_000);
  const origin = Date.parse(schedule.startAt);
  const first = Math.max(0, Math.ceil((startMs - origin) / schedule.intervalMs));
  const last = Math.max(0, Math.ceil((endMs - origin) / schedule.intervalMs));
  const expectedPolls = last - first;
  check(Number.isSafeInteger(expectedPolls) && expectedPolls <= PREPARED_MAX_QUERY_POLLS);
  return { startMs, endMs, limit, expectedPolls };
}
