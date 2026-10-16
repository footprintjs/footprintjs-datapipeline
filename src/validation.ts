import { createHash } from 'node:crypto';
import type { CandidateBatch, ObservationTime, VersionedRef } from './contracts.js';

export class PipelineFault extends Error {
  constructor(readonly code: string, message: string) { super(message); this.name = 'PipelineFault'; }
}
export function ensure(condition: unknown, message: string): asserts condition {
  if (!condition) throw new PipelineFault('INVALID_CONTRACT', message);
}

/** Canonical JSON also rejects values JSON.stringify would silently discard. */
export function canonical(value: unknown): string {
  const ancestors = new Set<object>();
  function visit(item: unknown): string {
    if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
    if (typeof item === 'number') { ensure(Number.isFinite(item), 'Numbers must be finite'); return JSON.stringify(item); }
    ensure(typeof item === 'object' && item !== null, 'Values must be JSON safe');
    ensure(!ancestors.has(item), 'Cyclic values are not JSON');
    ensure(Array.isArray(item) || Object.getPrototypeOf(item) === Object.prototype || Object.getPrototypeOf(item) === null, 'Only plain JSON objects are supported');
    ensure(Object.getOwnPropertySymbols(item).length === 0, 'Symbol keys are not JSON');
    ancestors.add(item);
    let result: string;
    if (Array.isArray(item)) {
      ensure(Object.keys(item).length === item.length, 'Sparse arrays or extra array properties are not JSON');
      const elements: string[] = [];
      for (let index = 0; index < item.length; index++) {
        const property = Object.getOwnPropertyDescriptor(item, String(index));
        ensure(property && 'value' in property, 'Sparse arrays and accessors are not JSON');
        elements.push(visit(property.value));
      }
      result = '[' + elements.join(',') + ']';
    } else {
      result = '{' + Object.keys(item).sort().map(key => {
        const property = Object.getOwnPropertyDescriptor(item, key)!;
        ensure('value' in property, 'Accessors are not JSON');
        return JSON.stringify(key) + ':' + visit(property.value);
      }).join(',') + '}';
    }
    ancestors.delete(item);
    return result;
  }
  return visit(value);
}
export function cloneJson<T>(value: T): T { return JSON.parse(canonical(value)) as T; }
export function digest(value: unknown): string { return createHash('sha256').update(canonical(value)).digest('hex'); }
export function assertRef(ref: unknown): asserts ref is VersionedRef {
  ensure(ref !== null && typeof ref === 'object', 'A versioned reference is required');
  const value = ref as VersionedRef;
  ensure(typeof value.id === 'string' && value.id.trim() && typeof value.version === 'string' && value.version.trim(), 'References need a nonempty id and exact version');
}
export function assertTimestamp(value: unknown): asserts value is string {
  ensure(typeof value === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)$/.test(value) && Number.isFinite(Date.parse(value)), 'A timestamp with timezone is required');
  const [year, month, day] = value.slice(0, 10).split('-').map(Number) as [number, number, number];
  ensure(month >= 1 && month <= 12 && day >= 1 && day <= new Date(Date.UTC(year, month, 0)).getUTCDate() && Number(value.slice(11, 13)) < 24, 'Invalid calendar date or clock time');
}
export function assertTime(time: unknown): asserts time is ObservationTime {
  ensure(time !== null && typeof time === 'object', 'Observation time is required');
  const value = time as ObservationTime;
  if (value.kind === 'source-time-unknown') return;
  ensure(value.basis === 'source' || value.basis === 'collection', 'Time basis must be explicit');
  if (value.kind === 'point') assertTimestamp(value.at);
  else {
    ensure(value.kind === 'interval', 'Unsupported time kind');
    assertTimestamp(value.start); assertTimestamp(value.end);
    ensure(Date.parse(value.start) < Date.parse(value.end), 'Intervals must have positive duration');
  }
}
export interface PayloadLimits { readonly maxObservations: number; readonly maxBytes: number }
export function validateCandidate(candidate: unknown, limits: PayloadLimits): CandidateBatch {
  const encoded = canonical(candidate);
  ensure(Buffer.byteLength(encoded) <= limits.maxBytes, 'Candidate exceeds maxBytes');
  const value = JSON.parse(encoded) as CandidateBatch;
  ensure(value !== null && typeof value === 'object', 'A candidate batch is required');
  ensure(typeof value.key === 'string' && value.key.trim(), 'Batch key is required');
  ensure(['complete', 'partial', 'failed'].includes(value.outcome), 'Invalid batch outcome');
  ensure(value.coverage && ['complete', 'partial', 'unknown'].includes(value.coverage.status), 'Coverage is required');
  if (value.details !== undefined) ensure(value.details !== null && typeof value.details === 'object' && !Array.isArray(value.details), 'Details must be a JSON object');
  assertTime(value.coverage.time);
  ensure(Array.isArray(value.observations) && value.observations.length <= limits.maxObservations, 'Candidate exceeds maxObservations or lacks observations');
  const keys = new Set<string>();
  for (const observation of value.observations) {
    ensure(observation && typeof observation.key === 'string' && observation.key.trim(), 'Observation key is required');
    ensure(!keys.has(observation.key), 'Duplicate observation key'); keys.add(observation.key);
    ensure(observation.subject !== null && typeof observation.subject === 'object' && !Array.isArray(observation.subject), 'Subject must be an object');
    assertRef(observation.schemaRef); assertTime(observation.time);
    if (observation.collectedAt !== undefined) assertTimestamp(observation.collectedAt);
    ensure(Object.hasOwn(observation, 'payload') !== Object.hasOwn(observation, 'externalArtifactRef'), 'Supply exactly one payload or external artifact');
    if (Object.hasOwn(observation, 'externalArtifactRef')) {
      const artifact = observation.externalArtifactRef;
      ensure(artifact !== null && typeof artifact === 'object' && !Array.isArray(artifact), 'External artifact must be an object');
      ensure(typeof artifact.uri === 'string' && artifact.uri.length && /^[a-f0-9]{64}$/.test(artifact.sha256) && Number.isSafeInteger(artifact.sizeBytes) && artifact.sizeBytes >= 0, 'Invalid external artifact');
    }
    if (observation.inputRefs !== undefined) {
      ensure(Array.isArray(observation.inputRefs) && observation.inputRefs.length > 0, 'Input support cannot be empty');
      for (const ref of observation.inputRefs) ensure((ref.kind === 'batch' || ref.kind === 'observation') && typeof ref.id === 'string' && ref.id.length, 'Invalid input reference');
    }
  }
  if (value.errors !== undefined) {
    ensure(Array.isArray(value.errors), 'Errors must be an array');
    for (const error of value.errors) ensure(typeof error.code === 'string' && error.code.length && typeof error.message === 'string', 'Invalid error');
  }
  return value;
}
