import type { JsonObject, ObservationRef, TimeBasis, VersionedRef } from '../contracts.js';
import { assertTimestamp, canonical, ensure } from '../validation.js';
export interface NumericMetricDefinition { readonly sourceRef: VersionedRef; readonly metricRef: VersionedRef; readonly unit: string;
  readonly grain: string; readonly weighting: 'equal-per-sample'; readonly clock: TimeBasis; readonly dimensions: JsonObject;
  readonly populationRef: VersionedRef; readonly retentionPolicyRef: VersionedRef; readonly gtThresholds: readonly number[] }
export type NumericSample = { readonly inputRef: ObservationRef; readonly at: string } &
  ({ readonly status: 'valid'; readonly value: number } | { readonly status: 'missing' | 'invalid' });
export interface NumericAnchor { readonly value: number; readonly at: string; readonly inputRef: ObservationRef }
export interface NumericSupport { readonly inputRef: ObservationRef; readonly at: string; readonly status: 'valid' | 'missing' | 'invalid' }
export interface NumericStatistics { readonly version: 1; readonly methodRef: VersionedRef; readonly metric: NumericMetricDefinition;
  readonly count: number; readonly missingCount: number; readonly invalidCount: number; readonly mean: number | null; readonly M2: number;
  readonly sampleVariance: number | null; readonly min: NumericAnchor | null; readonly max: NumericAnchor | null;
  readonly first: NumericAnchor | null; readonly last: NumericAnchor | null;
  readonly thresholds: readonly { readonly threshold: number; readonly eligibleCount: number; readonly count: number }[];
  readonly support: readonly NumericSupport[] }
export const NUMERIC_STATISTICS_REF = Object.freeze({ id: 'datapipeline.numeric-sample-statistics', version: '1' });
export const MAX_NUMERIC_SUPPORT = 4096;
const MAX_BYTES = 1024 * 1024;
// This is a rejection threshold for a materially lost intermediate, not a
// guarantee of arbitrary-precision arithmetic or a bound on every final value.
const PRECISION_LOSS_FRACTION = 1e-5;
type Bag = Record<string, unknown>;
export function numericDifference(left: number, right: number): number {
  const delta = left - right;
  ensure(Number.isFinite(delta), 'Numeric subtraction overflow');
  const recovered = delta + right;
  const recoveredRight = left - delta;
  ensure(Math.abs(recovered - left) <= Math.abs(left) * PRECISION_LOSS_FRACTION
    && Math.abs(recoveredRight - right) <= Math.abs(right) * PRECISION_LOSS_FRACTION,
    'Numeric precision loss in subtraction; use a metric representation with a smaller offset');
  return delta;
}
function meanStep(mean: number, step: number): number {
  const next = mean + step;
  ensure(Number.isFinite(next), 'Numeric mean overflow');
  ensure(Math.abs((next - mean) - step) <= Math.abs(step) * PRECISION_LOSS_FRACTION,
    'Numeric precision loss in mean update; use a metric representation with a smaller offset');
  return next;
}

/** Bound traversal before canonical JSON validation; no accessors are executed. */
export function numericJson<T>(value: T, maximumBytes = MAX_BYTES): T {
  let nodes = 0;
  function visit(item: unknown, depth: number): void {
    ensure(++nodes <= 50_000 && depth <= 16, 'Numeric JSON exceeds its bounded depth or size');
    if (typeof item === 'string') ensure(Buffer.byteLength(item) <= 4096, 'Numeric JSON string exceeds its size limit');
    if (item === null || typeof item !== 'object') return;
    ensure(Array.isArray(item) ? item.length <= MAX_NUMERIC_SUPPORT : Object.keys(item).length <= 64, 'Numeric JSON exceeds its bounded collection size');
    for (const key of Object.keys(item)) {
      const property = Object.getOwnPropertyDescriptor(item, key)!;
      ensure('value' in property, 'Numeric JSON accessors are unsupported'); visit(property.value, depth + 1);
    }
  }
  visit(value, 0);
  const encoded = canonical(value);
  ensure(Buffer.byteLength(encoded) <= maximumBytes, 'Numeric JSON exceeds its byte size limit');
  return JSON.parse(encoded) as T;
}
function bag(value: unknown, keys: readonly string[], label: string): asserts value is Bag {
  ensure(value !== null && typeof value === 'object' && !Array.isArray(value), `${label} must be an object`);
  ensure(Object.keys(value).every(key => keys.includes(key)), `${label} has unsupported fields`);
}
function text(value: unknown, maximum = 256): asserts value is string {
  ensure(typeof value === 'string' && value.trim().length > 0 && Buffer.byteLength(value) <= maximum, 'Numeric identity text exceeds its bounds or is missing');
}
function ref(value: unknown): asserts value is VersionedRef {
  bag(value, ['id', 'version'], 'Numeric versioned reference'); text(value.id); text(value.version);
}
function inputRef(value: unknown): asserts value is ObservationRef {
  bag(value, ['kind', 'id'], 'Numeric observation reference'); ensure(value.kind === 'observation', 'Numeric samples need exact observation references'); text(value.id, 1024);
}
export function numericInstant(value: unknown): string {
  text(value, 64); assertTimestamp(value);
  ensure(!/\.\d{4,}/.test(value), 'Numeric timestamps require millisecond-or-coarser precision');
  return new Date(value).toISOString();
}
export function checkedMetric(input: NumericMetricDefinition): NumericMetricDefinition {
  const value = numericJson(input, 32 * 1024);
  bag(value, ['sourceRef', 'metricRef', 'unit', 'grain', 'weighting', 'clock', 'dimensions', 'populationRef', 'retentionPolicyRef', 'gtThresholds'], 'Numeric metric');
  ref(value.sourceRef); ref(value.metricRef); ref(value.populationRef); ref(value.retentionPolicyRef);
  text(value.unit); text(value.grain); ensure(value.weighting === 'equal-per-sample', 'Numeric weighting must be equal-per-sample');
  ensure(value.clock === 'source' || value.clock === 'collection', 'Numeric clock must be source or collection');
  ensure(value.dimensions !== null && typeof value.dimensions === 'object' && !Array.isArray(value.dimensions), 'Numeric dimensions must be a JSON object');
  numericJson(value.dimensions, 16 * 1024);
  ensure(Array.isArray(value.gtThresholds) && value.gtThresholds.length <= 32
    && value.gtThresholds.every(item => typeof item === 'number' && Number.isFinite(item))
    && new Set(value.gtThresholds).size === value.gtThresholds.length, 'Numeric gt thresholds must be unique finite numbers within the32-threshold limit');
  return { ...value, gtThresholds: [...value.gtThresholds].sort((a, b) => a - b) };
}
type LocatedRef = Pick<NumericSupport, 'inputRef' | 'at'>;
const supportKey = (value: LocatedRef) => value.inputRef.id;
const chronological = (a: LocatedRef, b: LocatedRef) => a.at.localeCompare(b.at) || supportKey(a).localeCompare(supportKey(b));
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const count = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
function support(value: unknown): NumericSupport {
  bag(value, ['inputRef', 'at', 'status'], 'Numeric support'); inputRef(value.inputRef);
  ensure(['valid', 'missing', 'invalid'].includes(String(value.status)), 'Numeric support status is invalid');
  return { inputRef: { ...value.inputRef }, at: numericInstant(value.at), status: value.status as NumericSupport['status'] };
}
function anchor(value: unknown, valid: readonly NumericSupport[], label: string): NumericAnchor {
  bag(value, ['inputRef', 'at', 'value'], `Numeric ${label} anchor`); inputRef(value.inputRef);
  const at = numericInstant(value.at), target = value.inputRef; ensure(finite(value.value), 'Numeric anchor values must be finite');
  ensure(valid.some(item => item.inputRef.id === target.id && item.at === at), 'Numeric anchor is outside valid input support');
  return { value: value.value, at, inputRef: { ...value.inputRef } };
}
/** Validate persisted sufficient statistics; authenticity still belongs to their evidence/store. */
export function checkedStatistics(input: NumericStatistics): NumericStatistics {
  const value = numericJson(input);
  bag(value, ['version', 'methodRef', 'metric', 'count', 'missingCount', 'invalidCount', 'mean', 'M2', 'sampleVariance', 'min', 'max', 'first', 'last', 'thresholds', 'support'], 'Numeric statistics');
  ensure(value.version === 1 && canonical(value.methodRef) === canonical(NUMERIC_STATISTICS_REF), 'Numeric statistics method revision is unsupported');
  const metric = checkedMetric(value.metric);
  ensure(Array.isArray(value.support) && value.support.length <= MAX_NUMERIC_SUPPORT, 'Numeric support exceeds4096');
  const refs = value.support.map(support).sort(chronological), seen = new Set<string>();
  for (const item of refs) { ensure(!seen.has(supportKey(item)), 'Numeric support contains duplicate or overlapping observations'); seen.add(supportKey(item)); }
  const valid = refs.filter(item => item.status === 'valid');
  ensure(count(value.count) && count(value.missingCount) && count(value.invalidCount)
    && value.count === valid.length && value.missingCount === refs.filter(item => item.status === 'missing').length
    && value.invalidCount === refs.filter(item => item.status === 'invalid').length, 'Numeric counts disagree with exact support');
  ensure(finite(value.M2) && value.M2 >= 0, 'Numeric M2 must be finite and nonnegative');
  const n = value.count, variance = n > 1 ? value.M2 / (n - 1) : null;
  ensure(value.sampleVariance === variance, 'Numeric sample variance disagrees with count/M2');
  let min: NumericAnchor | null = null, max: NumericAnchor | null = null, first: NumericAnchor | null = null, last: NumericAnchor | null = null;
  if (n === 0) ensure(value.mean === null && value.M2 === 0 && ['min', 'max', 'first', 'last'].every(key => value[key as keyof NumericStatistics] === null), 'Empty numeric statistics must contain no value or anchors');
  else {
    ensure(finite(value.mean), 'Numeric mean must be finite');
    min = anchor(value.min, valid, 'minimum'); max = anchor(value.max, valid, 'maximum');
    first = anchor(value.first, valid, 'first'); last = anchor(value.last, valid, 'last');
    ensure(min.value <= value.mean && value.mean <= max.value && first.value >= min.value && first.value <= max.value
      && last.value >= min.value && last.value <= max.value, 'Numeric mean/anchors disagree with extrema');
    ensure(first.inputRef.id === valid[0]!.inputRef.id && last.inputRef.id === valid.at(-1)!.inputRef.id, 'Numeric first/last anchors disagree with chronological support');
    if (n === 1 || min.value === max.value) ensure(value.M2 === 0 && value.mean === min.value && max.value === min.value, 'Constant numeric observations require zero M2');
    else ensure(value.M2 > 0, 'Distinct numeric extrema require nonzero M2');
  }
  ensure(Array.isArray(value.thresholds) && value.thresholds.length === metric.gtThresholds.length, 'Numeric threshold state is incomplete');
  const thresholds = value.thresholds.map((item, index) => {
    bag(item, ['threshold', 'eligibleCount', 'count'], 'Numeric threshold counter');
    ensure(finite(item.threshold) && item.threshold === metric.gtThresholds[index] && item.eligibleCount === n && count(item.count) && item.count <= n, 'Numeric threshold count/eligible denominator is invalid');
    if (min && max) {
      if (min.value > item.threshold) ensure(item.count === n, 'Numeric threshold count contradicts minimum');
      if (max.value <= item.threshold) ensure(item.count === 0, 'Numeric threshold count contradicts maximum');
      if (min.value <= item.threshold && max.value > item.threshold) ensure(item.count > 0 && item.count < n, 'Numeric strict threshold count contradicts extrema');
    }
    return { threshold: item.threshold, eligibleCount: n, count: item.count };
  });
  ensure(thresholds.every((item, index) => index === 0 || thresholds[index - 1]!.count >= item.count), 'Numeric threshold counters must be monotonic');
  return { version: 1, methodRef: { ...NUMERIC_STATISTICS_REF }, metric, count: n, missingCount: value.missingCount,
    invalidCount: value.invalidCount, mean: value.mean, M2: value.M2, sampleVariance: variance, min, max, first, last, thresholds, support: refs };
}
function earlier(a: NumericAnchor, b: NumericAnchor): NumericAnchor { return chronological(a, b) <= 0 ? a : b; }
function later(a: NumericAnchor, b: NumericAnchor): NumericAnchor { return chronological(a, b) >= 0 ? a : b; }
function empty(metric: NumericMetricDefinition): NumericStatistics {
  return { version: 1, methodRef: { ...NUMERIC_STATISTICS_REF }, metric, count: 0, missingCount: 0, invalidCount: 0,
    mean: null, M2: 0, sampleVariance: null, min: null, max: null, first: null, last: null,
    thresholds: metric.gtThresholds.map(threshold => ({ threshold, eligibleCount: 0, count: 0 })), support: [] };
}

/** Values describe this metric's samples only; no operation weighting or percentiles are inferred. */
export function summarizeNumericSamples(metric: NumericMetricDefinition, samples: readonly NumericSample[]): NumericStatistics {
  const definition = checkedMetric(metric), input = numericJson(samples);
  ensure(Array.isArray(input) && input.length <= MAX_NUMERIC_SUPPORT, 'Numeric samples exceed4096');
  const values = input.map(item => {
    bag(item, item.status === 'valid' ? ['inputRef', 'at', 'status', 'value'] : ['inputRef', 'at', 'status'], 'Numeric sample');
    const checked = support({ inputRef: item.inputRef, at: item.at, status: item.status });
    if (checked.status === 'valid') ensure(finite((item as { value: number }).value), 'Numeric sample values must be finite');
    return { ...checked, ...(checked.status === 'valid' ? { value: (item as { value: number }).value } : {}) };
  }).sort(chronological);
  const seen = new Set<string>(); let n = 0, mean = 0, M2 = 0;
  let min: NumericAnchor | null = null, max: NumericAnchor | null = null, first: NumericAnchor | null = null, last: NumericAnchor | null = null;
  const thresholds = definition.gtThresholds.map(threshold => ({ threshold, eligibleCount: 0, count: 0 }));
  for (const item of values) {
    ensure(!seen.has(supportKey(item)), 'Numeric samples contain duplicate or overlapping observations'); seen.add(supportKey(item));
    if (item.status !== 'valid') continue;
    const value = item.value!, delta = numericDifference(value, mean); n++;
    // The first value avoids value^2 or an unsafe sum of large constants.
    if (n === 1) mean = value;
    else {
      const step = delta / n, increment = delta * (delta * ((n - 1) / n));
      ensure(delta === 0 || step !== 0 && increment !== 0, 'Numeric arithmetic underflow would lose a nonzero variation');
      mean = meanStep(mean, step); M2 += increment;
    }
    ensure(Number.isFinite(mean) && Number.isFinite(M2) && M2 >= 0, 'Numeric arithmetic overflow or nonfinite M2');
    const current = { value, at: item.at, inputRef: { ...item.inputRef } };
    if (!min || value < min.value) min = current;
    if (!max || value > max.value) max = current;
    first ??= current; last = current;
    for (const counter of thresholds) { counter.eligibleCount++; if (value > counter.threshold) counter.count++; }
  }
  return checkedStatistics({ ...empty(definition), count: n, mean: n ? mean : null, M2,
    sampleVariance: n > 1 ? M2 / (n - 1) : null, min, max, first, last, thresholds,
    missingCount: values.filter(item => item.status === 'missing').length, invalidCount: values.filter(item => item.status === 'invalid').length,
    support: values.map(({ inputRef, at, status }) => ({ inputRef, at, status })) });
}

/** Merge disjoint exact support only; n/mean/M2 cannot safely deduplicate overlap. */
export function mergeNumericStatistics(states: readonly NumericStatistics[]): NumericStatistics {
  ensure(Array.isArray(states) && states.length > 0 && states.length <= MAX_NUMERIC_SUPPORT, 'Numeric merge needs1–4096states');
  const inputs = states.map(checkedStatistics).sort((a, b) => a.support[0]?.at.localeCompare(b.support[0]?.at ?? '') ?? -1);
  const metric = inputs[0]!.metric; let result = empty(metric); const seen = new Set<string>();
  for (const state of inputs) {
    ensure(canonical(state.metric) === canonical(metric), 'Numeric metric identities are incompatible');
    for (const item of state.support) { ensure(!seen.has(supportKey(item)), 'Numeric merge has duplicate or overlapping support'); seen.add(supportKey(item)); }
    ensure(seen.size <= MAX_NUMERIC_SUPPORT, 'Numeric merged support exceeds4096');
    const n = result.count + state.count; let mean = result.mean, M2 = result.M2;
    if (state.count && result.count) {
      const delta = numericDifference(state.mean!, result.mean!);
      mean = meanStep(result.mean!, delta * (state.count / n));
      const between = delta * (delta * (result.count * (state.count / n)));
      ensure(delta === 0 || between !== 0, 'Numeric merge arithmetic underflow would lose a nonzero variation');
      M2 += state.M2 + between;
    } else if (state.count) { mean = state.mean; M2 = state.M2; }
    ensure((mean === null || Number.isFinite(mean)) && Number.isFinite(M2), 'Numeric merge arithmetic overflow');
    const min = !result.min ? state.min : !state.min ? result.min : state.min.value < result.min.value ? state.min : state.min.value === result.min.value ? earlier(result.min, state.min) : result.min;
    const max = !result.max ? state.max : !state.max ? result.max : state.max.value > result.max.value ? state.max : state.max.value === result.max.value ? earlier(result.max, state.max) : result.max;
    result = { ...result, count: n, mean, M2, sampleVariance: n > 1 ? M2 / (n - 1) : null, min, max,
      first: !result.first ? state.first : !state.first ? result.first : earlier(result.first, state.first),
      last: !result.last ? state.last : !state.last ? result.last : later(result.last, state.last),
      missingCount: result.missingCount + state.missingCount, invalidCount: result.invalidCount + state.invalidCount,
      thresholds: result.thresholds.map((item, index) => ({ ...item, eligibleCount: item.eligibleCount + state.thresholds[index]!.eligibleCount,
        count: item.count + state.thresholds[index]!.count })), support: [...result.support, ...state.support] };
  }
  return checkedStatistics(result);
}
