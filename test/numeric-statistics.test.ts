import { describe, expect, it } from 'vitest';
import { mergeNumericStatistics, summarizeNumericSamples, type NumericMetricDefinition, type NumericSample } from '../src/statistics/numericStatistics.js';

export const metric: NumericMetricDefinition = {
  sourceRef: { id: 'synthetic-gauge-source', version: '1' }, metricRef: { id: 'synthetic-sample-value', version: '1' },
  unit: 'example-unit', grain: 'retained-observation', weighting: 'equal-per-sample', clock: 'collection',
  dimensions: { entity: 'synthetic-a' }, populationRef: { id: 'selected-population', version: '1' },
  retentionPolicyRef: { id: 'selection-policy', version: '1' }, gtThresholds: [0, 3, 8],
};
export function samples(values: readonly number[], prefix = 'sample', start = 0): NumericSample[] {
  return values.map((value, index) => ({ inputRef: { kind: 'observation', id: `${prefix}-${index}` },
    at: new Date(Date.UTC(2026, 9, 18, 0, start + index)).toISOString(), status: 'valid', value }));
}

describe('optional mergeable numeric statistics', () => {
  it('matches an independent known sample oracle including strict threshold equality and evidence anchors', () => {
    // Independent oracle: values [2,4,4,4,5,5,7,9], sum40, mean5,
    // squared deviations [9,1,1,1,0,0,4,16] sum32; sample variance32/7.
    const input = samples([2, 4, 4, 4, 5, 5, 7, 9]);
    const state = summarizeNumericSamples(metric, input);
    expect(state.count).toBe(8); expect(state.mean).toBe(5); expect(state.M2).toBeCloseTo(32, 12);
    expect(state.sampleVariance).toBeCloseTo(32 / 7, 12);
    expect(state.thresholds).toEqual([{ threshold: 0, eligibleCount: 8, count: 8 },
      { threshold: 3, eligibleCount: 8, count: 7 }, { threshold: 8, eligibleCount: 8, count: 1 }]);
    expect(state.min).toEqual({ value: 2, at: input[0]!.at, inputRef: input[0]!.inputRef });
    expect(state.max?.inputRef).toEqual(input[7]!.inputRef); expect(state.first?.value).toBe(2); expect(state.last?.value).toBe(9);
    expect(state.metric.clock).toBe('collection'); expect(state.metric.grain).toBe('retained-observation');
    const equal = summarizeNumericSamples(metric, samples([0, 3, 8]));
    expect(equal.thresholds.map(item => item.count)).toEqual([2, 1, 0]);
  });

  it('merges unequal partitions by sample count, retaining variance and metric-specific extrema refs', () => {
    const input = samples([2, 4, 4, 4, 5, 5, 7, 9]);
    const states = [input.slice(0, 1), input.slice(1, 3), input.slice(3)].map(part => summarizeNumericSamples(metric, part));
    const merged = mergeNumericStatistics(states.reverse());
    expect(merged.count).toBe(8); expect(merged.mean).toBeCloseTo(5, 12); expect(merged.M2).toBeCloseTo(32, 12);
    expect(merged.sampleVariance).toBeCloseTo(32 / 7, 12);
    expect(merged.thresholds.map(item => item.count)).toEqual([8, 7, 1]);
    expect(merged.first?.inputRef).toEqual(input[0]!.inputRef); expect(merged.last?.inputRef).toEqual(input[7]!.inputRef);
    expect(merged.min?.inputRef).toEqual(input[0]!.inputRef); expect(merged.max?.inputRef).toEqual(input[7]!.inputRef);
    expect(merged.support).toHaveLength(8);
  });

  it('keeps missing/invalid observations explicit, excluding them from eligible denominators and first/last valid values', () => {
    const input: NumericSample[] = [...samples([0, -2]),
      { inputRef: { kind: 'observation', id: 'missing' }, at: '2026-10-18T00:02:00Z', status: 'missing' },
      { inputRef: { kind: 'observation', id: 'invalid' }, at: '2026-10-18T00:03:00Z', status: 'invalid' }];
    const state = summarizeNumericSamples(metric, input);
    expect(state.count).toBe(2); expect(state.missingCount).toBe(1); expect(state.invalidCount).toBe(1);
    expect(state.mean).toBe(-1); expect(state.M2).toBe(2); expect(state.sampleVariance).toBe(2);
    expect(state.thresholds.map(item => item.eligibleCount)).toEqual([2, 2, 2]);
    expect(state.last?.inputRef).toEqual(input[1]!.inputRef);
    const empty = summarizeNumericSamples(metric, input.slice(2));
    expect(empty.mean).toBeNull(); expect(empty.M2).toBe(0); expect(empty.sampleVariance).toBeNull();
    expect(empty.min).toBeNull(); expect(empty.first).toBeNull();
  });

  it('refuses duplicate/overlapping support even if the duplicated partition contents agree', () => {
    const input = samples([2, 4, 8]);
    expect(() => summarizeNumericSamples(metric, [input[0]!, input[0]!])).toThrow(/overlap|duplicate/i);
    const left = summarizeNumericSamples(metric, input.slice(0, 2));
    const right = summarizeNumericSamples(metric, input.slice(1));
    expect(() => mergeNumericStatistics([left, right])).toThrow(/overlap|duplicate/i);
    expect(() => mergeNumericStatistics([left, left])).toThrow(/overlap|duplicate/i);
  });

  it('refuses metric identity, source revision, units, grain, weighting, clock, dimension and population incompatibility', () => {
    const base = summarizeNumericSamples(metric, samples([1], 'left'));
    const changes = [{ sourceRef: { ...metric.sourceRef, version: '2' } }, { metricRef: { ...metric.metricRef, version: '2' } },
      { unit: 'other-unit' }, { grain: 'other-grain' }, { clock: 'source' }, { dimensions: { entity: 'different' } },
      { populationRef: { ...metric.populationRef, version: '2' } },
      { retentionPolicyRef: { ...metric.retentionPolicyRef, version: '2' } }, { gtThresholds: [1] }];
    for (const change of changes) {
      const other = summarizeNumericSamples({ ...metric, ...change } as NumericMetricDefinition, samples([2], 'right'));
      expect(() => mergeNumericStatistics([base, other])).toThrow(/incompatible/i);
    }
    expect(() => summarizeNumericSamples({ ...metric, weighting: 'operation-weighted' } as never, samples([1]))).toThrow(/weight/i);
  });

  it('rejects forged summary state and nonfinite, overflowing or oversized JSON without mutating inputs', () => {
    const state = summarizeNumericSamples(metric, samples([1, 2, 3]));
    const original = JSON.stringify(state);
    for (const change of [{ count: 4 }, { M2: -1 }, { M2: 0, sampleVariance: 0 }, { mean: Infinity }, { mean: 9 }, { sampleVariance: 9 },
      { methodRef: { ...state.methodRef, version: 'unknown' } },
      { thresholds: state.thresholds.map(item => ({ ...item, eligibleCount: 9 })) },
      { max: { ...state.max!, inputRef: { kind: 'observation' as const, id: 'invented' } } }]) {
      expect(() => mergeNumericStatistics([{ ...state, ...change }])).toThrow();
    }
    for (const value of [NaN, Infinity, -Infinity]) expect(() => summarizeNumericSamples(metric, samples([value]))).toThrow(/finite/i);
    expect(() => summarizeNumericSamples(metric, samples([-1e308, 1e308]))).toThrow(/overflow|finite/i);
    expect(() => summarizeNumericSamples(metric, samples(Array.from({ length: 4097 }, () => 1)))).toThrow(/limit|bound|4096/i);
    expect(() => summarizeNumericSamples({ ...metric, dimensions: { giant: 'x'.repeat(20000) } }, samples([1]))).toThrow(/limit|bound|size/i);
    expect(JSON.stringify(state)).toBe(original);
  });

  it('handles a full24h of288polls, deterministic ties and a large constant without unsafe sums', () => {
    const input = samples(Array.from({ length: 288 }, (_, index) => index % 2 ? 3 : 1));
    const state = mergeNumericStatistics(input.map(item => summarizeNumericSamples(metric, [item])));
    expect(state.count).toBe(288); expect(state.mean).toBeCloseTo(2, 12); expect(state.M2).toBeCloseTo(288, 10);
    expect(state.min?.inputRef).toEqual(input[0]!.inputRef); expect(state.max?.inputRef).toEqual(input[1]!.inputRef);
    const same = summarizeNumericSamples(metric, samples([1e308, 1e308]));
    expect(same.mean).toBe(1e308); expect(same.M2).toBe(0); expect(same.sampleVariance).toBe(0);
  });

  it('preserves a representable large cross-partition M2 and refuses unrepresentable nonzero variance', () => {
    const input = samples([0, 1.5e154]);
    const merged = mergeNumericStatistics(input.map(item => summarizeNumericSamples(metric, [item])));
    expect(merged.mean).toBe(7.5e153); expect(merged.M2).toBeCloseTo(1.125e308, -295);
    expect(merged.sampleVariance).toBe(merged.M2);
    expect(() => summarizeNumericSamples(metric, samples([0, 1e-200]))).toThrow(/underflow/i);
    const small = samples([0, 1e-200], 'small');
    expect(() => mergeNumericStatistics(small.map(item => summarizeNumericSamples(metric, [item])))).toThrow(/underflow/i);
  });

  it('refuses material floating-point precision loss rather than returning a wrong finite mean or variance', () => {
    // Independent exact oracles: [1e16,1e16+2,1e16+4] has mean1e16+2,
    // M2=8, variance4. [-1e16,1,1e16] has exact mean1/3.
    for (const values of [[1e16, 1e16 + 2, 1e16 + 4], [-1e16, 1, 1e16]]) {
      const input = samples(values);
      expect(() => summarizeNumericSamples(metric, input)).toThrow(/precision/i);
      expect(() => mergeNumericStatistics(input.map(item => summarizeNumericSamples(metric, [item])))).toThrow(/precision/i);
    }
    const reordered = summarizeNumericSamples(metric, samples([-1e16, 1e16, 1]));
    expect(reordered.mean).toBe(1 / 3);
    const common = samples(Array.from({ length: 288 }, (_, index) => index % 2 ? 137451 : 137450));
    const state = mergeNumericStatistics(common.map(item => summarizeNumericSamples(metric, [item])));
    expect(state.mean).toBeCloseTo(137450.5, 8); expect(state.M2).toBeCloseTo(72, 8);
  });
});
