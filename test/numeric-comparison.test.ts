import { describe, expect, it } from 'vitest';
import { summarizeNumericSamples, type NumericMetricDefinition } from '../src/statistics/numericStatistics.js';
import { compareNumericWindows, type NumericWindow, type NumericComparisonPolicy } from '../src/statistics/numericComparison.js';

const metric: NumericMetricDefinition = { sourceRef: { id: 'synthetic-series', version: '1' }, metricRef: { id: 'sample-value', version: '1' },
  unit: 'example-unit', grain: 'retained-sample', weighting: 'equal-per-sample', clock: 'collection', dimensions: { entity: 'fixed-cohort-a' },
  populationRef: { id: 'cohort-a', version: '1' }, retentionPolicyRef: { id: 'selection-a', version: '1' }, gtThresholds: [] };
const policy: NumericComparisonPolicy = { minimumValidSamples: 2, minimumCoverage: 0.75, minimumAbsoluteChange: 1 };
function window(values: readonly number[], from: number, prefix: string, definition = metric): NumericWindow {
  return { window: { basis: 'collection', start: new Date(Date.UTC(2026, 9, 18, 0, from)).toISOString(),
    end: new Date(Date.UTC(2026, 9, 18, 0, from + 4)).toISOString() },
  coverage: { expectedSlots: 4, observedSlots: values.length },
  statistics: summarizeNumericSamples(definition, values.map((value, index) => ({ status: 'valid', value,
    at: new Date(Date.UTC(2026, 9, 18, 0, from + index)).toISOString(), inputRef: { kind: 'observation', id: `${prefix}-${index}` } }))) };
}

describe('one comparable-window sample-mean change detector', () => {
  it('uses independent known current/baseline means and records policy, method and exact input evidence', () => {
    const baseline = window([2, 2, 4, 4], 0, 'baseline'), current = window([4, 4, 8, 8], 4, 'current');
    const result = compareNumericWindows({ baseline, current, policy });
    expect(result.status).toBe('increased'); expect(result.reasons).toEqual([]);
    expect(result.effect).toEqual({ baselineMean: 3, currentMean: 6, difference: 3, absoluteChange: 3,
      relativeChange: 1, relativeBasis: 'absolute-baseline' });
    expect(result.methodRef).toEqual({ id: 'datapipeline.numeric-window-mean-change', version: '1' });
    expect(result.current.inputRefs).toEqual(current.statistics.support.map(item => item.inputRef));
    expect(result.baseline.inputRefs).toEqual(baseline.statistics.support.map(item => item.inputRef));
    expect(result.current.window).toEqual(current.window); expect(result.policy).toEqual(policy);
    expect(result.interpretation).toBe('observed-sample-mean-change-not-cause');
  });

  it('handles equality at both strict change thresholds, decreases and negative-baseline relative effects', () => {
    const baseline = window([4, 4, 4, 4], 0, 'baseline');
    expect(compareNumericWindows({ baseline, current: window([5, 5, 5, 5], 4, 'a'), policy }).status).toBe('unchanged');
    expect(compareNumericWindows({ baseline, current: window([6, 6, 6, 6], 4, 'b'),
      policy: { ...policy, minimumRelativeChange: 0.5 } }).status).toBe('unchanged');
    expect(compareNumericWindows({ baseline, current: window([2, 2, 2, 2], 4, 'c'), policy }).status).toBe('decreased');
    const negative = compareNumericWindows({ baseline: window([-4, -4, -4, -4], 0, 'negative'), current: window([-2, -2, -2, -2], 4, 'd'), policy });
    expect(negative.status).toBe('increased'); expect(negative.effect?.relativeChange).toBe(0.5);
  });

  it('never invents Infinity/NaN for a zero baseline and distinguishes absolute-only from a required relative criterion', () => {
    const baseline = window([0, 0, 0, 0], 0, 'baseline');
    const same = compareNumericWindows({ baseline, current: window([0, 0, 0, 0], 4, 'same'), policy: { ...policy, minimumRelativeChange: 0.2 } });
    expect(same.status).toBe('unchanged'); expect(same.effect?.relativeChange).toBeNull(); expect(same.effect?.relativeBasis).toBe('zero-baseline');
    const current = window([3, 3, 3, 3], 4, 'current');
    expect(compareNumericWindows({ baseline, current, policy }).status).toBe('increased');
    const required = compareNumericWindows({ baseline, current, policy: { ...policy, minimumRelativeChange: 0.2 } });
    expect(required.status).toBe('insufficient-data'); expect(required.reasons).toContain('zero-baseline-relative-change');
    expect(JSON.stringify(required)).not.toMatch(/Infinity|NaN/);
  });

  it('returns explicit insufficient data for small/partial/unknown windows and known missing/invalid values', () => {
    const baseline = window([2, 2, 2, 2], 0, 'baseline');
    const small = compareNumericWindows({ baseline, current: window([9], 4, 'small'), policy });
    expect(small.status).toBe('insufficient-data'); expect(small.reasons).toContain('current-insufficient-samples');
    expect(small.reasons).toContain('current-insufficient-coverage');
    const empty = window([], 4, 'empty');
    const unknown = compareNumericWindows({ baseline, current: { ...empty, coverage: { expectedSlots: 0, observedSlots: 0 } }, policy });
    expect(unknown.status).toBe('insufficient-data'); expect(unknown.reasons).toContain('unknown-coverage');
    const current = window([9, 9, 9, 9], 4, 'current');
    const degraded = summarizeNumericSamples(metric, current.statistics.support.map((item, index) => index < 2
      ? { ...item, status: 'valid' as const, value: 9 } : { ...item, status: index === 2 ? 'missing' as const : 'invalid' as const }));
    const quality = compareNumericWindows({ baseline, current: { ...current, statistics: degraded }, policy });
    expect(quality.status).toBe('insufficient-data'); expect(quality.reasons).toContain('current-insufficient-validity');
    expect(quality.current.missingSamples).toBe(1); expect(quality.current.invalidSamples).toBe(1);
  });

  it('refuses to compare changing populations/selection, clocks, durations, overlapping windows or reused evidence', () => {
    const baseline = window([2, 2, 2, 2], 0, 'baseline');
    for (const definition of [{ ...metric, populationRef: { ...metric.populationRef, version: '2' } },
      { ...metric, retentionPolicyRef: { ...metric.retentionPolicyRef, version: '2' } }, { ...metric, unit: 'other' }]) {
      const result = compareNumericWindows({ baseline, current: window([8, 8, 8, 8], 4, 'new', definition), policy });
      expect(result.status).toBe('insufficient-data'); expect(result.reasons).toContain('incompatible-metric'); expect(result.effect).toBeNull();
    }
    const overlap = compareNumericWindows({ baseline, current: window([8, 8, 8, 8], 2, 'overlap'), policy });
    expect(overlap.status).toBe('insufficient-data'); expect(overlap.reasons).toContain('windows-not-ordered');
    const reused = compareNumericWindows({ baseline, current: window([8, 8, 8, 8], 4, 'baseline'), policy });
    expect(reused.status).toBe('insufficient-data'); expect(reused.reasons).toContain('overlapping-input-support');
    const current = window([8, 8, 8, 8], 4, 'new');
    const longer = { ...current, window: { ...current.window, end: '2026-10-18T00:09:00.000Z' } };
    expect(compareNumericWindows({ baseline, current: longer, policy }).reasons).toContain('unequal-window-duration');
    expect(() => compareNumericWindows({ baseline, current: { ...current, window: { ...current.window, basis: 'source' } }, policy })).toThrow(/clock|basis/i);
  });

  it('rejects ungrounded support windows, impossible coverage and invalid thresholds instead of silently changing them', () => {
    const baseline = window([2, 2, 2, 2], 0, 'baseline'), current = window([8, 8, 8, 8], 4, 'current');
    for (const policy of [{ minimumValidSamples: 0, minimumCoverage: 0.8, minimumAbsoluteChange: 1 },
      { minimumValidSamples: 2, minimumCoverage: 1.1, minimumAbsoluteChange: 1 },
      { minimumValidSamples: 2, minimumCoverage: 0.8, minimumAbsoluteChange: Infinity }])
      expect(() => compareNumericWindows({ baseline, current, policy })).toThrow();
    expect(() => compareNumericWindows({ baseline, current: { ...current, coverage: { expectedSlots: 3, observedSlots: 4 } }, policy })).toThrow(/coverage|slot/i);
    expect(() => compareNumericWindows({ baseline, current: { ...current,
      window: { ...current.window, start: '2026-10-18T00:05:00.000Z' } }, policy })).toThrow(/support|window/i);
  });

  it('does not classify a strict change using a materially rounded cancellation difference', () => {
    // Exact difference1e16+1 cannot be represented; rounded1e16 would
    // incorrectly compare equal to this strict threshold.
    for (const [prior, next] of [[-1e16, 1], [-1, 1e16], [1, -1e16]]) {
      const baseline = window([prior!, prior!, prior!, prior!], 0, 'baseline');
      const current = window([next!, next!, next!, next!], 4, 'current');
      const result = compareNumericWindows({ baseline, current, policy: { ...policy, minimumAbsoluteChange: 1e16 } });
      expect(result.status).toBe('insufficient-data'); expect(result.reasons).toContain('arithmetic-precision-loss');
      expect(result.effect).toBeNull();
    }
  });
});
