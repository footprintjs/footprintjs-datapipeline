import type { DeriveWindow, ObservationRef, VersionedRef } from '../contracts.js';
import { canonical, ensure } from '../validation.js';
import { checkedStatistics, MAX_NUMERIC_SUPPORT, numericDifference, numericInstant, numericJson, type NumericMetricDefinition, type NumericStatistics } from './numericStatistics.js';

export interface NumericWindow { readonly window: DeriveWindow; readonly statistics: NumericStatistics;
  readonly coverage: { readonly expectedSlots: number; readonly observedSlots: number } }
export interface NumericComparisonPolicy { readonly minimumValidSamples: number; readonly minimumCoverage: number;
  readonly minimumAbsoluteChange: number; readonly minimumRelativeChange?: number }
export type NumericComparisonReason = 'incompatible-metric' | 'unequal-window-duration' | 'windows-not-ordered'
  | 'overlapping-input-support' | 'unequal-expected-slots' | 'unknown-coverage' | 'current-insufficient-samples'
  | 'baseline-insufficient-samples' | 'current-insufficient-coverage' | 'baseline-insufficient-coverage'
  | 'current-insufficient-validity' | 'baseline-insufficient-validity' | 'zero-baseline-relative-change' | 'arithmetic-overflow' | 'arithmetic-precision-loss';
export interface NumericWindowEvidence { readonly window: DeriveWindow; readonly inputRefs: readonly ObservationRef[];
  readonly validSamples: number; readonly missingSamples: number; readonly invalidSamples: number;
  readonly coverage: NumericWindow['coverage'] & { readonly fraction: number | null; readonly validFraction: number | null } }
export interface NumericWindowComparison { readonly version: 1; readonly methodRef: VersionedRef;
  readonly status: 'increased' | 'decreased' | 'unchanged' | 'insufficient-data'; readonly reasons: readonly NumericComparisonReason[];
  readonly currentMetric: NumericMetricDefinition; readonly baselineMetric: NumericMetricDefinition; readonly policy: NumericComparisonPolicy;
  readonly current: NumericWindowEvidence; readonly baseline: NumericWindowEvidence;
  readonly effect: { readonly currentMean: number; readonly baselineMean: number; readonly difference: number;
    readonly absoluteChange: number; readonly relativeChange: number | null; readonly relativeBasis: 'absolute-baseline' | 'zero-baseline' } | null;
  readonly interpretation: 'observed-sample-mean-change-not-cause' }
export const NUMERIC_WINDOW_COMPARISON_REF = Object.freeze({ id: 'datapipeline.numeric-window-mean-change', version: '1' });
function checkedWindow(input: NumericWindow): { statistics: NumericStatistics; evidence: NumericWindowEvidence } {
  const value = numericJson(input, 2 * 1024 * 1024), statistics = checkedStatistics(value.statistics);
  ensure(Object.keys(value).every(key => ['window', 'statistics', 'coverage'].includes(key)), 'Numeric window has unsupported fields');
  ensure(value.window && Object.keys(value.window).every(key => ['basis', 'start', 'end'].includes(key))
    && value.window.basis === statistics.metric.clock, 'Numeric window basis disagrees with metric clock');
  const window = { basis: value.window.basis, start: numericInstant(value.window.start), end: numericInstant(value.window.end) };
  ensure(Date.parse(window.start) < Date.parse(window.end), 'Numeric comparison window must be positive');
  ensure(statistics.support.every(item => item.at >= window.start && item.at < window.end), 'Numeric input support lies outside its declared window');
  const cov = value.coverage;
  ensure(cov && Object.keys(cov).every(key => ['expectedSlots', 'observedSlots'].includes(key))
    && Number.isSafeInteger(cov.expectedSlots) && cov.expectedSlots >= 0 && cov.expectedSlots <= 20_000
    && Number.isSafeInteger(cov.observedSlots) && cov.observedSlots >= 0 && cov.observedSlots <= cov.expectedSlots
    && cov.observedSlots <= statistics.support.length, 'Numeric coverage slots exceed their bounds or exact observation support');
  return { statistics, evidence: { window, inputRefs: statistics.support.map(item => ({ ...item.inputRef })),
    validSamples: statistics.count, missingSamples: statistics.missingCount, invalidSamples: statistics.invalidCount,
    coverage: { ...cov, fraction: cov.expectedSlots ? cov.observedSlots / cov.expectedSlots : null,
      validFraction: statistics.support.length ? statistics.count / statistics.support.length : null } } };
}

/**
 * One deterministic observation, not a cause or statistical significance test.
 * Coverage is host-declared slot coverage; validity is the recorded sample fraction.
 * Both must reach minimumCoverage. Exact compatible policies/cohorts are required.
 */
export function compareNumericWindows(input: { readonly current: NumericWindow; readonly baseline: NumericWindow;
  readonly policy: NumericComparisonPolicy }): NumericWindowComparison {
  const policy = numericJson(input.policy, 4096);
  ensure(policy && Object.keys(policy).every(key => ['minimumValidSamples', 'minimumCoverage', 'minimumAbsoluteChange', 'minimumRelativeChange'].includes(key)), 'Numeric comparison policy has unsupported fields');
  ensure(Number.isSafeInteger(policy.minimumValidSamples) && policy.minimumValidSamples >= 1 && policy.minimumValidSamples <= MAX_NUMERIC_SUPPORT,
    'Numeric minimumValidSamples must be1–4096');
  ensure(Number.isFinite(policy.minimumCoverage) && policy.minimumCoverage >= 0 && policy.minimumCoverage <= 1, 'Numeric minimumCoverage must be a fraction from0to1');
  ensure(Number.isFinite(policy.minimumAbsoluteChange) && policy.minimumAbsoluteChange >= 0
    && (policy.minimumRelativeChange === undefined || Number.isFinite(policy.minimumRelativeChange) && policy.minimumRelativeChange >= 0),
  'Numeric change thresholds must be finite and nonnegative');
  const current = checkedWindow(input.current), baseline = checkedWindow(input.baseline), reasons: NumericComparisonReason[] = [];
  if (canonical(current.statistics.metric) !== canonical(baseline.statistics.metric)) reasons.push('incompatible-metric');
  const duration = (value: NumericWindowEvidence) => Date.parse(value.window.end) - Date.parse(value.window.start);
  if (duration(current.evidence) !== duration(baseline.evidence)) reasons.push('unequal-window-duration');
  if (Date.parse(baseline.evidence.window.end) > Date.parse(current.evidence.window.start)) reasons.push('windows-not-ordered');
  const prior = new Set(baseline.evidence.inputRefs.map(ref => ref.id));
  if (current.evidence.inputRefs.some(ref => prior.has(ref.id))) reasons.push('overlapping-input-support');
  if (current.evidence.coverage.expectedSlots !== baseline.evidence.coverage.expectedSlots) reasons.push('unequal-expected-slots');
  if (current.evidence.coverage.fraction === null || baseline.evidence.coverage.fraction === null) reasons.push('unknown-coverage');
  for (const [name, value] of [['current', current.evidence], ['baseline', baseline.evidence]] as const) {
    if (value.validSamples < policy.minimumValidSamples) reasons.push(`${name}-insufficient-samples`);
    if (value.coverage.fraction === null || value.coverage.fraction < policy.minimumCoverage) reasons.push(`${name}-insufficient-coverage`);
    if (value.coverage.validFraction === null || value.coverage.validFraction < policy.minimumCoverage) reasons.push(`${name}-insufficient-validity`);
  }
  let effect: NumericWindowComparison['effect'] = null;
  let status: NumericWindowComparison['status'] = 'insufficient-data';
  if (!reasons.length) {
    const currentMean = current.statistics.mean!, baselineMean = baseline.statistics.mean!;
    let difference = Number.NaN;
    try { difference = numericDifference(currentMean, baselineMean); }
    catch (error) { reasons.push(error instanceof Error && /precision/.test(error.message) ? 'arithmetic-precision-loss' : 'arithmetic-overflow'); }
    const relativeChange = baselineMean === 0 ? null : difference / Math.abs(baselineMean);
    if (reasons.length) { /* The unrepresentable difference cannot classify a strict threshold. */ }
    else if (!Number.isFinite(difference) || relativeChange !== null && !Number.isFinite(relativeChange)) reasons.push('arithmetic-overflow');
    else {
      effect = { currentMean, baselineMean, difference, absoluteChange: Math.abs(difference), relativeChange,
        relativeBasis: baselineMean === 0 ? 'zero-baseline' : 'absolute-baseline' };
      if (difference === 0) status = 'unchanged';
      else if (baselineMean === 0 && policy.minimumRelativeChange !== undefined) reasons.push('zero-baseline-relative-change');
      else {
        const passes = Math.abs(difference) > policy.minimumAbsoluteChange
          && (policy.minimumRelativeChange === undefined || Math.abs(relativeChange!) > policy.minimumRelativeChange);
        status = !passes ? 'unchanged' : difference > 0 ? 'increased' : 'decreased';
      }
    }
  }
  return numericJson({ version: 1, methodRef: { ...NUMERIC_WINDOW_COMPARISON_REF }, status, reasons,
    currentMetric: current.statistics.metric, baselineMetric: baseline.statistics.metric, policy,
    current: current.evidence, baseline: baseline.evidence, effect, interpretation: 'observed-sample-mean-change-not-cause' }, 3 * 1024 * 1024);
}
