import { decide, flowChart } from 'footprintjs';
import type { RequirementResult, RequirementReason } from 'foottrace';
import type { JsonObject, JsonValue } from '../contracts.js';
import { summarizeNumericSamples, type NumericSample } from '../statistics/numericStatistics.js';
import type { RunSlot, RunWindowEvidenceOptions } from './contracts.js';

type Scope = Record<string, unknown>;
const payload = (value: unknown) => value as JsonObject;
const optionalPayload = (value: unknown) => value as JsonObject | null;
export function evidenceChart(slots: readonly RunSlot[], options: RunWindowEvidenceOptions, windowRef: string) {
  let chart = flowChart<Scope>('Record declared series and schedule policy', scope => {
    scope.metric = options.metric;
  }, 'run-window-policy');
  for (const [i, slot] of slots.entries()) {
    chart = chart.addFunction('Import saved poll receipt', scope => {
      scope[`receipt_${i}`] = slot.receipt
        ? { ...payload(slot.receipt.payload), inputRef: slot.receipt.ref } : { status: 'not-collected', slotAt: slot.at };
    }, `receipt-${i}`).addFunction('Import saved compact contribution', scope => {
      const receipt = payload(scope[`receipt_${i}`]);
      scope[`input_${i}`] = slot.contribution ? { ...payload(slot.contribution.payload),
        inputRef: slot.contribution.ref, at: slot.contribution.collectedAt, receiptRef: receipt.inputRef } : null;
    }, `input-${i}`).addDeciderFunction('Check poll and contribution completeness', scope => decide(scope, [
      { when: state => payload(state[`receipt_${i}`]).status === 'complete'
        && optionalPayload(state[`input_${i}`])?.status === 'valid', then: `complete-${i}`, label: 'Saved poll and numeric contribution are complete' },
    ], { branch: `gap-${i}`, label: 'Record the missing or incomplete evidence; never substitute zero' }), `coverage-rule-${i}`)
      .addFunctionBranch(`complete-${i}`, 'Record satisfied slot requirement', scope => {
        const receipt = payload(scope[`receipt_${i}`]), sample = optionalPayload(scope[`input_${i}`]);
        scope[`requirement_${i}`] = requirement(i, windowRef, slot.at, receipt, sample, true);
      }).addFunctionBranch(`gap-${i}`, 'Record unresolved slot requirement', scope => {
        const receipt = payload(scope[`receipt_${i}`]), sample = optionalPayload(scope[`input_${i}`]);
        const result = requirement(i, windowRef, slot.at, receipt, sample, false);
        scope[`requirement_${i}`] = result;
        scope[`attention_${i}`] = { kind: 'not-collected', reason: 'RUN_WINDOW_EVIDENCE_GAP', evidence: [result.id] };
      }).end();
  }
  return chart.addFunction('Summarize saved contributions only', scope => {
    const samples: NumericSample[] = [];
    for (let i = 0; i < slots.length; i++) {
      const item = optionalPayload(scope[`input_${i}`]);
      if (item) samples.push({ inputRef: item.inputRef, at: item.at, status: item.status,
        ...(item.status === 'valid' ? { value: item.value } : {}) } as unknown as NumericSample);
    }
    scope.statistics = summarizeNumericSamples(scope.metric as RunWindowEvidenceOptions['metric'], samples);
  }, 'statistics').addFunction('Count scheduled poll gaps independently of numeric values', scope => {
    let receivedPolls = 0, missingPolls = 0, failedPolls = 0, partialPolls = 0, emptyPolls = 0;
    const gaps: RequirementResult[] = [];
    for (let i = 0; i < slots.length; i++) {
      const receipt = payload(scope[`receipt_${i}`]);
      if (receipt.status === 'not-collected') missingPolls++; else receivedPolls++;
      if (receipt.status === 'failed') failedPolls++;
      if (receipt.status === 'partial') partialPolls++;
      if (receipt.status === 'empty') emptyPolls++;
      const result = scope[`requirement_${i}`] as RequirementResult;
      if (result.state === 'unresolved') gaps.push(result);
    }
    scope.coverage = { expectedPolls: slots.length, receivedPolls, missingPolls, failedPolls, partialPolls, emptyPolls, continuous: 'unknown' };
    scope.gaps = gaps;
  }, 'coverage').build();
}
function requirement(i: number, windowRef: string, at: string, receipt: JsonObject, sample: JsonObject | null, complete: boolean): RequirementResult {
  const common = { id: `${windowRef}:slot:${i}`, requirementRef: `run-window-poll:${at}`, findingRef: `${windowRef}:summary`, windowRef,
    affects: [`${windowRef}:completeness`], evidenceRefs: [receipt.inputRef, sample?.inputRef].filter(Boolean).map(ref => (ref as JsonObject).id as string), checks: [], actionRefs: [] };
  if (complete) return { ...common, state: 'satisfied' };
  return { ...common, state: 'unresolved', reason: reasonFor(receipt, sample), detail: `run-window:${String(receipt.status)}` };
}

function reasonFor(receipt: JsonObject, sample: JsonObject | null): RequirementReason {
  if (receipt.status === 'failed' || receipt.status === 'partial') return 'unavailable';
  if (receipt.status === 'empty') return 'no-match';
  if (sample?.status === 'invalid') return 'invalid-data';
  return 'not-collected';
}
