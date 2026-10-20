import type { BatchInput, DeriveRequest, Observation, JsonObject } from '../contracts.js';
import { canonical, ensure } from '../validation.js';
import { numericInstant, numericJson } from '../statistics/numericStatistics.js';
import { MAX_RUN_WINDOW_INPUTS, MAX_RUN_WINDOW_SLOTS, RUN_CONTRIBUTION_SCHEMA, RUN_RECEIPT_SCHEMA, type RunSlot, type RunWindowEvidenceOptions } from './contracts.js';

function object(value: unknown, limit: number): JsonObject {
  const checked = numericJson(value, limit);
  ensure(checked !== null && typeof checked === 'object' && !Array.isArray(checked), 'Expected JSON object');
  return checked as JsonObject;
}
const matches = (a: unknown, b: unknown) => canonical(a) === canonical(b);
export function admitWindow(inputs: readonly BatchInput[], request: DeriveRequest, options: RunWindowEvidenceOptions) {
  ensure(request.window.basis === 'collection', 'Collection-clock windows only');
  const start = Date.parse(numericInstant(request.window.start)), end = Date.parse(numericInstant(request.window.end));
  const anchor = Date.parse(options.anchorAt), interval = options.intervalMs;
  ensure(start >= anchor && end > start && (start - anchor) % interval === 0 && (end - start) % interval === 0, 'Window must align to schedule');
  const count = (end - start) / interval;
  ensure(count <= MAX_RUN_WINDOW_SLOTS && inputs.length <= MAX_RUN_WINDOW_SLOTS, 'Partition the window');
  const parameters = numericJson(request.request, 4096);
  ensure(parameters && typeof parameters === 'object' && !Array.isArray(parameters)
    && Object.keys(parameters).length === 1 && 'knowledgeCutoff' in parameters, 'Knowledge cutoff required');
  const cutoff = Date.parse(numericInstant(parameters.knowledgeCutoff));
  ensure(cutoff >= end, 'Cutoff precedes window end');
  const slots: RunSlot[] = Array.from({ length: count }, (_, i) => ({ at: new Date(start + i * interval).toISOString() }));
  const seen = new Set<string>(); let scanned = 0;
  for (const input of inputs) {
    ensure(matches(input.batch.sourceRef, options.metric.sourceRef), 'Source mismatch');
    ensure(input.observations.length + scanned <= MAX_RUN_WINDOW_INPUTS, 'Input scan bound');
    scanned += input.observations.length;
    for (const observation of input.observations) {
      ensure(!seen.has(observation.ref.id) && observation.batchRef.id === input.batch.ref.id
        && input.batch.observationRefs.some(ref => ref.id === observation.ref.id), 'Duplicate or foreign observation');
      seen.add(observation.ref.id);
    }
    // The durable batch clock is when this assessment could first know the data.
    if (Date.parse(numericInstant(input.batch.finishedAt)) > cutoff) continue;
    const receipts = input.observations.filter(item => matches(item.schemaRef, RUN_RECEIPT_SCHEMA));
    ensure(receipts.length === 1, 'Exactly one receipt per supplied run');
    const receipt = receipts[0]!;
    const value = object(receipt.payload, 16 * 1024);
    ensure(value && typeof value === 'object' && !Array.isArray(value), 'Receipt payload');
    ensure(Object.keys(value).every(key => ['slotAt', 'status', 'rawDisposition', 'retentionPolicyRef'].includes(key)), 'Receipt fields');
    const at = numericInstant(value.slotAt), index = (Date.parse(at) - start) / interval;
    ensure(Number.isSafeInteger(index) && index >= 0 && index < count, 'Receipt outside schedule');
    ensure(!slots[index]!.receipt && matches(value.retentionPolicyRef, options.metric.retentionPolicyRef), 'Duplicate slot or policy mismatch');
    ensure(value.status !== 'complete' || (input.batch.outcome !== 'failed' && input.batch.coverage.status !== 'partial'), 'Conflicting completeness');
    ensure(input.batch.outcome !== 'failed' || value.status === 'failed', 'Failed batch status');
    const received = Date.parse(numericInstant(receipt.collectedAt));
    ensure(received >= Date.parse(at) && received < Date.parse(at) + interval, 'Receipt outside its slot');
    ensure(['complete', 'partial', 'failed', 'empty'].includes(String(value.status))
      && ['retained', 'not-retained', 'expired'].includes(String(value.rawDisposition)), 'Receipt status');
    const candidates = input.observations.filter(item => matches(item.schemaRef, RUN_CONTRIBUTION_SCHEMA) && matches(item.subject, options.metric.dimensions));
    ensure(candidates.length <= 1, 'Ambiguous series');
    const contribution: Observation | undefined = candidates[0];
    if (contribution) {
      const sample = object(contribution.payload, 40 * 1024);
      ensure(sample && typeof sample === 'object' && !Array.isArray(sample), 'Contribution payload');
      ensure(Object.keys(sample).every(key => ['receiptKey', 'metric', 'status', 'value'].includes(key))
        && sample.receiptKey === receipt.key && matches(sample.metric, options.metric), 'Contribution identity');
      ensure(['valid', 'missing', 'invalid'].includes(String(sample.status)), 'Contribution status');
      ensure(sample.status === 'valid' ? typeof sample.value === 'number' && Number.isFinite(sample.value) : sample.value === undefined, 'Contribution value');
      const capturedAt = numericInstant(contribution.collectedAt);
      ensure(Date.parse(capturedAt) >= Date.parse(at) && Date.parse(capturedAt) < Date.parse(at) + interval, 'Contribution outside its slot');
      ensure(!['failed', 'empty'].includes(String(value.status)), 'Failed or empty receipt cannot carry a contribution');
    }
    slots[index] = { at, receipt, ...(contribution ? { contribution } : {}) };
  }
  return { slots, knowledgeCutoff: new Date(cutoff).toISOString() };
}
