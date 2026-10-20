import { createHash } from 'node:crypto';
import type { CandidateBatch, JsonObject } from '../contracts.js';
import type { Transform } from '../ports.js';
import { canonical, ensure } from '../validation.js';
import { checkedMetric, numericInstant } from '../statistics/numericStatistics.js';
import { runWindowFlow } from '../window.js';
import { admitWindow } from './admission.js';
import { evidenceChart } from './chart.js';
import { RunWindowEvidenceError, type RunWindowEvidenceOptions } from './contracts.js';

/** An explicitly registered transform, not a timer, collector or retention service. */
export function createRunWindowEvidenceTransform(options: RunWindowEvidenceOptions): Transform | undefined {
  if (options.enabled !== true) return undefined;
  try {
    const metric = checkedMetric(options.metric), anchorAt = numericInstant(options.anchorAt);
    ensure(metric.clock === 'collection' && Number.isSafeInteger(options.intervalMs) && options.intervalMs > 0, 'Schedule interval and clock');
    const admitted = { ...options, metric, anchorAt };
    const digest = createHash('sha256').update(canonical({ metric, anchorAt, intervalMs: options.intervalMs })).digest('hex');
    const ref = { id: `datapipeline.run-window:${digest}`, version: '1' };
    return { ref, async apply(inputs, request, context): Promise<CandidateBatch> {
      try {
        context.signal.throwIfAborted();
        ensure(canonical(request.methodRef) === canonical(ref), 'Method revision');
        const { slots, knowledgeCutoff } = admitWindow(inputs, request, admitted);
        const windowRef = 'run-window:' + createHash('sha256').update(canonical({ methodRef: ref, window: request.window, knowledgeCutoff })).digest('hex');
        const result = await runWindowFlow({ operationId: context.operationId, chart: evidenceChart(slots, admitted, windowRef, knowledgeCutoff),
          signal: context.signal, clock: context.now, ...(options.traceStore ? { traceStore: options.traceStore } : {}) });
        const state = result.record.sharedState;
        const gaps = state.gaps as unknown[];
        const payload = JSON.parse(canonical({ statistics: state.statistics, coverage: state.coverage, gaps,
          knowledgeCutoff, windowRef, window: request.window, rawEvidence: 'consult-each-run-receipt', interpretation: 'retained-sample-statistics-not-physical-cause' })) as JsonObject;
        const inputRefs = slots.flatMap(slot => [slot.receipt?.ref, slot.contribution?.ref].filter(ref => ref !== undefined));
        return { key: 'run-window', outcome: gaps.length ? 'partial' : 'complete',
          coverage: { status: gaps.length ? 'partial' : 'unknown', time: { kind: 'interval', ...request.window },
            reason: 'Scheduled receipt coverage does not establish continuous source-operation coverage.' },
          observations: [{ key: 'run-window-summary', schemaRef: ref, subject: metric.dimensions, time: { kind: 'interval', ...request.window },
            payload, ...(inputRefs.length ? { inputRefs } : {}) }],
          details: { nativeTrace: JSON.parse(canonical(result.trace)), knowledgeCutoff, expectedPolls: slots.length } };
      } catch { throw new RunWindowEvidenceError(); }
    } };
  } catch { throw new RunWindowEvidenceError(); }
}
