import type { RuntimeSnapshot } from 'footprintjs';
import type { TraceOutcome } from '../contracts.js';
import { validateWindowOptions, type WindowFlowOptions } from './options.js';
import { withDeadline } from './deadline.js';
import { executeWindow } from './execution.js';
import { serializeRecord } from './record.js';
import { persistTrace } from './persistence.js';

export interface WindowFlowResult {
  readonly record: RuntimeSnapshot;
  readonly trace: TraceOutcome;
}

/**
 * Execute one opt-in collection-window chart with native read/write and decision evidence.
 * This records the chart's actual inputs, not physical causality or cross-window identity.
 * Domain attention and required-evidence declarations remain the adapter's responsibility.
 */
export async function runWindowFlow(options: WindowFlowOptions): Promise<WindowFlowResult> {
  const validated = validateWindowOptions(options);
  return withDeadline(validated, async deadline => {
    const executor = await executeWindow(validated, deadline);
    const record = serializeRecord(executor, validated.maxRecordBytes);
    return { record, trace: await persistTrace(record, validated, deadline) };
  });
}
