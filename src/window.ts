import {
  FlowChartExecutor, stringifySnapshot,
  type FlowChart, type FlowErrorEvent, type FlowStageEvent, type RecorderSnapshot, type RuntimeSnapshot,
} from 'footprintjs';
import { ControlDepRecorder, inOutRecorder } from 'footprintjs/trace';
import type { ControlDependency } from 'foottrace';
import type { JsonValue, TraceOutcome, TraceRef } from './contracts.js';
import type { TraceStore } from './ports.js';
import { assertTimestamp, canonical } from './validation.js';

const MAX_RECORD_BYTES = 8 * 1024 * 1024;
const MAX_INPUT_BYTES = 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;

export type WindowErrorCode = 'WINDOW_INVALID' | 'WINDOW_ABORTED' | 'WINDOW_FAILED' | 'WINDOW_RECORD_LIMIT';

/** Fixed, non-echoing refusal. Source and storage exception text is never retained. */
export class WindowExecutionError extends Error {
  constructor(readonly code: WindowErrorCode) {
    super({
      WINDOW_INVALID: 'Window execution options are invalid.',
      WINDOW_ABORTED: 'Window execution was cancelled or timed out.',
      WINDOW_FAILED: 'Window execution did not complete.',
      WINDOW_RECORD_LIMIT: 'Window record exceeds the configured size limit.',
    }[code]);
    this.name = 'WindowExecutionError';
  }
}

export interface WindowFlowOptions {
  readonly operationId: string;
  /** Trusted native chart. Use independent top-level resource keys and focused stages. */
  readonly chart: FlowChart;
  /** Compact run arguments; raw source bodies belong in the source repository. */
  readonly input?: JsonValue;
  readonly traceStore?: TraceStore;
  readonly clock?: () => string;
  readonly signal?: AbortSignal;
  /** Bounds async execution and trace acknowledgement together; default 30 seconds. */
  readonly timeoutMs?: number;
  /** May lower the fixed 8 MiB retained-record ceiling. Transport limits remain host-owned. */
  readonly maxRecordBytes?: number;
}

export interface WindowFlowResult {
  readonly record: RuntimeSnapshot;
  readonly trace: TraceOutcome;
}

/** Persist the native recorder's public outputs in the standard recorder bundle. */
class WindowControlRecorder extends ControlDepRecorder {
  private readonly stages = new Set<string>();
  failed = false;

  override onStageExecuted(event: FlowStageEvent): void {
    super.onStageExecuted(event);
    if (event.traversalContext) this.stages.add(event.traversalContext.runtimeStageId);
  }

  override onError(event: FlowErrorEvent): void {
    super.onError(event);
    this.failed = true;
    if (event.traversalContext) this.stages.add(event.traversalContext.runtimeStageId);
  }

  override clear(): void {
    super.clear(); this.stages.clear(); this.failed = false;
  }

  toSnapshot(): Omit<RecorderSnapshot, 'id'> {
    const controlDeps: Record<string, ControlDependency> = {};
    for (const stage of this.stages) {
      const dependency = this.lookup(stage);
      if (dependency) controlDeps[stage] = dependency;
    }
    return { name: 'window-control-dependencies', data: { decisions: this.getDecisions(), controlDeps } };
  }
}

function identifier(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 512 && !/[\u0000-\u001f\u007f]/.test(value);
}

function unavailable(): TraceOutcome {
  return { status: 'unavailable', error: {
    code: 'WINDOW_TRACE_UNAVAILABLE',
    message: 'Window execution completed; trace persistence was not acknowledged.',
  } };
}

function traceReference(ack: unknown): TraceRef | undefined {
  if (ack === null || typeof ack !== 'object') return undefined;
  const kind = Object.getOwnPropertyDescriptor(ack, 'kind');
  const id = Object.getOwnPropertyDescriptor(ack, 'id');
  if (!kind || !('value' in kind) || kind.value !== 'trace' || !id || !('value' in id) || !identifier(id.value)) return undefined;
  return { kind: 'trace', id: id.value };
}

/** Wait without allowing an uncooperative asynchronous chart or sink to retain the caller. */
function abortable<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener('abort', abort);
    const abort = () => { cleanup(); reject(new WindowExecutionError('WINDOW_ABORTED')); };
    // Install rejection handling even when the signal was already cancelled.
    work.then(value => { cleanup(); resolve(value); }, () => { cleanup(); reject(new WindowExecutionError('WINDOW_FAILED')); });
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
  });
}

/**
 * Execute one opt-in collection-window chart with native read/write and decision evidence.
 * This records the chart's actual inputs, not physical causality or cross-window identity.
 * Domain attention and required-evidence declarations remain the adapter's responsibility.
 */
export async function runWindowFlow(options: WindowFlowOptions): Promise<WindowFlowResult> {
  let input: JsonValue | undefined;
  let recordedAt: string;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxRecordBytes = options.maxRecordBytes ?? MAX_RECORD_BYTES;
  try {
    if (!identifier(options.operationId) || !options.chart?.root || !(options.chart.stageMap instanceof Map)
      || !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647
      || !Number.isSafeInteger(maxRecordBytes) || maxRecordBytes <= 0 || maxRecordBytes > MAX_RECORD_BYTES) {
      throw new WindowExecutionError('WINDOW_INVALID');
    }
    if (options.input !== undefined) {
      const encoded = canonical(options.input);
      if (Buffer.byteLength(encoded) > MAX_INPUT_BYTES) throw new WindowExecutionError('WINDOW_INVALID');
      input = JSON.parse(encoded) as JsonValue;
    }
    recordedAt = (options.clock ?? (() => new Date().toISOString()))();
    assertTimestamp(recordedAt);
  } catch { throw new WindowExecutionError('WINDOW_INVALID'); }

  const controller = new AbortController();
  const abort = () => controller.abort();
  if (options.signal?.aborted) throw new WindowExecutionError('WINDOW_ABORTED');
  options.signal?.addEventListener('abort', abort, { once: true });
  const timer = setTimeout(abort, timeoutMs);
  try {
    const control = new WindowControlRecorder();
    let executor: FlowChartExecutor;
    try {
      executor = new FlowChartExecutor(options.chart, { readTracking: 'full', writeProvenance: 'reads-prefix' });
      executor.attachCombinedRecorder(inOutRecorder());
      executor.attachCombinedRecorder(control);
      const execution = await abortable(executor.run({ ...(input === undefined ? {} : { input }), signal: controller.signal }), controller.signal);
      if (controller.signal.aborted) throw new WindowExecutionError('WINDOW_ABORTED');
      if (executor.isPaused() || execution instanceof Error || control.failed) throw new WindowExecutionError('WINDOW_FAILED');
    } catch {
      if (controller.signal.aborted) throw new WindowExecutionError('WINDOW_ABORTED');
      throw new WindowExecutionError('WINDOW_FAILED');
    }

    let encoded: string;
    try { encoded = stringifySnapshot(executor.getSnapshot()); }
    catch { throw new WindowExecutionError('WINDOW_FAILED'); }
    if (Buffer.byteLength(encoded) > maxRecordBytes) throw new WindowExecutionError('WINDOW_RECORD_LIMIT');
    // Native serialization is the detached record boundary; never cast arbitrary source input.
    const record = JSON.parse(encoded) as RuntimeSnapshot;
    if (!options.traceStore) return { record, trace: { status: 'disabled' } };
    try {
      if (controller.signal.aborted) return { record, trace: unavailable() };
      const ack = await abortable(options.traceStore.store({
        operationId: options.operationId, recordedAt, value: JSON.parse(encoded) as JsonValue,
      }), controller.signal);
      const ref = traceReference(ack);
      if (!ref) return { record, trace: unavailable() };
      return { record, trace: { status: 'stored', ref } };
    } catch { return { record, trace: unavailable() }; }
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}
