import {
  FlowChartExecutor, errorCategory, stringifySnapshot,
  type AttemptCategory, type FlowAttemptEvent, type FlowChart, type FlowErrorEvent, type FlowRecorder,
  type FlowStageEvent, type RecorderSnapshot, type RuntimeSnapshot,
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
export interface WindowFailureContext {
  readonly phase: 'validation' | 'initialization' | 'execution' | 'serialization' | 'paused' | 'aborted';
  readonly category: AttemptCategory;
  /** Present only when a native failure or pause event identifies the execution. */
  readonly runtimeStageId?: string;
}
const DEFAULT_FAILURE_PHASE = {
  WINDOW_INVALID: 'validation', WINDOW_ABORTED: 'aborted', WINDOW_FAILED: 'execution', WINDOW_RECORD_LIMIT: 'serialization',
} as const;

/** Fixed refusal with safe native context; never copies raw error text or objects. */
export class WindowExecutionError extends Error {
  readonly context: Readonly<WindowFailureContext>;
  constructor(readonly code: WindowErrorCode, context: WindowFailureContext = { phase: DEFAULT_FAILURE_PHASE[code], category: 'unknown' }) {
    super({
      WINDOW_INVALID: 'Window execution options are invalid.',
      WINDOW_ABORTED: 'Window execution was cancelled or timed out.',
      WINDOW_FAILED: 'Window execution did not complete.',
      WINDOW_RECORD_LIMIT: 'Window record exceeds the configured size limit.',
    }[code]);
    this.name = 'WindowExecutionError';
    this.context = Object.freeze({ phase: context.phase, category: context.category,
      ...(identifier(context.runtimeStageId) ? { runtimeStageId: context.runtimeStageId } : {}) });
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

/** TODO: remove this snapshot bridge after a compatible built-in release:
 * https://github.com/footprintjs/footPrint/issues/108 */
class WindowControlRecorder extends ControlDepRecorder {
  private readonly stages = new Set<string>();

  override onStageExecuted(event: FlowStageEvent): void {
    super.onStageExecuted(event);
    if (event.traversalContext) this.stages.add(event.traversalContext.runtimeStageId);
  }

  override onError(event: FlowErrorEvent): void {
    super.onError(event);
    if (event.traversalContext) this.stages.add(event.traversalContext.runtimeStageId);
  }

  override clear(): void { super.clear(); this.stages.clear(); }

  toSnapshot(): Omit<RecorderSnapshot, 'id'> {
    const controlDeps: Record<string, ControlDependency> = {};
    for (const stage of this.stages) {
      const dependency = this.lookup(stage);
      if (dependency) controlDeps[stage] = dependency;
    }
    return { name: 'window-control-dependencies', data: { decisions: this.getDecisions(), controlDeps } };
  }
}

/** Observe safe terminal context independently of the control recorder's resets. */
class WindowFailureRecorder implements FlowRecorder {
  readonly id = 'window-failure-context';
  private readonly failedAttempts = new Map<string, AttemptCategory>();
  failure: WindowFailureContext | undefined;
  paused: WindowFailureContext | undefined;
  started = false;

  onRunStart(): void { this.started = true; }

  onAttempt(event: FlowAttemptEvent): void {
    const stage = event.traversalContext?.runtimeStageId;
    // Calls/observations and recovered retries are not a terminal stage failure.
    if (stage && event.record.kind === 'stage' && event.record.series === stage
      && event.record.next === 'stop' && event.record.outcome !== 'ok' && event.record.outcome !== 'paused'
      && event.record.category !== undefined) this.failedAttempts.set(stage, event.record.category);
  }

  onStageExecuted(event: FlowStageEvent): void {
    if (event.traversalContext) this.failedAttempts.delete(event.traversalContext.runtimeStageId);
  }

  onError(event: FlowErrorEvent): void {
    const stage = event.traversalContext?.runtimeStageId;
    if (!this.failure) {
      // Served fields only. The shared classifier produces its fixed vocabulary;
      // message, issues, raw exception and arbitrary names/codes never escape.
      const category = (stage && this.failedAttempts.get(stage)) || errorCategory({
        name: event.structuredError.name, code: event.structuredError.code,
      });
      this.failure = { phase: 'execution', category, ...(identifier(stage) ? { runtimeStageId: stage } : {}) };
    }
  }

  onPause(event: Parameters<NonNullable<FlowRecorder['onPause']>>[0]): void {
    const stage = event.traversalContext?.runtimeStageId;
    this.paused = { phase: 'paused', category: 'unknown', ...(identifier(stage) ? { runtimeStageId: stage } : {}) };
  }

  clear(): void {
    this.failedAttempts.clear();
    this.failure = undefined; this.paused = undefined; this.started = false;
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

interface ValidatedWindowOptions {
  readonly operationId: string;
  readonly chart: FlowChart;
  readonly input?: JsonValue;
  readonly traceStore?: TraceStore;
  readonly signal?: AbortSignal;
  readonly timeoutMs: number;
  readonly maxRecordBytes: number;
  readonly recordedAt: string;
}

function validateWindowOptions(options: WindowFlowOptions): ValidatedWindowOptions {
  try {
    const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const maxRecordBytes = options.maxRecordBytes ?? MAX_RECORD_BYTES;
    if (!identifier(options.operationId) || !options.chart?.root || !(options.chart.stageMap instanceof Map)
      || !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647
      || !Number.isSafeInteger(maxRecordBytes) || maxRecordBytes <= 0 || maxRecordBytes > MAX_RECORD_BYTES
      || (options.signal !== undefined && !(options.signal instanceof AbortSignal))) throw undefined;
    let input: JsonValue | undefined;
    if (options.input !== undefined) {
      const encoded = canonical(options.input);
      if (Buffer.byteLength(encoded) > MAX_INPUT_BYTES) throw undefined;
      input = JSON.parse(encoded) as JsonValue;
    }
    const recordedAt = (options.clock ?? (() => new Date().toISOString()))();
    assertTimestamp(recordedAt);
    return { operationId: options.operationId, chart: options.chart, timeoutMs, maxRecordBytes, recordedAt,
      ...(input === undefined ? {} : { input }), ...(options.signal ? { signal: options.signal } : {}),
      ...(options.traceStore ? { traceStore: options.traceStore } : {}) };
  } catch { throw new WindowExecutionError('WINDOW_INVALID', { phase: 'validation', category: 'unknown' }); }
}

interface WindowDeadline {
  readonly signal: AbortSignal;
  abortedContext(): WindowFailureContext;
}

async function withDeadline<T>(options: ValidatedWindowOptions, work: (deadline: WindowDeadline) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  const abort = () => controller.abort();
  const deadline: WindowDeadline = { signal: controller.signal,
    abortedContext: () => ({ phase: 'aborted', category: timedOut ? 'timeout' : 'unknown' }) };
  if (options.signal?.aborted) throw new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
  options.signal?.addEventListener('abort', abort, { once: true });
  const timer = setTimeout(() => { timedOut = true; abort(); }, options.timeoutMs);
  try { return await work(deadline); }
  finally { clearTimeout(timer); options.signal?.removeEventListener('abort', abort); }
}

/** Bound asynchronous waiting without interpreting the thrown value. */
function abortable<T>(work: Promise<T>, deadline: WindowDeadline): Promise<T> {
  const signal = deadline.signal;
  return new Promise<T>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener('abort', abort);
    const abort = () => { cleanup(); reject(new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext())); };
    // Install rejection handling even when the signal was already cancelled.
    work.then(value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
  });
}

async function executeWindow(options: ValidatedWindowOptions, deadline: WindowDeadline): Promise<FlowChartExecutor> {
  const failure = new WindowFailureRecorder();
  let executor: FlowChartExecutor;
  try {
    executor = new FlowChartExecutor(options.chart, { readTracking: 'full', writeProvenance: 'reads-prefix' });
    executor.attachCombinedRecorder(inOutRecorder());
    executor.attachCombinedRecorder(new WindowControlRecorder());
    executor.attachFlowRecorder(failure);
  } catch {
    if (deadline.signal.aborted) throw new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
    throw new WindowExecutionError('WINDOW_FAILED', { phase: 'initialization', category: 'unknown' });
  }
  try {
    const execution = await abortable(executor.run({ ...(options.input === undefined ? {} : { input: options.input }), signal: deadline.signal }), deadline);
    if (deadline.signal.aborted || executor.isPaused() || failure.failure || execution instanceof Error) throw undefined;
    return executor;
  } catch {
    // A window-wide abort cannot blame an unrelated earlier branch failure.
    if (deadline.signal.aborted) throw new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
    throw new WindowExecutionError('WINDOW_FAILED', failure.failure ?? failure.paused ?? {
      phase: failure.started ? 'execution' : 'initialization', category: 'unknown',
    });
  }
}

function serializeRecord(executor: FlowChartExecutor, maxBytes: number): RuntimeSnapshot {
  let encoded: string;
  try { encoded = stringifySnapshot(executor.getSnapshot()); }
  catch { throw new WindowExecutionError('WINDOW_FAILED', { phase: 'serialization', category: 'unknown' }); }
  if (Buffer.byteLength(encoded) > maxBytes) throw new WindowExecutionError('WINDOW_RECORD_LIMIT', { phase: 'serialization', category: 'unknown' });
  try {
    // One native serialization boundary. Reuse the detached immutable value for
    // storage and the caller, with no second parse or mutable shared aliases.
    const record = JSON.parse(encoded) as RuntimeSnapshot;
    const pending: object[] = [record];
    while (pending.length) {
      const item = pending.pop()!;
      for (const value of Object.values(item)) if (value !== null && typeof value === 'object') pending.push(value);
      Object.freeze(item);
    }
    return record;
  } catch { throw new WindowExecutionError('WINDOW_FAILED', { phase: 'serialization', category: 'unknown' }); }
}

async function persistTrace(record: RuntimeSnapshot, options: ValidatedWindowOptions, deadline: WindowDeadline): Promise<TraceOutcome> {
  if (!options.traceStore) return { status: 'disabled' };
  try {
    if (deadline.signal.aborted) return unavailable();
    const ack = await abortable(options.traceStore.store({ operationId: options.operationId, recordedAt: options.recordedAt,
      value: record as unknown as JsonValue }), deadline);
    const ref = traceReference(ack);
    return ref ? { status: 'stored', ref } : unavailable();
  } catch { return unavailable(); }
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
