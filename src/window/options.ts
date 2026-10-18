import type { FlowChart } from 'footprintjs';
import type { JsonValue } from '../contracts.js';
import type { TraceStore } from '../ports.js';
import { assertTimestamp, canonical } from '../validation.js';
import { WindowExecutionError } from './errors.js';
import { identifier } from './references.js';

const MAX_RECORD_BYTES = 8 * 1024 * 1024;
const MAX_INPUT_BYTES = 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;

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

export interface ValidatedWindowOptions {
  readonly operationId: string;
  readonly chart: FlowChart;
  readonly input?: JsonValue;
  readonly traceStore?: TraceStore;
  readonly signal?: AbortSignal;
  readonly timeoutMs: number;
  readonly maxRecordBytes: number;
  readonly recordedAt: string;
}

export function validateWindowOptions(options: WindowFlowOptions): ValidatedWindowOptions {
  try {
    const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const maxRecordBytes = options.maxRecordBytes ?? MAX_RECORD_BYTES;
    if (!identifier(options.operationId) || !options.chart?.root || !(options.chart.stageMap instanceof Map)
      || !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647
      || !Number.isSafeInteger(maxRecordBytes) || maxRecordBytes <= 0 || maxRecordBytes > MAX_RECORD_BYTES
      || (options.signal !== undefined && !(options.signal instanceof AbortSignal))) throw new WindowExecutionError('WINDOW_INVALID');
    let input: JsonValue | undefined;
    if (options.input !== undefined) {
      const encoded = canonical(options.input);
      if (Buffer.byteLength(encoded) > MAX_INPUT_BYTES) throw new WindowExecutionError('WINDOW_INVALID');
      input = JSON.parse(encoded) as JsonValue;
    }
    const recordedAt = (options.clock ?? (() => new Date().toISOString()))();
    assertTimestamp(recordedAt);
    return { operationId: options.operationId, chart: options.chart, timeoutMs, maxRecordBytes, recordedAt,
      ...(input === undefined ? {} : { input }), ...(options.signal ? { signal: options.signal } : {}),
      ...(options.traceStore ? { traceStore: options.traceStore } : {}) };
  } catch { throw new WindowExecutionError('WINDOW_INVALID', { phase: 'validation', category: 'unknown' }); }
}

