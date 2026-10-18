import type { RuntimeSnapshot } from 'footprintjs';
import type { JsonValue, TraceOutcome } from '../contracts.js';
import { abortable, type WindowDeadline } from './deadline.js';
import type { ValidatedWindowOptions } from './options.js';
import { traceReference } from './references.js';

function unavailable(): TraceOutcome {
  return { status: 'unavailable', error: {
    code: 'WINDOW_TRACE_UNAVAILABLE',
    message: 'Window execution completed; trace persistence was not acknowledged.',
  } };
}

export async function persistTrace(record: RuntimeSnapshot, options: ValidatedWindowOptions, deadline: WindowDeadline): Promise<TraceOutcome> {
  if (!options.traceStore) return { status: 'disabled' };
  try {
    if (deadline.signal.aborted) return unavailable();
    const ack = await abortable(options.traceStore.store({ operationId: options.operationId, recordedAt: options.recordedAt,
      value: record as unknown as JsonValue }), deadline);
    const ref = traceReference(ack);
    return ref ? { status: 'stored', ref } : unavailable();
  } catch { return unavailable(); }
}

