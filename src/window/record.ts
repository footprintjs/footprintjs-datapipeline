import { stringifySnapshot, type FlowChartExecutor, type RuntimeSnapshot } from 'footprintjs';
import { deepFreeze } from 'foottrace/write';
import { WindowExecutionError } from './errors.js';

export function serializeRecord(executor: FlowChartExecutor, maxBytes: number): RuntimeSnapshot {
  let encoded: string;
  try { encoded = stringifySnapshot(executor.getSnapshot()); }
  catch { throw new WindowExecutionError('WINDOW_FAILED', { phase: 'serialization', category: 'unknown' }); }
  if (Buffer.byteLength(encoded) > maxBytes) throw new WindowExecutionError('WINDOW_RECORD_LIMIT', { phase: 'serialization', category: 'unknown' });
  try {
    // One native serialization boundary. Reuse the detached immutable value for
    // storage and the caller, with no second parse or mutable shared aliases.
    // Fresh JSON has no already-frozen or unsealable values skipped by deepFreeze.
    const record = JSON.parse(encoded) as RuntimeSnapshot;
    return deepFreeze(record);
  } catch { throw new WindowExecutionError('WINDOW_FAILED', { phase: 'serialization', category: 'unknown' }); }
}

