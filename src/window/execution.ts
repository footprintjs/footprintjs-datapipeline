import { FlowChartExecutor } from 'footprintjs';
import { inOutRecorder } from 'footprintjs/trace';
import { WindowControlRecorder, WindowFailureRecorder } from './recorders.js';
import { WindowExecutionError } from './errors.js';
import { abortable, type WindowDeadline } from './deadline.js';
import type { ValidatedWindowOptions } from './options.js';

/** A window-wide abort cannot blame an unrelated earlier branch failure. */
function executionFailure(failure: WindowFailureRecorder, deadline: WindowDeadline): WindowExecutionError {
  if (deadline.signal.aborted) return new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
  return new WindowExecutionError('WINDOW_FAILED', failure.failure ?? failure.paused ?? {
    phase: failure.started ? 'execution' : 'initialization', category: 'unknown',
  });
}

export async function executeWindow(options: ValidatedWindowOptions, deadline: WindowDeadline): Promise<FlowChartExecutor> {
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
    if (deadline.signal.aborted || executor.isPaused() || failure.failure || execution instanceof Error) throw executionFailure(failure, deadline);
    return executor;
  } catch {
    throw executionFailure(failure, deadline);
  }
}

