import { FlowChartExecutor } from 'footprintjs';
import { controlDepRecorder, inOutRecorder } from 'footprintjs/trace';
import { WindowExecutionError } from './errors.js';
import { abortable, type WindowDeadline } from './deadline.js';
import type { ValidatedWindowOptions } from './options.js';

/** A window-wide abort cannot blame an unrelated earlier branch failure. */
function executionFailure(executor: FlowChartExecutor, deadline: WindowDeadline, started: boolean): WindowExecutionError {
  if (deadline.signal.aborted) return new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
  const failure = executor.getFailureContext();
  return new WindowExecutionError('WINDOW_FAILED', {
    phase: started ? 'execution' : 'initialization', category: failure?.category ?? 'unknown',
    ...(failure?.stage ? { runtimeStageId: failure.stage.runtimeStageId } : {}),
  });
}

function pausedFailure(executor: FlowChartExecutor): WindowExecutionError {
  const execution = executor.getCheckpoint()?.pausedExecution;
  return new WindowExecutionError('WINDOW_FAILED', { phase: 'paused', category: 'unknown',
    ...(execution ? { runtimeStageId: execution.runtimeStageId } : {}) });
}

export async function executeWindow(options: ValidatedWindowOptions, deadline: WindowDeadline): Promise<FlowChartExecutor> {
  let executor: FlowChartExecutor;
  let started = false;
  try {
    executor = new FlowChartExecutor(options.chart, { readTracking: 'full', writeProvenance: 'reads-prefix' });
    executor.attachCombinedRecorder(inOutRecorder());
    executor.attachCombinedRecorder(controlDepRecorder());
    // Lifecycle phase only; categories and ownership belong to the native accessor.
    executor.attachFlowRecorder({ id: 'window-lifecycle', onRunStart() { started = true; } });
  } catch {
    if (deadline.signal.aborted) throw new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
    throw new WindowExecutionError('WINDOW_FAILED', { phase: 'initialization', category: 'unknown' });
  }
  let execution: unknown;
  try {
    execution = await abortable(executor.run({ ...(options.input === undefined ? {} : { input: options.input }), signal: deadline.signal }), deadline);
  } catch {
    throw executionFailure(executor, deadline, started);
  }
  if (deadline.signal.aborted) throw new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
  if (executor.isPaused()) throw pausedFailure(executor);
  if (execution instanceof Error) throw executionFailure(executor, deadline, started);
  return executor;
}

