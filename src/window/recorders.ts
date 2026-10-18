import { errorCategory, type AttemptCategory, type FlowAttemptEvent, type FlowErrorEvent, type FlowRecorder, type FlowStageEvent, type RecorderSnapshot } from 'footprintjs';
import { ControlDepRecorder } from 'footprintjs/trace';
import type { ControlDependency } from 'foottrace';
import type { WindowFailureContext } from './errors.js';
import { identifier } from './references.js';

/** TODO: remove this snapshot bridge after a compatible built-in release:
 * https://github.com/footprintjs/footPrint/issues/108 */
export class WindowControlRecorder extends ControlDepRecorder {
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

/** TODO: replace this observer with the run's safe terminal failure API:
 * https://github.com/footprintjs/footPrint/issues/109 */
export class WindowFailureRecorder implements FlowRecorder {
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

