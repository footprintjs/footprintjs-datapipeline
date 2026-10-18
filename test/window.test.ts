import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { decide, flowChart } from 'footprintjs';
import { causalChain, flattenCausalDAG, keysReadFromExecutionTree, stateAt } from 'foottrace';
import { runWindowFlow } from '../src/index.js';
import { createFilesystemTraceStore } from '../src/storage/filesystem.js';
import type { TraceStore } from '../src/ports.js';

const temporary: string[] = [];
afterEach(async () => { await Promise.all(temporary.splice(0).map(path => rm(path, { recursive: true, force: true }))); });
const now = '2026-10-18T10:00:00.000Z';
type State = { resource_a?: number; resource_b?: number; mean_a?: number; finding_a?: string };

function chart() {
  return flowChart<State>('Capture A', scope => { scope.resource_a = 40; }, 'capture-a')
    .addFunction('Capture B', scope => { scope.resource_b = 900; }, 'capture-b')
    .addFunction('Mean A', scope => { scope.mean_a = scope.resource_a!; }, 'mean-a')
    .addDeciderFunction('Check A', scope => decide(scope, [
      { when: { mean_a: { gt: 30 } }, then: 'slow', label: 'Retained mean exceeds 30 ms' },
    ], { branch: 'unknown', label: 'No supported positive rule' }), 'check-a')
      .addFunctionBranch('slow', 'Record slow A', scope => { scope.finding_a = 'slow'; })
      .addFunctionBranch('unknown', 'Record unknown A', scope => { scope.finding_a = 'unknown'; })
    .end().build();
}

describe('native window execution', () => {
  it('captures exact per-write inputs and native decisions without linking an unread resource', async () => {
    const result = await runWindowFlow({ operationId: 'window:one', chart: chart() });
    expect(result.trace.status).toBe('disabled');
    const derived = result.record.commitLog.find(bundle => bundle.stageId === 'mean-a')!;
    expect(derived.trace.find(entry => entry.path === 'mean_a')?.readKeys).toEqual(['resource_a']);
    const dag = causalChain(result.record.commitLog, derived.runtimeStageId,
      keysReadFromExecutionTree(result.record.executionTree).lookup,
      { edgeAttribution: 'per-write', rootLinkKeys: ['mean_a'] });
    expect(dag).toBeDefined();
    const stages = flattenCausalDAG(dag!).map(node => node.stageId);
    expect(stages).toContain('capture-a');
    expect(stages).not.toContain('capture-b');
    expect(result.record.writeProvenance).toBe('reads-prefix');
    expect(result.record.sharedState.finding_a).toBe('slow');
    const controls = result.record.recorders?.find(item => item.name === 'window-control-dependencies');
    expect(controls?.data).toMatchObject({ decisions: [expect.objectContaining({
      evidence: expect.objectContaining({ rules: [expect.objectContaining({ conditions: [expect.objectContaining({ key: 'mean_a', op: 'gt', threshold: 30, result: true })] })] }),
    })] });
    expect(JSON.stringify(controls?.data)).toContain('deciderId');
  });

  it('stores and reloads the same native record for public Foottrace replay', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'datapipeline-window-')); temporary.push(directory);
    const traceStore = createFilesystemTraceStore({ directory });
    const result = await runWindowFlow({ operationId: 'window:stored', chart: chart(), traceStore, clock: () => now });
    expect(result.trace.status).toBe('stored');
    if (result.trace.status !== 'stored') throw new Error('Stored trace required');
    const stored = await traceStore.read(result.trace.ref);
    expect(stored).toEqual({ operationId: 'window:stored', recordedAt: now, value: result.record });
    const replay = stateAt(result.record, result.record.commitLog.length - 1);
    expect(replay.state.mean_a).toBe(40);
    expect(replay.state.resource_b).toBe(900);
    expect(replay.state.finding_a).toBe('slow');
  });

  it('does not let a failed trace sink erase completed native facts or leak its error', async () => {
    const result = await runWindowFlow({ operationId: 'window:storage-failed', chart: chart(),
      traceStore: { async store() { throw Error('private source password'); } } });
    expect(result.record.sharedState.mean_a).toBe(40);
    expect(result.trace).toEqual({ status: 'unavailable', error: { code: 'WINDOW_TRACE_UNAVAILABLE', message: 'Window execution completed; trace persistence was not acknowledged.' } });
    expect(JSON.stringify(result)).not.toContain('private source password');
  });

  it('detaches the returned record from a sink that mutates its input', async () => {
    const traceStore: TraceStore = { async store(trace) {
      (trace.value as { sharedState: Record<string, unknown> }).sharedState.mean_a = -1;
      return { kind: 'trace', id: 'stored:one' };
    } };
    const result = await runWindowFlow({ operationId: 'window:detached', chart: chart(), traceStore });
    expect(result.record.sharedState.mean_a).toBe(40);
  });

  it('records compact input without mutating the caller', async () => {
    const input = { value: 7 };
    const native = flowChart<{ observed?: number }>('Read run input', scope => {
      scope.observed = scope.$getArgs<{ value: number }>().value;
    }, 'read-input').build();
    const result = await runWindowFlow({ operationId: 'window:input', chart: native, input });
    expect(result.record.sharedState.observed).toBe(7);
    expect(input).toEqual({ value: 7 });
  });

  it('refuses a pre-aborted window before any stage or sink runs', async () => {
    const controller = new AbortController(); controller.abort(Error('secret abort reason'));
    const execute = vi.fn(); const store = vi.fn();
    const native = flowChart('Never start', execute, 'never-start').build();
    await expect(runWindowFlow({ operationId: 'window:aborted', chart: native, signal: controller.signal, traceStore: { store } }))
      .rejects.toMatchObject({ code: 'WINDOW_ABORTED', message: 'Window execution was cancelled or timed out.' });
    expect(execute).not.toHaveBeenCalled(); expect(store).not.toHaveBeenCalled();
  });

  it('bounds a window whose asynchronous stage ignores cancellation', async () => {
    let finish!: () => void;
    const pending = new Promise<void>(resolve => { finish = resolve; });
    const native = flowChart('Slow source', async () => pending, 'slow-source').build();
    const store = vi.fn();
    await expect(runWindowFlow({ operationId: 'window:timeout', chart: native, timeoutMs: 20, traceStore: { store } }))
      .rejects.toMatchObject({ code: 'WINDOW_ABORTED' });
    expect(store).not.toHaveBeenCalled(); finish();
  });

  it('does not report a thrown native stage as a completed window', async () => {
    const native = flowChart('Failed source', () => { throw Error('private response body'); }, 'failed-source').build();
    await expect(runWindowFlow({ operationId: 'window:failed', chart: native }))
      .rejects.toMatchObject({ code: 'WINDOW_FAILED', message: 'Window execution did not complete.' });
  });

  it('does not present a native pause as a completed collection window', async () => {
    const native = flowChart('Start source', () => {}, 'start-source')
      .addPausableFunction('Wait for source', { execute: () => ({ pending: true }), resume: () => {} }, 'wait-source').build();
    const store = vi.fn();
    await expect(runWindowFlow({ operationId: 'window:paused', chart: native, traceStore: { store } }))
      .rejects.toMatchObject({ code: 'WINDOW_FAILED' });
    expect(store).not.toHaveBeenCalled();
  });

  it('returns an unavailable trace when a completed window has an unresponsive sink', async () => {
    const result = await runWindowFlow({ operationId: 'window:slow-sink', chart: chart(), timeoutMs: 30,
      traceStore: { store: () => new Promise(() => {}) } });
    expect(result.record.sharedState.mean_a).toBe(40);
    expect(result.trace.status).toBe('unavailable');
  });

  it('rejects malformed trace acknowledgements without losing the completed record', async () => {
    const result = await runWindowFlow({ operationId: 'window:bad-ack', chart: chart(),
      traceStore: { async store() { return { kind: 'trace', id: '' }; } } });
    expect(result.record.sharedState.mean_a).toBe(40); expect(result.trace.status).toBe('unavailable');
  });

  it('rejects oversized records before sending them to storage', async () => {
    const store = vi.fn();
    await expect(runWindowFlow({ operationId: 'window:limit', chart: chart(), maxRecordBytes: 32, traceStore: { store } }))
      .rejects.toMatchObject({ code: 'WINDOW_RECORD_LIMIT' });
    expect(store).not.toHaveBeenCalled();
  });

  it('cancels an in-flight stage without persisting a partial record', async () => {
    const controller = new AbortController();
    let started!: () => void; let finish!: () => void;
    const entered = new Promise<void>(resolve => { started = resolve; });
    const pending = new Promise<void>(resolve => { finish = resolve; });
    const native = flowChart('Await source', async () => { started(); await pending; }, 'await-source').build();
    const store = vi.fn();
    const running = runWindowFlow({ operationId: 'window:external-abort', chart: native, signal: controller.signal, traceStore: { store } });
    await entered; controller.abort(Error('private abort reason'));
    await expect(running).rejects.toMatchObject({ code: 'WINDOW_ABORTED', message: 'Window execution was cancelled or timed out.' });
    expect(store).not.toHaveBeenCalled(); finish();
  });

  it('does not invoke accessor fields on a trace acknowledgement', async () => {
    const get = vi.fn(() => 'secret');
    const ack = Object.defineProperty({ kind: 'trace' as const, id: 'ack' }, 'id', { get });
    const result = await runWindowFlow({ operationId: 'window:ack-accessor', chart: chart(), traceStore: { async store() { return ack; } } });
    expect(result.trace.status).toBe('unavailable'); expect(get).not.toHaveBeenCalled();
  });

  it('returns a fixed refusal when the clock fails', async () => {
    const execute = vi.fn();
    const native = flowChart('Never start', execute, 'never-start').build();
    await expect(runWindowFlow({ operationId: 'window:clock', chart: native, clock: () => { throw Error('private clock detail'); } }))
      .rejects.toMatchObject({ code: 'WINDOW_INVALID', message: 'Window execution options are invalid.' });
    expect(execute).not.toHaveBeenCalled();
  });

  it.each([0, -1, NaN, Infinity, 8 * 1024 * 1024 + 1])('refuses an invalid record cap %s', async maxRecordBytes => {
    await expect(runWindowFlow({ operationId: 'window:bad-cap', chart: chart(), maxRecordBytes }))
      .rejects.toMatchObject({ code: 'WINDOW_INVALID' });
  });

  it.each([0, -1, NaN, Infinity])('refuses invalid timeout %s before execution', async timeoutMs => {
    await expect(runWindowFlow({ operationId: 'window:bad-timeout', chart: chart(), timeoutMs }))
      .rejects.toMatchObject({ code: 'WINDOW_INVALID' });
  });

  it.each(['', '   ', 'a'.repeat(513), 'window\nsecret'])('refuses an invalid operation identifier', async operationId => {
    await expect(runWindowFlow({ operationId, chart: chart() })).rejects.toMatchObject({ code: 'WINDOW_INVALID' });
  });

  it('refuses unsafe input without executing accessors or retaining their contents', async () => {
    const get = vi.fn(() => 'secret');
    const input = Object.defineProperty({}, 'private', { enumerable: true, get });
    await expect(runWindowFlow({ operationId: 'window:unsafe-input', chart: chart(), input }))
      .rejects.toMatchObject({ code: 'WINDOW_INVALID' });
    expect(get).not.toHaveBeenCalled();
  });
});
