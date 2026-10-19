import { afterEach, describe, expect, it, vi } from 'vitest';
import { decide, flowChart, FlowChartExecutor, type FlowChart } from 'footprintjs';
import { ControlDepRecorder } from 'footprintjs/trace';
import { runWindowFlow } from '../src/index.js';

afterEach(() => { vi.restoreAllMocks(); });
const silent = { info() {}, log() {}, debug() {}, error() {}, warn() {} };

async function terminalProjection(chart: FlowChart) {
  const executor = new FlowChartExecutor(chart);
  await expect(executor.run()).rejects.toBeDefined();
  const projection = executor.getFailureContext();
  expect(projection?.stage?.runtimeStageId).toBeTypeOf('string');
  if (!projection?.stage) throw new Error('The native stage must own this fixture failure.');
  return { phase: 'execution', category: projection.category, runtimeStageId: projection.stage.runtimeStageId };
}

describe('native window evidence owners', () => {
  it('keeps input-contract admission in initialization before any native stage starts', async () => {
    const execute = vi.fn(); const store = vi.fn();
    const chart = flowChart('Read admitted source', execute, 'source')
      .contract({ input: { type: 'object', required: ['value'], properties: { value: { type: 'number' } } } })
      .setLogger(silent).build();
    const failure = await runWindowFlow({ operationId: 'native-owner:input-admission', chart,
      input: { missing: true }, traceStore: { store } }).catch(error => error);
    expect(failure).toMatchObject({ code: 'WINDOW_FAILED', context: { phase: 'initialization', category: 'unknown' } });
    expect(failure.context).not.toHaveProperty('runtimeStageId');
    expect(execute).not.toHaveBeenCalled();
    expect(store).not.toHaveBeenCalled();
  });

  it('uses the built-in control snapshot and its canonical bundle for a real rule', async () => {
    const snapshot = vi.spyOn(ControlDepRecorder.prototype, 'toSnapshot');
    const chart = flowChart<{ latency: number; finding: string }>('Read latency', scope => {
      scope.latency = 40;
    }, 'capture')
      .addDeciderFunction('Check latency', scope => decide(scope, [
        { when: { latency: { gt: 30 } }, then: 'slow', label: 'Latency exceeds the retained threshold' },
      ], 'normal'), 'check')
      .addFunctionBranch('slow', 'Record slow latency', scope => { scope.finding = 'slow'; })
      .addFunctionBranch('normal', 'Record normal latency', scope => { scope.finding = 'normal'; })
      .end().setLogger(silent).build();
    const { record } = await runWindowFlow({ operationId: 'native-owner:control', chart });
    expect(snapshot).toHaveBeenCalled();
    const controls = record.recorders?.filter(bundle => bundle.name === 'control-dependencies');
    expect(controls).toHaveLength(1);
    expect(controls?.[0]?.data).toMatchObject({ decisions: [expect.objectContaining({
      ruleLabel: 'Latency exceeds the retained threshold', evidence: expect.objectContaining({ chosen: 'slow' }),
    })], controlDeps: expect.objectContaining({}) });
    expect(JSON.stringify(controls?.[0]?.data)).toContain('deciderId');
    expect(record.recorders?.some(bundle => bundle.name === 'window-control-dependencies')).toBe(false);
  });

  it('reads terminal ownership from the native accessor rather than another error observer', async () => {
    const read = vi.spyOn(FlowChartExecutor.prototype, 'getFailureContext');
    const chart = flowChart('Read inventory', () => {
      throw Object.assign(new Error('synthetic private response'), { status: 503 });
    }, 'inventory').setLogger(silent).build();
    const failure = await runWindowFlow({ operationId: 'native-owner:failure', chart }).catch(error => error);
    expect(read).toHaveBeenCalled();
    const native = read.mock.results.find(result => result.type === 'return' && result.value?.stage)?.value;
    expect(native?.stage?.runtimeStageId).toBeTypeOf('string');
    expect(failure.context).toEqual({ phase: 'execution', category: native?.category,
      runtimeStageId: native?.stage?.runtimeStageId });
    expect(JSON.stringify(failure)).not.toMatch(/private response|503|raw|stack/);
  });

  it('preserves the exact inner execution identity of a nested terminal failure', async () => {
    const inner = flowChart('Read inner inventory', () => {
      throw Object.assign(new Error('synthetic nested failure'), { code: 'ECONNRESET' });
    }, 'inventory-source').setLogger(silent).build();
    const middle = flowChart('Prepare inner collection', () => {}, 'prepare-inner')
      .addSubFlowChartNext('inner-mount', inner, 'Inner inventory').setLogger(silent).build();
    const chart = flowChart('Prepare collection', () => {}, 'prepare')
      .addSubFlowChartNext('outer-mount', middle, 'Outer collection').setLogger(silent).build();
    const expected = await terminalProjection(chart);
    expect(expected.runtimeStageId).toContain('inventory-source#');
    await expect(runWindowFlow({ operationId: 'native-owner:nested', chart }))
      .rejects.toMatchObject({ code: 'WINDOW_FAILED', context: expected });
  });

  it('uses native commit-failure ownership even when the stage function returned normally', async () => {
    const chart = flowChart('Capture source', scope => { scope.$setValue('source', 40); }, 'source')
      .addFunction('Write an unsupported value', scope => {
        scope.$setValue('unsupported', () => 'synthetic uncloneable value');
      }, 'commit-source').setLogger(silent).build();
    const expected = await terminalProjection(chart);
    expect(expected.runtimeStageId).toContain('commit-source#');
    await expect(runWindowFlow({ operationId: 'native-owner:commit', chart }))
      .rejects.toMatchObject({ code: 'WINDOW_FAILED', context: expected });
  });

  it('does not promote a contained branch error into a failed completed window', async () => {
    const chart = flowChart('Read sources', scope => { scope.$setValue('latency', 40); }, 'capture')
      .addListOfFunction([
        { id: 'unavailable', name: 'Optional source unavailable', fn: () => {
          throw Object.assign(new Error('synthetic optional source'), { status: 503 });
        } },
        { id: 'healthy', name: 'Healthy source', fn: () => {} },
      ], { failFast: false }).setLogger(silent).build();
    const native = new FlowChartExecutor(chart);
    await native.run();
    const result = await runWindowFlow({ operationId: 'native-owner:contained', chart });
    expect(result.record.sharedState.latency).toBe(40);
    expect(result.trace.status).toBe('disabled');
  });

  it('reports the native pause when a sibling error is contained by the chart policy', async () => {
    const chart = flowChart('Read sources', () => {}, 'capture')
      .addSelectorFunction('Read optional sources', () => ['suspend', 'unavailable'], 'sources', undefined, { failFast: false })
        .addPausableFunctionBranch('suspend', 'Wait for inventory', { execute: () => ({ pending: true }), resume: () => {} })
        .addFunctionBranch('unavailable', 'Optional source unavailable', () => {
          throw Object.assign(new Error('synthetic optional source'), { code: 'ECONNRESET' });
        })
      .end().setLogger(silent).build();
    const native = new FlowChartExecutor(chart);
    await native.run();
    expect(native.isPaused()).toBe(true);
    expect(native.getFailureContext()).toBeUndefined();
    const execution = native.getCheckpoint()?.pausedExecution;
    expect(execution?.runtimeStageId).toMatch(/^suspend#\d+$/);
    await expect(runWindowFlow({ operationId: 'native-owner:contained-pause', chart }))
      .rejects.toMatchObject({ code: 'WINDOW_FAILED', context: {
        phase: 'paused', category: 'unknown', runtimeStageId: execution?.runtimeStageId,
      } });
  });

  it('does not let a contained branch steal ownership of the later escaping failure', async () => {
    const chart = flowChart('Read sources', () => {}, 'capture')
      .addListOfFunction([
        { id: 'unavailable', name: 'Optional source unavailable', fn: () => {
          throw Object.assign(new Error('synthetic optional source'), { status: 503 });
        } },
        { id: 'healthy', name: 'Healthy source', fn: () => {} },
      ], { failFast: false })
      .addFunction('Reject unsupported terminal state', () => {
        throw Object.assign(new Error('synthetic terminal validation'), { name: 'ValidationError' });
      }, 'terminal').setLogger(silent).build();
    const failure = await runWindowFlow({ operationId: 'native-owner:later-failure', chart }).catch(error => error);
    expect(failure.context).toEqual({ phase: 'execution', category: 'permanent',
      runtimeStageId: expect.stringMatching(/^terminal#\d+$/) });
  });
});
