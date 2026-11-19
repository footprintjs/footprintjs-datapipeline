import assert from 'node:assert/strict';
import { decide, flowChart } from 'footprintjs';
import { stateAt } from 'foottrace';
import { runWindowFlow, type JsonValue } from 'footprintjs-datapipeline';
import type { FilesystemTraceStore } from 'footprintjs-datapipeline/filesystem';

type PreparedScope = Record<string, JsonValue | undefined>;

function createPreparedChart() {
  const chart = flowChart<PreparedScope>('Capture fixture', scope => {
    scope.reading = 40;
  }, 'capture')
    .addDeciderFunction('Check fixture', scope => decide(scope, [
      { when: { reading: { gt: 30 } }, then: 'slow', label: 'Fixture exceeds 30' },
    ], 'normal'), 'check')
    .addFunctionBranch('slow', 'Keep finding', scope => { scope.finding = 'slow'; })
    .addFunctionBranch('normal', 'Keep normal', scope => { scope.finding = 'normal'; })
    .end()
    .setLogger({ log() {}, info() {}, warn() {}, error() {}, debug() {} })
    .build();
  return { chart };
}

export async function checkPreparedFlow(traceStore: FilesystemTraceStore, at: string) {
  const authored = createPreparedChart();
  const operationId = 'current-cohort:window';
  const result = await runWindowFlow({
    operationId,
    chart: authored.chart,
    traceStore,
    signal: new AbortController().signal,
    clock: () => at,
  });
  assert.equal(result.trace.status, 'stored');
  const stored = await traceStore.read(result.trace.ref);
  assert.equal(stored?.operationId, operationId);
  assert.deepEqual(stored.value, result.record);
  assert.equal(result.record.sharedState.finding, 'slow');
  assert.equal(stateAt(result.record, result.record.commitLog.length - 1).state.finding, 'slow');
  assert.ok(result.record.recorders?.some(item => item.name === 'control-dependencies'));
  assert.ok(Object.isFrozen(result.record));
  assert.ok(Object.isFrozen(result.record.sharedState));
  assert.equal(Reflect.set(result.record.sharedState, 'finding', 'changed'), false);
}
