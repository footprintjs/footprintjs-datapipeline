import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { flowChart } from 'footprintjs';
import { stateAt } from 'foottrace';
import {
  createCollectionScheduler, createPipeline, runWindowFlow, WindowExecutionError,
  type PipelineResult, type Source, type SourceAdapter, type Transform,
} from 'footprintjs-datapipeline';
import { createFilesystemRepository, createFilesystemTraceStore } from 'footprintjs-datapipeline/filesystem';
import { createSqliteSummaryStore } from 'footprintjs-datapipeline/sqlite';
import { encodeLineProtocol, type InfluxPoint } from 'footprintjs-datapipeline/influxdb';
import { checkPreparedFlow } from './prepared.js';

// Built by installed TypeScript against PUBLIC packed declarations; executed as emitted ESM.
// Synthetic finite data follows examples/basic.ts and the package's window/prepared-store tests.
const at = '2026-10-01T12:00:00.000Z';
const end = '2026-10-01T12:02:00.000Z';
const window = { basis: 'source' as const, start: at, end };
const directory = join(process.cwd(), 'smoke-data');
await mkdir(directory);
const source: Source = { ref: { id: 'sample-series', version: '1' }, adapterRef: { id: 'sample-adapter', version: '1' } };
let acquisitions = 0;
const adapter: SourceAdapter = { ref: source.adapterRef, async collect() {
  acquisitions++;
  return { key: 'sample-window', outcome: 'complete',
    coverage: { status: 'complete', time: { kind: 'interval', ...window } },
    observations: [10, 14].map((value, index) => ({ key: `reading-${index}`, subject: { series: 'sample-series' },
      schemaRef: { id: 'reading', version: '1' }, time: { kind: 'point' as const, basis: 'source' as const, at }, payload: { value } })),
  };
} };
const transform: Transform = { ref: { id: 'mean', version: '1' }, async apply(inputs, request) {
  const observations = inputs.flatMap(input => input.observations);
  const values = observations.map(observation => {
    const p = observation.payload;
    if (p === null || typeof p !== 'object' || Array.isArray(p) || !('value' in p) || typeof p.value !== 'number')
      throw new Error('Expected finite fixture reading');
    assert.ok(Number.isFinite(p.value)); return p.value;
  });
  assert.equal(values.length, 2);
  return { key: 'mean', outcome: 'complete', coverage: { status: 'complete', time: { kind: 'interval', ...request.window } },
    observations: [{ key: 'mean', subject: { series: 'sample-series' }, schemaRef: { id: 'summary', version: '1' },
      time: { kind: 'interval', ...request.window }, payload: { mean: values.reduce((sum, n) => sum + n, 0) / values.length, count: values.length },
      inputRefs: observations.map(observation => observation.ref) }],
  };
} };
const repositoryDirectory = join(directory, 'records');
const traces = createFilesystemTraceStore({ directory: join(directory, 'traces') });
const pipeline = createPipeline({ sources: [source], adapters: [adapter], transforms: [transform],
  repository: createFilesystemRepository({ directory: repositoryDirectory }), traceStore: traces, clock: () => at });
function committed(result: PipelineResult) {
  assert.equal(result.commit.status, 'committed');
  if (result.commit.status !== 'committed' || !result.commit.batchRef) throw new Error('Missing committed batch');
  return result.commit.batchRef;
}
const request = { sourceRef: source.ref, idempotencyKey: 'current-cohort:collect', request: { window } };
const collected = await pipeline.collect(request);
const first = committed(collected);
assert.equal(committed(await pipeline.collect(request)).id, first.id);
assert.equal(acquisitions, 1, 'Retry must reconcile rather than reacquire');
assert.equal(collected.trace.status, 'stored');
const derived = await pipeline.derive({ methodRef: transform.ref, idempotencyKey: 'current-cohort:derive', inputBatchRefs: [first], window });
assert.equal(derived.trace.status, 'stored');
// Read the producer's actual serialized native records through a reopened
// public store, then replay their committed receipt with the shared reader.
const reopenedTraces = createFilesystemTraceStore({ directory: join(directory, 'traces') });
for (const result of [collected, derived]) {
  if (result.trace.status !== 'stored') throw new Error('Expected a stored native pipeline trace');
  const trace = await reopenedTraces.read(result.trace.ref);
  assert.equal(trace?.operationId, result.commit.operationId);
  assert.ok(trace?.value && typeof trace.value === 'object' && !Array.isArray(trace.value));
  const snapshot = trace.value as unknown as Parameters<typeof stateAt>[0];
  assert.ok(snapshot && Array.isArray(snapshot.commitLog) && snapshot.commitLog.length > 0,
    'The installed engine must produce a real committed native record');
  assert.deepEqual(stateAt(snapshot, snapshot.commitLog.length - 1).state.receipt, result.commit);
}
const batch = await pipeline.read(committed(derived));
assert.equal(batch?.kind, 'batch');
if (batch?.kind !== 'batch') throw new Error('Missing derived batch');
assert.equal((await pipeline.read(batch.evidenceRef))?.kind, 'evidence');
const observation = await pipeline.read(batch.observationRefs[0]!);
assert.equal(observation?.kind, 'observation');
if (observation?.kind !== 'observation') throw new Error('Missing derived observation');
assert.deepEqual(observation.payload, { mean: 12, count: 2 });
assert.deepEqual(await createFilesystemRepository({ directory: repositoryDirectory }).read(observation.ref), observation);

// The scheduler creates real valid summary identities; no private ID/hash law is reimplemented here.
const schedule = { ref: { id: 'current-cohort-poll', version: '1' }, namespace: 'current-cohort',
  sourceRef: source.ref, methodRef: transform.ref, summaryRef: { id: 'mean-summary', version: '1' },
  startAt: at, intervalMs: 120_000, request: {} };
const filename = join(directory, 'summaries.sqlite');
const summaries = createSqliteSummaryStore({ filename });
try {
  const scheduler = createCollectionScheduler({ pipeline, store: summaries, schedules: [schedule], clock: () => at,
    projectors: [{ ref: schedule.summaryRef, async project(value) {
      const p = value.payload;
      if (p === null || typeof p !== 'object' || Array.isArray(p) || !('mean' in p) || !('count' in p) ||
          typeof p.mean !== 'number' || typeof p.count !== 'number')
        throw new Error('Expected derived fixture summary');
      return { key: value.key, subject: value.subject, collectedAt: at, payload: { mean: p.mean, count: p.count } };
    } }],
  });
  const tick = await scheduler.runDue({ now: at });
  assert.equal(tick.hasMore, false); assert.equal(tick.polls.length, 1); assert.equal(tick.polls[0]?.status, 'collected');
} finally { await summaries.close?.(); }
const reopened = createSqliteSummaryStore({ filename });
try {
  const query = await reopened.query({ schedule, start: at, end });
  assert.equal(query.truncated, false); assert.equal(query.summaries.length, 1);
  assert.deepEqual(query.summaries[0]?.payload, { mean: 12, count: 2 });
  assert.equal(query.coverage.collectedPolls, 1);
} finally { await reopened.close?.(); }

const silent = { log() {}, info() {}, warn() {}, error() {}, debug() {} };
await checkPreparedFlow(reopenedTraces, at);
await assert.rejects(runWindowFlow({ operationId: 'current-cohort:failure', chart:
  flowChart('Fail fixture', () => { throw new Error('synthetic failure'); }, 'failure').setLogger(silent).build(),
}), error => error instanceof WindowExecutionError && error.code === 'WINDOW_FAILED' && error.context.phase === 'execution');
await assert.rejects(runWindowFlow({ operationId: 'current-cohort:pause', chart:
  flowChart('Start fixture', () => {}, 'start').addPausableFunction('Pause fixture', {
    execute: () => ({ pending: true }), resume: () => {},
  }, 'paused').setLogger(silent).build(),
}), error => error instanceof WindowExecutionError && error.code === 'WINDOW_FAILED' && error.context.phase === 'paused');
let ran = false;
const stopped = new AbortController(); stopped.abort();
await assert.rejects(runWindowFlow({ operationId: 'current-cohort:cancel', signal: stopped.signal,
  chart: flowChart('Must not execute', () => { ran = true; }, 'never').setLogger(silent).build(),
}), error => error instanceof WindowExecutionError && error.code === 'WINDOW_ABORTED');
assert.equal(ran, false);

// Import and exercise the pure Influx door only; live roundtrips stay in existing owned CI service tests.
const points: InfluxPoint[] = [{ measurement: 'fixture', tags: [], fields: [['value', { type: 'float', value: 12 }]], timestampNs: '1790856000000000000' }];
assert.ok(encodeLineProtocol(points).includes('fixture'));
console.log(JSON.stringify({ publicDoors: ['.', './filesystem', './sqlite', './influxdb'],
  pipeline: 'collect/reconcile/derive/read/reopen', summary: 'scheduled/published/reopened',
  window: 'native-control/failure/pause/preabort/immutable', influx: 'pure-encoder-only' }));
