/** LOCAL SEED: persisted inputs only; no device, InfluxDB, Python or model call. */
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { RuntimeSnapshot } from 'footprintjs';
import { findLastWriter, keysReadFromExecutionTree, type ControlDependency } from 'foottrace';
import { createRecordGraph } from 'foottrace/cause';
import { createPipeline, createRunWindowEvidenceTransform, RUN_RECEIPT_SCHEMA, RUN_CONTRIBUTION_SCHEMA,
  type CandidateBatch, type NumericMetricDefinition, type JsonObject } from '../src/index.js';
import { createFilesystemRepository, createFilesystemTraceStore } from '../src/storage/filesystem.js';

const start = '2026-10-20T12:00:00.000Z', end = '2026-10-20T12:05:00.000Z';
const args = process.argv.slice(2);
assert.ok(args.length === 0 || (args.length === 2 && args[0] === '--output'));
const directory = args[1] ? resolve(args[1]) : await mkdtemp(join(tmpdir(), 'run-window-example-'));
await mkdir(directory, { recursive: true });
assert.equal((await readdir(directory)).length, 0, 'Use a new empty output folder');
const source = { ref: { id: 'LOCAL-SEED', version: '1' }, adapterRef: { id: 'synthetic-runs', version: '1' } };
const metric: NumericMetricDefinition = { sourceRef: source.ref, metricRef: { id: 'poll-average', version: '1' }, unit: 'us',
  grain: 'retained-sample-average', weighting: 'equal-per-sample', clock: 'collection', dimensions: { client: '192.0.2.1' },
  populationRef: { id: 'synthetic', version: '1' }, retentionPolicyRef: { id: 'no-deletion', version: '1' }, gtThresholds: [400] };
const repository = createFilesystemRepository({ directory: join(directory, 'data') });
const traceStore = createFilesystemTraceStore({ directory: join(directory, 'traces'), compression: 'gzip' });
const transform = createRunWindowEvidenceTransform({ enabled: true, metric, anchorAt: start, intervalMs: 5000, traceStore })!;
let now = start;
const pipeline = createPipeline({ repository, traceStore, sources: [source], transforms: [transform], clock: () => now,
  adapters: [{ ref: source.adapterRef, async collect(_source, request): Promise<CandidateBatch> {
    const slot = Number(request), collectedAt = new Date(Date.parse(start) + slot * 5000).toISOString();
    const status = slot === 23 ? 'failed' : slot === 44 ? 'partial' : 'complete';
    return { key: `poll-${slot}`, outcome: 'complete', coverage: { status: 'unknown', time: { kind: 'source-time-unknown' } },
      observations: [{ key: 'receipt', subject: {}, schemaRef: RUN_RECEIPT_SCHEMA, collectedAt,
        time: { kind: 'source-time-unknown' }, payload: { slotAt: collectedAt, status, rawDisposition: 'not-retained', retentionPolicyRef: { ...metric.retentionPolicyRef } } },
      ...(status === 'failed' ? [] : [{ key: 'sample', subject: metric.dimensions, schemaRef: RUN_CONTRIBUTION_SCHEMA, collectedAt,
        time: { kind: 'source-time-unknown' as const }, payload: { receiptKey: 'receipt', metric: JSON.parse(JSON.stringify(metric)), status: 'valid', value: (slot + 1) * 10 } }])] };
  } }] });
try {
  const refs = [];
  const runTraceRefs = [];
  for (let i = 0; i < 60; i++) {
    if (i === 17) continue; // Deliberately no saved poll: never backfill it with a current read.
    now = new Date(Date.parse(start) + i * 5000 + 1000).toISOString();
    const result = await pipeline.collect({ sourceRef: source.ref, idempotencyKey: `poll-${i}`, request: i });
    assert.equal(result.commit.status, 'committed'); refs.push(result.commit.batchRef!);
    assert.equal(result.trace.status, 'stored');
    if (result.trace.status === 'stored') runTraceRefs.push(result.trace.ref);
  }
  now = end;
  const result = await pipeline.derive({ methodRef: transform.ref, idempotencyKey: 'window-cutoff-1205', inputBatchRefs: refs,
    window: { basis: 'collection', start, end }, request: { knowledgeCutoff: end } });
  assert.equal(result.commit.status, 'committed');
  assert.equal(result.trace.status, 'stored');
  const reader = createFilesystemTraceStore({ directory: join(directory, 'traces') });
  if (result.trace.status === 'stored') assert.ok(await reader.read(result.trace.ref), 'Outer derive trace must reopen');
  for (const ref of runTraceRefs) assert.ok(await reader.read(ref), 'Every saved run trace must reopen');
  assert.equal(runTraceRefs.length, 59);
  const traceFiles = await readdir(join(directory, 'traces', 'traces'));
  assert.equal(traceFiles.length, 61); // 59 run traces + native window + outer derive trace.
  assert.ok(traceFiles.every(file => file.endsWith('.json.gz')));
  const summary = await pipeline.read(result.commit.observationRefs![0]!);
  assert.equal(summary?.kind, 'observation');
  if (summary?.kind !== 'observation') throw Error('Missing summary');
  const payload = summary.payload as JsonObject;
  assert.deepEqual(payload.coverage, { expectedPolls: 60, receivedPolls: 59, missingPolls: 1, failedPolls: 1, partialPolls: 1, emptyPolls: 0, continuous: 'unknown' });
  assert.equal((payload.statistics as JsonObject).count, 58);
  assert.equal((payload.gaps as unknown[]).length, 3);
  const evidence = await pipeline.read(summary.evidenceRef);
  if (evidence?.kind !== 'evidence') throw Error('Missing derivation evidence');
  const native = evidence.details?.nativeTrace as JsonObject;
  assert.equal(native.status, 'stored');
  const trace = await reader.read(native.ref as unknown as { kind: 'trace'; id: string });
  const record = trace!.value as unknown as RuntimeSnapshot;
  const reads = keysReadFromExecutionTree(record.executionTree);
  const control = record.recorders!.find(row => row.name === 'control-dependencies')!.data as { controlDeps: Record<string, ControlDependency> };
  const graph = createRecordGraph([{ address: record.logAddress!, commitLog: record.commitLog, complete: true, controlDeps: control.controlDeps,
    reads: Object.fromEntries(record.commitLog.map(c => [c.runtimeStageId, reads.lookup(c.runtimeStageId)])) }]);
  const id = (key: string) => { const c = findLastWriter(record.commitLog, key)!; return graph.nodeId(record.logAddress!, { key, runtimeStageId: c.runtimeStageId, commitIdx: record.commitLog.indexOf(c) }); };
  const context = { signal: new AbortController().signal };
  const statisticParents = await graph.parents(id('statistics'), context);
  assert.ok(statisticParents.edges.some(edge => edge.nodeId === id('input_0') && edge.kind === 'data'));
  const inputParents = await graph.parents(id('input_0'), context);
  assert.ok(inputParents.edges.some(edge => edge.nodeId === id('receipt_0') && edge.kind === 'data'));
  const report = { provenance: 'LOCAL SEED', compression: 'gzip', savedRunTraceCount: runTraceRefs.length, savedTraceCount: traceFiles.length, runTraceRefs, summary, nativeTrace: native, nativeRecordBytes: Buffer.byteLength(JSON.stringify(record)),
    causeProof: 'statistics → input_0 → receipt_0; saved observation IDs are retained at the import boundary',
    limitations: ['No physical cause or cross-window graph edge', 'No original raw response retained', 'No source-operation interval established', 'No retention deletion or production settings changed'] };
  await writeFile(join(directory, 'report.json'), JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ directory, ...payload.coverage as JsonObject, validSamples: 58, gaps: 3, compression: 'gzip', savedRunTraces: runTraceRefs.length, savedTraces: traceFiles.length, nativeRecordBytes: report.nativeRecordBytes, savedSummary: summary.ref.id }));
} finally { if (!args[1]) await rm(directory, { recursive: true, force: true }); }
