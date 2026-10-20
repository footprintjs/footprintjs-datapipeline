/** LOCAL SEED benchmark: real filesystem publication; no device or database. */
import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { RuntimeSnapshot } from 'footprintjs';
import { createPipeline, createRunWindowEvidenceTransform, RUN_RECEIPT_SCHEMA, RUN_CONTRIBUTION_SCHEMA,
  MAX_RUN_WINDOW_INPUTS, type BatchInput, type CandidateBatch, type JsonObject, type NumericMetricDefinition, type Observation, type Repository } from '../src/index.js';
import { createFilesystemRepository, createFilesystemTraceStore } from '../src/storage/filesystem.js';
import { admitWindow } from '../src/runWindow/admission.js';
import { evidenceChart } from '../src/runWindow/chart.js';
import { validateWindowOptions } from '../src/window/options.js';
import { withDeadline } from '../src/window/deadline.js';
import { executeWindow } from '../src/window/execution.js';
import { serializeRecord } from '../src/window/record.js';

const start = '2026-10-20T12:00:00.000Z', end = '2026-10-20T12:05:20.000Z';
const source = { ref: { id: 'LOCAL-SEED', version: '1' }, adapterRef: { id: 'timing-fixture', version: '1' } };
const metric: NumericMetricDefinition = { sourceRef: source.ref, metricRef: { id: 'poll-value', version: '1' }, unit: 'us',
  grain: 'retained-poll-average', weighting: 'equal-per-sample', clock: 'collection', dimensions: { client: '192.0.2.1' },
  populationRef: { id: 'synthetic', version: '1' }, retentionPolicyRef: { id: 'no-deletion', version: '1' }, gtThresholds: [25] };
const round = (ms: number) => Math.round(ms * 100) / 100;
const profiles = [];
for (const series of [1, 127]) {
  const directory = await mkdtemp(join(tmpdir(), 'window-timing-'));
  try {
    const raw = createFilesystemRepository({ directory: join(directory, 'data') });
    const fsTraces = createFilesystemTraceStore({ directory: join(directory, 'traces') });
    let readMs = 0, readCount = 0, commitMs = 0, traceMs = 0;
    const repository: Repository = { findOperation: id => raw.findOperation(id),
      async readMany(refs) { const t = performance.now(); try { return await raw.readMany(refs); } finally { readMs += performance.now() - t; readCount += refs.length; } },
      async read(ref) { const t = performance.now(); try { return await raw.read(ref); } finally { readMs += performance.now() - t; readCount++; } },
      async commit(write) { const t = performance.now(); try { return await raw.commit(write); } finally { commitMs += performance.now() - t; } } };
    let record: RuntimeSnapshot | undefined;
    const options = { enabled: true, metric, anchorAt: start, intervalMs: 5000,
      traceStore: { async store(trace: Parameters<typeof fsTraces.store>[0]) {
        record = trace.value as unknown as RuntimeSnapshot;
        const t = performance.now(); try { return await fsTraces.store(trace); } finally { traceMs += performance.now() - t; }
      } } };
    const transform = createRunWindowEvidenceTransform(options)!;
    let now = start;
    const pipeline = createPipeline({ repository, sources: [source], transforms: [transform], clock: () => now,
      adapters: [{ ref: source.adapterRef, async collect(_source, request): Promise<CandidateBatch> {
        const slot = Number(request), at = new Date(Date.parse(start) + slot * 5000).toISOString();
        return { key: `run-${slot}`, outcome: 'complete', coverage: { status: 'unknown', time: { kind: 'source-time-unknown' } },
          observations: [{ key: 'receipt', subject: {}, schemaRef: RUN_RECEIPT_SCHEMA, collectedAt: at,
            time: { kind: 'source-time-unknown' }, payload: { slotAt: at, status: 'complete', rawDisposition: 'not-retained', retentionPolicyRef: { ...metric.retentionPolicyRef } } },
          ...Array.from({ length: series }, (_, i) => { const dimensions = { client: `192.0.2.${i + 1}` };
            return { key: `series-${i}`, subject: dimensions, schemaRef: RUN_CONTRIBUTION_SCHEMA, collectedAt: at,
              time: { kind: 'source-time-unknown' as const }, payload: { receiptKey: 'receipt',
                metric: JSON.parse(JSON.stringify({ ...metric, dimensions })), status: 'valid', value: slot + i } }; })] };
      } }] });
    const setupStarted = performance.now(), refs = [];
    for (let slot = 0; slot < 64; slot++) {
      now = new Date(Date.parse(start) + slot * 5000 + 1000).toISOString();
      const result = await pipeline.collect({ sourceRef: source.ref, idempotencyKey: `poll-${slot}`, request: slot });
      assert.equal(result.commit.status, 'committed'); refs.push(result.commit.batchRef!);
    }
    const setupMs = performance.now() - setupStarted;
    now = end;
    const base = { methodRef: transform.ref, inputBatchRefs: refs, window: { basis: 'collection' as const, start, end }, request: { knowledgeCutoff: end } };
    const derivations = [];
    for (let trial = 0; trial < 3; trial++) {
      readMs = 0; readCount = 0; commitMs = 0; traceMs = 0;
      const t = performance.now();
      const result = await pipeline.derive({ ...base, idempotencyKey: `assessment-${trial}` });
      const elapsedMs = performance.now() - t;
      assert.equal(result.commit.status, 'committed');
      assert.ok(record); assert.equal((record.sharedState.statistics as JsonObject).count, 64);
      assert.equal((record.sharedState.statistics as JsonObject).mean, 31.5);
      assert.equal((record.sharedState.coverage as JsonObject).missingPolls, 0);
      assert.ok(Buffer.byteLength(JSON.stringify(record)) < 8 * 1024 * 1024);
      derivations.push({ elapsedMs: round(elapsedMs), repositoryReadMs: round(readMs), repositoryReads: readCount,
        publicationMs: round(commitMs), nativeTraceSaveMs: round(traceMs) });
    }
    // Reopen once, outside the component timings, then use actual production stages.
    const inputs: BatchInput[] = [];
    for (const ref of refs) {
      const batch = await raw.read(ref); assert.equal(batch?.kind, 'batch');
      if (batch?.kind !== 'batch') throw Error('Missing saved batch');
      const observations = await raw.readMany(batch.observationRefs);
      assert.ok(observations.every(value => value?.kind === 'observation'));
      inputs.push({ batch, observations: observations as Observation[] });
    }
    const observationCount = inputs.reduce((sum, input) => sum + input.observations.length, 0);
    if (series === 127) assert.equal(observationCount, MAX_RUN_WINDOW_INPUTS);
    const components = [];
    for (let trial = 0; trial < 3; trial++) {
      const t = performance.now();
      const admitted = admitWindow(inputs, { ...base, idempotencyKey: 'component-check' }, options);
      const admissionMs = performance.now() - t, buildAt = performance.now();
      const chart = evidenceChart(admitted.slots, options, String((record!.sharedState.schedule as JsonObject).windowRef), end);
      const chartBuildMs = performance.now() - buildAt;
      const validated = validateWindowOptions({ operationId: `component-${trial}`, chart });
      const measured = await withDeadline(validated, async deadline => {
        const runAt = performance.now(); const executor = await executeWindow(validated, deadline);
        const executionMs = performance.now() - runAt, serializeAt = performance.now();
        const reopened = serializeRecord(executor, validated.maxRecordBytes);
        assert.equal((reopened.sharedState.statistics as JsonObject).mean, 31.5);
        return { executionMs: round(executionMs), serializeParseFreezeMs: round(performance.now() - serializeAt), nativeRecordBytes: Buffer.byteLength(JSON.stringify(reopened)) };
      });
      components.push({ admissionMs: round(admissionMs), chartBuildMs: round(chartBuildMs), ...measured });
    }
    const profile = { slots: 64, observations: observationCount, seriesPerPoll: series, summarizedSeries: 1, setupMs: round(setupMs), derivations, components };
    profiles.push(profile); console.log(JSON.stringify(profile));
  } finally { await rm(directory, { recursive: true, force: true }); }
}
const report = { provenance: 'LOCAL SEED; actual filesystem repository; no live source', node: process.versions.node,
  platform: process.platform, architecture: process.arch, profiles,
  limitations: ['Three sequential trials on one developer laptop; not a production throughput guarantee.',
    'Filesystem/cache state affects I/O timings. Trial 1 is first derivation after ingestion, not a cold OS cache.',
    'Component timings use already-loaded saved inputs; end-to-end derivation separately includes repository reads and publication.',
    'The 8192-observation profile scans 127 series per poll but summarizes only the one declared series.',
    'No API limits, production deadlines, Python collector, mapping flag or deployment changed.'] };
if (process.argv[2]) await writeFile(resolve(process.argv[2]), JSON.stringify(report, null, 2) + '\n');
