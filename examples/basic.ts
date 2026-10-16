import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  createPipeline,
  type BatchRef,
  type PipelineResult,
  type Source,
  type SourceAdapter,
  type Transform,
} from '../src/index.js';
import {
  createFilesystemRepository,
  createFilesystemTraceStore,
} from '../src/storage/filesystem.js';

const window = {
  basis: 'source',
  start: '2026-10-01T12:00:00Z',
  end: '2026-10-01T12:02:00Z',
} as const;

const source: Source = {
  ref: { id: 'sample-series', version: '1' },
  adapterRef: { id: 'sample-adapter', version: '1' },
};

// An adapter owns acquisition and its completeness declaration. This one is local.
const adapter: SourceAdapter = {
  ref: source.adapterRef,
  async collect() {
    return {
      key: 'sample-window',
      outcome: 'complete',
      coverage: {
        status: 'complete',
        time: { kind: 'interval', ...window },
        selection: { series: 'sample-series', expectedReadings: 2 },
      },
      observations: [10, 14].map((value, index) => ({
        key: `reading-${index}`,
        subject: { series: 'sample-series' },
        schemaRef: { id: 'numeric-reading', version: '1' },
        time: {
          kind: 'point' as const,
          at: index === 0 ? window.start : '2026-10-01T12:01:00Z',
          basis: 'source' as const,
        },
        payload: { value, unit: 'example-unit' },
      })),
    };
  },
};

const mean: Transform = {
  ref: { id: 'arithmetic-mean', version: '1' },
  async apply(inputs, request) {
    const start = Date.parse(request.window.start);
    const end = Date.parse(request.window.end);
    if (!inputs.every(({ batch }) => {
      const time = batch.coverage.time;
      return time.kind === 'interval' && time.basis === request.window.basis
        && Date.parse(time.start) <= start && Date.parse(time.end) >= end;
    })) throw new Error('This example requires input coverage over the requested window.');

    const observations = inputs.flatMap((input) => input.observations).filter((observation) => {
      if (observation.time.kind !== 'point' || observation.time.basis !== request.window.basis) {
        throw new Error('This example averages point readings on the requested clock.');
      }
      const at = Date.parse(observation.time.at);
      return at >= start && at < end;
    });
    if (observations.length === 0) throw new Error('The mean needs at least one reading.');

    const values = observations.map((observation) => {
      const payload = observation.payload;
      if (
        observation.schemaRef.id !== 'numeric-reading' || observation.schemaRef.version !== '1'
        ||
        payload === null || typeof payload !== 'object' || Array.isArray(payload)
        || !('value' in payload) || typeof payload.value !== 'number'
        || !Number.isFinite(payload.value)
        || !('unit' in payload) || payload.unit !== 'example-unit'
      ) throw new Error('The mean needs finite numeric reading payloads.');
      return payload.value;
    });
    const coverage = inputs.some((input) => input.batch.coverage.status === 'unknown')
      ? 'unknown'
      : inputs.every((input) => input.batch.coverage.status === 'complete'
        && input.batch.outcome === 'complete') ? 'complete' : 'partial';

    return {
      key: 'mean',
      outcome: coverage === 'complete' ? 'complete' : 'partial',
      coverage: { status: coverage, time: { kind: 'interval', ...request.window } },
      observations: [{
        key: 'mean',
        subject: { series: 'sample-series' },
        schemaRef: { id: 'numeric-summary', version: '1' },
        time: { kind: 'interval', ...request.window },
        payload: {
          mean: values.reduce((total, value) => total + value, 0) / values.length,
          count: values.length,
          unit: 'example-unit',
        },
        inputRefs: observations.map((observation) => observation.ref),
      }],
    };
  },
};

function committedBatch(result: PipelineResult): BatchRef {
  if (result.commit.status !== 'committed' || result.commit.batchRef === undefined) {
    throw new Error(`Expected a committed batch: ${JSON.stringify(result.commit)}`);
  }
  return result.commit.batchRef;
}

const directory = await mkdtemp(join(tmpdir(), 'footprint-datapipeline-'));
try {
  const pipeline = createPipeline({
    sources: [source],
    adapters: [adapter],
    transforms: [mean],
    repository: createFilesystemRepository({ directory: join(directory, 'records') }),
    traceStore: createFilesystemTraceStore({ directory: join(directory, 'traces') }),
  });

  const request = {
    sourceRef: source.ref,
    idempotencyKey: 'collect-sample-window',
    request: { window },
  };
  const collected = await pipeline.collect(request);
  const collectedRef = committedBatch(collected);
  const retry = await pipeline.collect(request);
  if (committedBatch(retry).id !== collectedRef.id) {
    throw new Error('A retry should resolve to the same committed batch.');
  }

  const derived = await pipeline.derive({
    methodRef: mean.ref,
    idempotencyKey: 'derive-sample-mean',
    inputBatchRefs: [collectedRef],
    window,
  });

  // Read published records through the same API, including their evidence links.
  for (const [label, result] of [['collection', collected], ['derivation', derived]] as const) {
    const batch = await pipeline.read(committedBatch(result));
    if (batch?.kind !== 'batch') throw new Error('The published batch is missing.');
    const evidence = await pipeline.read(batch.evidenceRef);
    const observations = await Promise.all(batch.observationRefs.map((ref) => pipeline.read(ref)));
    console.log(label, JSON.stringify({
      commit: result.commit,
      trace: result.trace,
      coverage: batch.coverage,
      observations,
      evidence,
    }, null, 2));
  }
} finally {
  await rm(directory, { recursive: true, force: true });
}
