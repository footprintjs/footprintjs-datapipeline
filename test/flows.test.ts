import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPipeline } from '../src/index.js';
import type { Batch, CandidateBatch, Observation, ObservationRef, PipelineResult, Source } from '../src/contracts.js';
import type { OperationContext, SourceAdapter, TraceStore, Transform } from '../src/ports.js';
import { createFilesystemRepository, createFilesystemTraceStore } from '../src/storage/filesystem.js';

const now = '2026-10-16T12:03:00.000Z';
const window = { basis: 'source', start: '2026-10-16T12:00:00.000Z', end: '2026-10-16T12:02:00.000Z' } as const;
const source: Source = { ref: { id: 'array-a', version: 'config-1' }, adapterRef: { id: 'samples', version: '1' }, config: { selection: 'one-node' } };
const request = { sourceRef: source.ref, idempotencyKey: 'capture-one', request: { selection: 'one-node' } };
const temporary: string[] = [];
afterEach(async () => { await Promise.all(temporary.splice(0).map(path => rm(path, { recursive: true, force: true }))); });

function samples(): CandidateBatch {
  return {
    key: 'capture', outcome: 'complete',
    coverage: { status: 'complete', time: { kind: 'interval', basis: 'source', start: '2026-10-16T11:59:00.000Z', end: '2026-10-16T12:03:00.000Z' } },
    observations: [
      ['before', '2026-10-16T11:59:00.000Z', 900],
      ['start', window.start, 10],
      ['middle', '2026-10-16T12:01:00.000Z', 20],
      ['end', window.end, 1000],
    ].map(([key, at, value]) => ({
      key: String(key), subject: { node: 1 }, schemaRef: { id: 'sample-latency', version: '1' },
      time: { kind: 'point', basis: 'source', at: String(at) }, payload: { value: Number(value), unit: 'us' },
    })),
  };
}

async function setup(options: { adapter?: SourceAdapter; transforms?: readonly Transform[]; traceStore?: TraceStore } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'datapipeline-flows-'));
  temporary.push(directory);
  const repository = createFilesystemRepository({ directory });
  const adapter = options.adapter ?? { ref: source.adapterRef, collect: vi.fn(async () => samples()) };
  const pipeline = createPipeline({
    sources: [source], adapters: [adapter], transforms: options.transforms ?? [], repository,
    traceStore: options.traceStore ?? createFilesystemTraceStore({ directory }), clock: () => now,
  });
  return { directory, repository, pipeline, adapter };
}

async function batchOf(pipeline: ReturnType<typeof createPipeline>, result: PipelineResult): Promise<Batch> {
  expect(result.commit.status).toBe('committed');
  expect(result.commit.batchRef).toBeDefined();
  const record = await pipeline.read(result.commit.batchRef!);
  if (record?.kind !== 'batch') throw new Error('The committed batch must resolve.');
  return record;
}

describe('collect and derive with the filesystem repository', () => {
  it('reports a source failure without publishing batch data', async () => {
    const adapter: SourceAdapter = { ref: source.adapterRef, async collect() { throw new Error('Vendor timed out'); } };
    const { pipeline, repository } = await setup({ adapter });
    const result = await pipeline.collect(request);
    expect(result.commit.status).toBe('failed');
    expect(result.commit.error?.message).toContain('Vendor timed out');
    expect(result.commit.batchRef).toBeUndefined();
    expect(await repository.findOperation(result.commit.operationId)).toBeUndefined();
  });

  it('recovers committed data across pipeline instances without recollecting on an idempotent retry', async () => {
    const collect = vi.fn(async (_source: Source, _request: unknown, _context: OperationContext) => samples());
    const adapter: SourceAdapter = { ref: source.adapterRef, collect };
    const { pipeline, directory } = await setup({ adapter });
    const first = await pipeline.collect(request);
    const batch = await batchOf(pipeline, first);
    const reopened = createPipeline({ sources: [source], adapters: [adapter], transforms: [],
      repository: createFilesystemRepository({ directory }), clock: () => now });
    const second = await reopened.collect(request);
    expect(second.commit).toEqual(first.commit);
    expect(collect).toHaveBeenCalledTimes(1);
    expect(await reopened.read(batch.ref)).toEqual(batch);
  });

  it('refuses a different request using an existing key without overwriting or recollecting', async () => {
    const collect = vi.fn(async () => samples());
    const { pipeline } = await setup({ adapter: { ref: source.adapterRef, collect } });
    const first = await pipeline.collect(request);
    const batch = await batchOf(pipeline, first);
    const conflict = await pipeline.collect({ ...request, request: { selection: 'different-node' } });
    expect(conflict.commit.status).toBe('failed');
    expect(conflict.commit.error).toBeDefined();
    expect(collect).toHaveBeenCalledTimes(1);
    expect(await pipeline.read(batch.ref)).toEqual(batch);
    expect((await pipeline.collect(request)).commit).toEqual(first.commit);
  });

  it('keeps the committed result readable when post-run trace storage fails', async () => {
    const store = vi.fn(async () => { throw new Error('Trace disk unavailable'); });
    const { pipeline } = await setup({ traceStore: { store } });
    const result = await pipeline.collect(request);
    const batch = await batchOf(pipeline, result);
    expect(result.trace.status).toBe('unavailable');
    expect(store).toHaveBeenCalledTimes(1);
    expect(await pipeline.read(batch.evidenceRef)).toMatchObject({ kind: 'evidence', outcome: 'complete' });
    expect(await pipeline.read(batch.observationRefs[0]!)).toMatchObject({ kind: 'observation', payload: { value: 900, unit: 'us' } });
  });

  it('resolves committed observations on the source clock with start included and end excluded', async () => {
    let seen: readonly Observation[] = [];
    const transform: Transform = {
      ref: { id: 'mean', version: '1' },
      async apply(inputs, deriveRequest) {
        seen = inputs.flatMap(input => input.observations);
        expect(inputs).toHaveLength(1);
        expect(inputs[0]!.batch.observationRefs).toHaveLength(4);
        expect(seen.map(observation => observation.key)).toEqual(['start', 'middle']);
        const values = seen.map(observation => (observation.payload as { value: number }).value);
        return {
          key: 'mean', outcome: 'complete',
          coverage: { status: 'complete', time: { kind: 'interval', ...deriveRequest.window } },
          details: { weighting: 'equal-retained-observation', method: 'arithmetic-mean' },
          observations: [{ key: 'mean', subject: { node: 1 }, schemaRef: { id: 'summary', version: '1' },
            time: { kind: 'interval', ...deriveRequest.window },
            payload: { mean_us: values.reduce((sum, value) => sum + value, 0) / values.length, count: values.length },
            inputRefs: seen.map(observation => observation.ref) }],
        };
      },
    };
    const { pipeline } = await setup({ transforms: [transform] });
    const captured = await batchOf(pipeline, await pipeline.collect(request));
    const result = await pipeline.derive({ methodRef: transform.ref, idempotencyKey: 'derive-one', inputBatchRefs: [captured.ref], window });
    const derived = await batchOf(pipeline, result);
    const output = await pipeline.read(derived.observationRefs[0]!);
    expect(output).toMatchObject({ kind: 'observation', payload: { mean_us: 15, count: 2 }, support: { kind: 'inputs', inputRefs: seen.map(item => item.ref) } });
    const evidence = await pipeline.read(derived.evidenceRef);
    expect(evidence).toMatchObject({ kind: 'evidence', origin: { kind: 'derive', inputBatchRefs: [captured.ref], window },
      details: { weighting: 'equal-retained-observation', method: 'arithmetic-mean' } });
    expect(result.trace.status).toBe('stored');
  });

  it.each(['invented', 'outside-window'] as const)('rejects %s support references rather than fabricating lineage', async mode => {
    let invalidRef: ObservationRef = { kind: 'observation', id: 'invented' };
    const transform: Transform = {
      ref: { id: 'invalid-support', version: '1' },
      async apply(_inputs, deriveRequest) {
        return {
          key: 'bad-lineage', outcome: 'complete', coverage: { status: 'unknown', time: { kind: 'interval', ...deriveRequest.window } },
          observations: [{ key: 'result', subject: {}, schemaRef: { id: 'result', version: '1' },
            time: { kind: 'interval', ...deriveRequest.window }, payload: 1, inputRefs: [invalidRef] }],
        };
      },
    };
    const { pipeline, repository } = await setup({ transforms: [transform] });
    const captured = await batchOf(pipeline, await pipeline.collect(request));
    if (mode === 'outside-window') invalidRef = captured.observationRefs[3]!;
    const result = await pipeline.derive({ methodRef: transform.ref, idempotencyKey: 'invalid-derive', inputBatchRefs: [captured.ref], window });
    expect(result.commit.status).toBe('failed');
    expect(result.commit.error).toBeDefined();
    expect(await repository.findOperation(result.commit.operationId)).toBeUndefined();
    expect(await pipeline.read(captured.ref)).toEqual(captured);
  });
});
