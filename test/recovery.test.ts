import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPipeline } from '../src/index.js';
import type { Batch, CandidateBatch, Observation, Source } from '../src/contracts.js';
import type { Repository, SourceAdapter, Transform } from '../src/ports.js';
import { createFilesystemRepository, createFilesystemTraceStore } from '../src/storage/filesystem.js';

const now = '2026-10-16T12:03:00.000Z';
const window = { basis: 'source', start: '2026-10-16T12:00:00.000Z', end: '2026-10-16T12:02:00.000Z' } as const;
const source: Source = { ref: { id: 'array', version: '1' }, adapterRef: { id: 'vendor', version: '1' } };
const request = { sourceRef: source.ref, idempotencyKey: 'capture', request: {} };
const temporary: string[] = [];
afterEach(async () => { await Promise.all(temporary.splice(0).map(path => rm(path, { recursive: true, force: true }))); });
async function location() {
  const directory = await mkdtemp(join(tmpdir(), 'datapipeline-recovery-'));
  temporary.push(directory);
  return { directory, repository: createFilesystemRepository({ directory }) };
}
function sample(value = 10, key = 'sample') {
  return { key, subject: { node: 1 }, schemaRef: { id: 'numeric', version: '1' },
    time: { kind: 'point' as const, basis: 'source' as const, at: window.start }, payload: { value } };
}
function candidate(): CandidateBatch {
  return {
    key: 'capture', outcome: 'complete',
    coverage: { status: 'complete', time: { kind: 'interval', ...window } },
    observations: [sample()],
  };
}
function pipelineFor(repository: Repository, adapter: SourceAdapter, transforms: readonly Transform[] = []) {
  return createPipeline({ sources: [source], adapters: [adapter], transforms, repository, clock: () => now });
}
async function resolveBatch(repository: Repository, ref: { kind: 'batch'; id: string } | undefined): Promise<Batch> {
  if (!ref) throw new Error('A committed batch reference is required.');
  const batch = await repository.read(ref);
  if (batch?.kind !== 'batch') throw new Error('The batch must resolve.');
  return batch;
}

describe('pipeline failure and recovery boundaries', () => {
  it('does not publish a late adapter result after the collection timeout', async () => {
    const { repository } = await location();
    let complete: ((batch: CandidateBatch) => void) | undefined;
    let adapterSignal: AbortSignal | undefined;
    const commit = vi.fn(repository.commit.bind(repository));
    const wrapped: Repository = { findOperation: id => repository.findOperation(id), read: ref => repository.read(ref), commit };
    const adapter: SourceAdapter = {
      ref: source.adapterRef,
      async collect(_source, _request, context) {
        adapterSignal = context.signal;
        // Deliberately ignores cancellation, like an SDK with no abort support.
        return new Promise<CandidateBatch>(resolve => { complete = resolve; });
      },
    };
    const result = await pipelineFor(wrapped, adapter).collect(request, { timeoutMs: 30 });
    expect(result.commit.status).toBe('failed');
    expect(adapterSignal?.aborted).toBe(true);
    expect(complete).toBeDefined();
    complete!(candidate());
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(commit).not.toHaveBeenCalled();
    expect(await repository.findOperation(result.commit.operationId)).toBeUndefined();
  });

  it('reconciles a committed operation when the repository acknowledgement is lost', async () => {
    const { repository } = await location();
    const commit = vi.fn(async (write: Parameters<Repository['commit']>[0]) => {
      await repository.commit(write);
      throw new Error('Acknowledgement lost after durable publication');
    });
    const wrapped: Repository = { findOperation: id => repository.findOperation(id), read: ref => repository.read(ref), commit };
    const collect = vi.fn(async () => candidate());
    const pipeline = pipelineFor(wrapped, { ref: source.adapterRef, collect });
    const result = await pipeline.collect(request);
    expect(result.commit.status).toBe('committed');
    const batch = await resolveBatch(repository, result.commit.batchRef);
    expect(await repository.read(batch.evidenceRef)).toMatchObject({ kind: 'evidence', operationId: result.commit.operationId });
    expect((await pipeline.collect(request)).commit).toEqual(result.commit);
    expect(commit).toHaveBeenCalledTimes(1);
    expect(collect).toHaveBeenCalledTimes(1);
  });

  it('rejects a nonfinite candidate value without acknowledging any publication', async () => {
    const { repository } = await location();
    const commit = vi.fn(repository.commit.bind(repository));
    const wrapped: Repository = { findOperation: id => repository.findOperation(id), read: ref => repository.read(ref), commit };
    const adapter: SourceAdapter = { ref: source.adapterRef, async collect() {
      const batch = candidate();
      return { ...batch, observations: [sample(Number.NaN)] };
    } };
    const result = await pipelineFor(wrapped, adapter).collect(request);
    expect(result.commit.status).toBe('failed');
    expect(result.commit.error).toBeDefined();
    expect(commit).not.toHaveBeenCalled();
    expect(await repository.findOperation(result.commit.operationId)).toBeUndefined();
  });

  it('excludes unknown source times and prevents a transform from upgrading their coverage', async () => {
    const { repository } = await location();
    const adapter: SourceAdapter = { ref: source.adapterRef, async collect() {
      const batch = candidate();
      return { ...batch, observations: [...batch.observations, {
        ...sample(900, 'unknown-time'), time: { kind: 'source-time-unknown' },
      }] };
    } };
    let selected: readonly Observation[] = [];
    const transform: Transform = { ref: { id: 'summary', version: '1' }, async apply(inputs, input) {
      selected = inputs.flatMap(batch => batch.observations);
      return {
        key: 'summary', outcome: 'complete', coverage: { status: 'complete', time: { kind: 'interval', ...input.window } },
        observations: [{ key: 'summary', subject: {}, schemaRef: { id: 'summary', version: '1' },
          time: { kind: 'interval', ...input.window }, payload: { count: selected.length }, inputRefs: selected.map(item => item.ref) }],
      };
    } };
    const pipeline = pipelineFor(repository, adapter, [transform]);
    const capture = await pipeline.collect(request);
    const inputBatch = await resolveBatch(repository, capture.commit.batchRef);
    const derived = await pipeline.derive({ methodRef: transform.ref, idempotencyKey: 'derive', inputBatchRefs: [inputBatch.ref], window });
    expect(derived.commit.status).toBe('committed');
    expect(selected.map(item => item.key)).toEqual(['sample']);
    const outputBatch = await resolveBatch(repository, derived.commit.batchRef);
    expect(outputBatch.coverage.status).toBe('unknown');
    expect(outputBatch.outcome).toBe('partial');
    expect(await repository.read(outputBatch.evidenceRef)).toMatchObject({ kind: 'evidence', details: {
      excludedUnknownTimes: 1, selectedObservationRefs: selected.map(item => item.ref),
    } });
    // The excluded observation remains in the original capture, with unknown
    // time preserved, rather than being rewritten to the collection clock.
    expect(await repository.read(inputBatch.observationRefs[1]!)).toMatchObject({ time: { kind: 'source-time-unknown' } });
  });

  it('persists the four real FootPrint stage commits and input/output recording', async () => {
    const { repository, directory } = await location();
    const traceStore = createFilesystemTraceStore({ directory });
    const pipeline = createPipeline({ sources: [source], adapters: [{ ref: source.adapterRef, async collect() { return candidate(); } }],
      repository, traceStore, clock: () => now });
    const result = await pipeline.collect(request);
    expect(result.commit.status).toBe('committed');
    if (result.trace.status !== 'stored') throw new Error('The trace must be durably stored.');
    const trace = await traceStore.read(result.trace.ref);
    const snapshot = trace?.value as { commitLog?: { runtimeStageId: string; trace: unknown[] }[]; recorders?: { name: string; data: unknown }[] };
    expect(snapshot.commitLog).toHaveLength(4);
    expect(snapshot.commitLog?.map(commit => commit.runtimeStageId.split('#')[0])).toEqual([
      'resolve-source', 'collect-observations', 'prepare-collection', 'publish-collection',
    ]);
    expect(snapshot.commitLog?.every(commit => commit.trace.length > 0)).toBe(true);
    expect(snapshot.recorders?.find(recorder => recorder.name === 'InOut')?.data).toBeDefined();
  });
});
