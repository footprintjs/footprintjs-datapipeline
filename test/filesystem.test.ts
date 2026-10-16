import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { BatchOutcome, Evidence, Observation } from '../src/contracts.js';
import type { CommitWrite } from '../src/ports.js';
import { createFilesystemRepository, createFilesystemTraceStore } from '../src/storage/filesystem.js';

const fault = vi.hoisted(() => ({ failRename: false }));
vi.mock('node:fs/promises', async importOriginal => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    rename: async (...args: Parameters<typeof actual.rename>) => {
      if (fault.failRename) throw Object.assign(new Error('Injected publication failure'), { code: 'EIO' });
      return actual.rename(...args);
    },
  };
});

const temporary: string[] = [];
async function directory(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), 'datapipeline-filesystem-'));
  temporary.push(path);
  return path;
}
afterEach(async () => {
  fault.failRename = false;
  await Promise.all(temporary.splice(0).map(path => rm(path, { recursive: true, force: true })));
});

function candidate(operationId = 'collect:one', fingerprint = 'request-one', value = 12, outcome: BatchOutcome = 'complete'): CommitWrite {
  const batchRef = { kind: 'batch' as const, id: `${operationId}:batch` };
  const evidenceRef = { kind: 'evidence' as const, id: `${operationId}:evidence` };
  const observationRef = { kind: 'observation' as const, id: `${operationId}:sample` };
  const sourceRef = { id: 'array-a', version: 'config-1' };
  const timestamp = '2026-10-16T13:00:00.000Z';
  const time = { kind: 'point' as const, basis: 'source' as const, at: timestamp };
  const observation: Observation = {
    kind: 'observation', ref: observationRef, key: 'sample', batchRef, evidenceRef,
    subject: { node: 1 }, payload: { latency_us: value }, schemaRef: { id: 'latency', version: '1' },
    time, support: { kind: 'source', sourceRef },
  };
  const evidence: Evidence = {
    kind: 'evidence', ref: evidenceRef, operationId, fingerprint,
    origin: { kind: 'collect', sourceRef, adapterRef: { id: 'vendor', version: '1' }, sourceConfigFingerprint: 'configuration-digest', request: {} },
    startedAt: timestamp, finishedAt: timestamp, outcome, outputRefs: [batchRef, observationRef],
  };
  return {
    operationId, fingerprint, observations: [observation], evidence,
    batch: { kind: 'batch', ref: batchRef, key: 'capture', operationId, observationRefs: [observationRef], evidenceRef,
      coverage: { status: outcome === 'partial' ? 'partial' : 'unknown', time }, outcome,
      startedAt: timestamp, finishedAt: timestamp, sourceRef },
  };
}

describe('filesystem repository', () => {
  it('publishes a batch, observations and evidence readable by another instance', async () => {
    const path = await directory();
    const write = candidate();
    const first = createFilesystemRepository({ directory: path });
    const receipt = await first.commit(write);
    expect(receipt).toMatchObject({ status: 'committed', batchRef: write.batch.ref, evidenceRef: write.evidence.ref });
    const second = createFilesystemRepository({ directory: path });
    expect(await second.read(write.batch.ref)).toEqual(write.batch);
    expect(await second.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
    expect(await second.read(write.evidence.ref)).toEqual(write.evidence);
    expect(await second.findOperation(write.operationId)).toEqual({ fingerprint: write.fingerprint, receipt });
    expect(await second.read({ kind: 'batch', id: 'missing' })).toBeUndefined();
  });

  it('recovers a lost acknowledgement and preserves the first observation on retry', async () => {
    const path = await directory();
    const write = candidate();
    const receipt = await createFilesystemRepository({ directory: path }).commit(write);
    // The caller loses the receipt. Its next process reconciles instead of
    // replacing the captured source values with a later live response.
    const restarted = createFilesystemRepository({ directory: path });
    expect((await restarted.findOperation(write.operationId))?.receipt).toEqual(receipt);
    expect(await restarted.commit(candidate(write.operationId, write.fingerprint, 999))).toEqual(receipt);
    expect(await restarted.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
    await expect(restarted.commit(candidate(write.operationId, 'different-request'))).rejects.toMatchObject({ code: 'idempotency_conflict' });
  });

  it('publishes one immutable winner for simultaneous identical operation keys', async () => {
    const path = await directory();
    const left = createFilesystemRepository({ directory: path });
    const right = createFilesystemRepository({ directory: path });
    const results = await Promise.all([left.commit(candidate('concurrent', 'same', 10)), right.commit(candidate('concurrent', 'same', 20))]);
    expect(results[0]).toEqual(results[1]);
    const row = await left.read(candidate('concurrent').observations[0]!.ref);
    expect([{ latency_us: 10 }, { latency_us: 20 }]).toContainEqual(row?.kind === 'observation' && row.payload);
    expect((await readdir(join(path, 'operations'))).filter(name => !name.startsWith('.'))).toHaveLength(1);
  });

  it('rejects a simultaneous conflicting request without replacing its winner', async () => {
    const path = await directory();
    const results = await Promise.allSettled([
      createFilesystemRepository({ directory: path }).commit(candidate('raced', 'left')),
      createFilesystemRepository({ directory: path }).commit(candidate('raced', 'right')),
    ]);
    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1);
    const failed = results.find(result => result.status === 'rejected');
    expect(failed?.status === 'rejected' && failed.reason).toMatchObject({ code: 'idempotency_conflict' });
    const committed = await createFilesystemRepository({ directory: path }).findOperation('raced');
    expect(['left', 'right']).toContain(committed?.fingerprint);
  });

  it('does not publish records when the final durable publication fails', async () => {
    const path = await directory();
    const repository = createFilesystemRepository({ directory: path });
    const write = candidate();
    fault.failRename = true;
    await expect(repository.commit(write)).rejects.toThrow('Injected publication failure');
    fault.failRename = false;
    expect(await repository.findOperation(write.operationId)).toBeUndefined();
    expect(await repository.read(write.batch.ref)).toBeUndefined();
    expect(await readdir(join(path, 'operations'))).toEqual([]);
    expect((await repository.commit(write)).status).toBe('committed');
  });

  it('detects corrupted record bytes instead of returning a successful receipt', async () => {
    const path = await directory();
    const repository = createFilesystemRepository({ directory: path });
    const write = candidate();
    await repository.commit(write);
    const operation = (await readdir(join(path, 'operations'))).find(name => !name.startsWith('.'))!;
    const operationPath = join(path, 'operations', operation);
    const manifest = JSON.parse(await readFile(join(operationPath, 'manifest.json'), 'utf8')) as { records: { file: string; ref: { kind: string } }[] };
    const file = manifest.records.find(entry => entry.ref.kind === 'observation')!.file;
    await writeFile(join(operationPath, file), '{}');
    await expect(repository.read(write.observations[0]!.ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
    await expect(repository.findOperation(write.operationId)).rejects.toMatchObject({ code: 'storage_corrupt' });
  });

  it('rejects artifact references whose bytes this backend cannot verify', async () => {
    const path = await directory();
    const write = candidate();
    const { payload: _, ...observation } = write.observations[0]!;
    const external = { ...observation, externalArtifactRef: { uri: 'https://example.invalid/data', sha256: 'a'.repeat(64), sizeBytes: 20 } };
    const repository = createFilesystemRepository({ directory: path });
    await expect(repository.commit({ ...write, observations: [external] })).rejects.toMatchObject({ code: 'external_artifact_unsupported' });
    expect(await repository.findOperation(write.operationId)).toBeUndefined();
  });

  it.each(['partial', 'failed'] as const)('acknowledges a fully stored %s source outcome as committed', async outcome => {
    const path = await directory();
    const repository = createFilesystemRepository({ directory: path });
    const write = candidate('incomplete', 'request', 12, outcome);
    expect((await repository.commit(write)).status).toBe('committed');
    const stored = await repository.read(write.batch.ref);
    expect(stored?.kind === 'batch' && stored.outcome).toBe(outcome);
  });
});

describe('separate filesystem trace store', () => {
  it('stores post-run traces independently from committed data', async () => {
    const path = await directory();
    const trace = { operationId: 'operation', recordedAt: '2026-10-16T13:00:00.000Z', value: { state: 'finished', commitLog: [] } };
    const store = createFilesystemTraceStore({ directory: path });
    const ref = await store.store(trace);
    expect(ref.id).toHaveLength(64);
    expect(await createFilesystemTraceStore({ directory: path }).read(ref)).toEqual(trace);
    expect(await store.store(trace)).toEqual(ref);
    expect(await createFilesystemRepository({ directory: path }).findOperation(trace.operationId)).toBeUndefined();
    await writeFile(join(path, 'traces', `${ref.id}.json`), '{}');
    await expect(store.read(ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
  });
});
