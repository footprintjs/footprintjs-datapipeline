import { mkdtemp, realpath, rm, lstat, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createPipeline } from '../../src/createPipeline.js';
import { createRetainedFilesystemRepository } from '../../src/storage/retainedRepository/index.js';
import type { RepositoryHistoryPolicy } from '../../src/storage/retainedRepository/contracts.js';
export const DAY = 86400_000;
export const namespaceId = '0123456789abcdef0123456789abcdef';
export const request = (key: string) => ({ sourceRef: { id: 'synthetic', version: '1' }, idempotencyKey: key, request: {} });
export async function fixture(maxOperations = 80) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'laptop-history-')));
  let elapsed = 0, calls = 0, wall = 1_790_000_000_000;
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: DAY, requireSeparateFilesystem: false,
    expiryClock: 'verified-uptime' as const, monotonicClock: () => elapsed, clock: () => wall,
    metadataLimits: { maxBytes: 2 * 1024 * 1024, maxOperations, maxReferences: maxOperations * 3 },
    onHealth: (event: unknown) => { alerts.push(event); } };
  const handles: ReturnType<typeof createRetainedFilesystemRepository>[] = [];
  const open = (period: number, overrides: Partial<RepositoryHistoryPolicy> = {}) => {
    const repository = createRetainedFilesystemRepository({ ...options,
      historyRetirement: { profile: 'daily-periods-v1', keepDays: 7, namespaceId, period, maxOperationBytes: 65536, ...overrides } })!;
    handles.push(repository);
    const pipeline = createPipeline({ repository, sources: [{ ref: request('').sourceRef, adapterRef: { id: 'local', version: '1' } }],
      adapters: [{ ref: { id: 'local', version: '1' }, async collect() {
        calls++;
        return { key: 'saved', observations: [{ key: 'sample', subject: { node: 7 }, schemaRef: { id: 'latency', version: '1' },
          time: { kind: 'source-time-unknown' as const }, payload: { latency: 0 } }],
          coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } }, outcome: 'partial' as const };
      } }], clock: () => '2026-10-23T12:00:00.000Z' });
    return { repository, pipeline };
  };
  return { directory, options, open, alerts, calls: () => calls, at: (value: number) => { elapsed = value; }, wall: (value: number) => { wall = value; },
    async close() { for (const repository of handles) repository!.close(); await rm(directory, { recursive: true, force: true }); } };
}
export async function bytes(directory: string): Promise<number> {
  const info = await lstat(directory);
  return info.blocks * 512 + (info.isDirectory() ? (await Promise.all((await readdir(directory)).map(name => bytes(join(directory, name))))).reduce((a, b) => a + b, 0) : 0);
}


export async function scopedWrite(repository: NonNullable<ReturnType<typeof createRetainedFilesystemRepository>>, key: string) {
  const { candidate } = await import('./filesystem-candidate.js');
  const { digest } = await import('../../src/validation.js');
  const seed = candidate(key);
  const identity = repository.identity!;
  const operationId = identity.operationId(digest({ key }));
  await identity.assertOperation(operationId, 'admit');
  const batchRef = identity.reference(operationId, 'batch', seed.batch.key);
  const evidenceRef = identity.reference(operationId, 'evidence', seed.batch.key);
  const observations = seed.observations.map(item => ({ ...item,
    ref: identity.reference(operationId, 'observation', item.key), batchRef, evidenceRef }));
  return { ...seed, operationId, observations,
    batch: { ...seed.batch, operationId, ref: batchRef, evidenceRef, observationRefs: observations.map(item => item.ref) },
    evidence: { ...seed.evidence, operationId, ref: evidenceRef, outputRefs: [batchRef, ...observations.map(item => item.ref)] } };
}

/** Drain the explicit maintenance API; each transaction remains a bounded batch. */
export async function drainHistory(repository: NonNullable<ReturnType<typeof createRetainedFilesystemRepository>>, limit = 100) {
  for (let pass = 0; pass < 10000; pass++) {
    const result = await repository.retireHistory({ limit });
    if (!result.pending || result.blocked) return result;
  }
  throw Error('History maintenance did not converge');
}
