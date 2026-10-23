import { mkdtemp, realpath, rm, unlink } from 'node:fs/promises';
import { lstatSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { RepositoryLedger } from '../src/storage/retainedRepository/ledger.js';
import { candidate } from './helpers/filesystem-candidate.js';
import type { CommitWrite } from '../src/ports.js';

async function fixture(maxOperations = 2, maxReferences = 100) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'metadata-limits-')));
  let elapsed = 0; const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, expiryClock: 'verified-uptime' as const,
    clock: () => 1_790_000_000_000, monotonicClock: () => elapsed, requireSeparateFilesystem: false,
    metadataLimits: { maxBytes: 2 * 1024 * 1024, maxOperations, maxReferences },
    onHealth: (event: unknown) => { alerts.push(event); } };
  const repo = createRetainedFilesystemRepository(options)!;
  return { directory, options, repo, alerts, elapsed: (value: number) => { elapsed = value; },
    async close() { repo.close(); vi.restoreAllMocks(); await rm(directory, { recursive: true, force: true }); } };
}
function controlBytes(directory: string): number {
  return readdirSync(directory).filter(name => /^retention(?:-v2)?\.sqlite(?:-(?:journal|wal|shm))?$/.test(name))
    .reduce((sum, name) => { const info = lstatSync(join(directory, name)); return sum + Math.max(info.size, info.blocks * 512); }, 0);
}
function manyRecords(count: number): CommitWrite {
  const seed = candidate('many-records');
  const observations = Array.from({ length: count }, (_, i) => ({ ...seed.observations[0]!,
    ref: { kind: 'observation' as const, id: `many-records:sample-${i}` }, key: `sample-${i}` }));
  return { ...seed, observations, batch: { ...seed.batch, observationRefs: observations.map(item => item.ref) },
    evidence: { ...seed.evidence, outputRefs: [seed.batch.ref, ...observations.map(item => item.ref)] } };
}

it('refuses a new identity at the operation limit, emits health, and keeps exact old identities after expiry', async () => {
  const f = await fixture();
  try {
    await f.repo.commit(candidate('a')); await f.repo.commit(candidate('b'));
    await expect(f.repo.commit(candidate('c'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(await f.repo.receipt('c')).toBeUndefined();
    expect(await f.repo.read(candidate('a').batch.ref)).toEqual(candidate('a').batch);
    f.elapsed(100); expect(await f.repo.sweep({ limit: 10 })).toEqual({ deleted: 2 });
    await expect(f.repo.commit(candidate('a'))).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
    await expect(f.repo.commit(candidate('c'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
  } finally { await f.close(); }
});

it('counts every reference membership, and a rejected intent rolls back without a native publication', async () => {
  const f = await fixture(100, 5);
  try {
    await f.repo.commit(candidate('a'));
    await expect(f.repo.commit(candidate('b'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(await f.repo.findOperation('b')).toBeUndefined();
    expect(await f.repo.metadataCapacity({ operations: 1, references: 3 })).toMatchObject({
      available: false, operations: 1, references: 3, remainingReferences: 2,
    });
  } finally { await f.close(); }
});

it('persists immutable limits, snapshots caller settings and refuses readers without the same limits', async () => {
  const f = await fixture(1);
  try {
    f.options.metadataLimits.maxOperations = 100;
    await f.repo.commit(candidate('a'));
    await expect(f.repo.commit(candidate('b'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    const changed = createRetainedFilesystemRepository(f.options)!;
    try { await expect(changed.read(candidate('a').batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); }
    finally { changed.close(); }
    const { metadataLimits: _limits, ...withoutLimits } = f.options;
    const unsupported = createRetainedFilesystemRepository(withoutLimits)!;
    try { await expect(unsupported.read(candidate('a').batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); }
    finally { unsupported.close(); }
  } finally { await f.close(); }
});

it('bounds native SQLite growth even when a single new intent exhausts its page budget', async () => {
  const f = await fixture(10000, 100000);
  let nativeError: unknown;
  const original = RepositoryLedger.prototype.intent;
  vi.spyOn(RepositoryLedger.prototype, 'intent').mockImplementation(function (this: RepositoryLedger, ...args) {
    try { original.apply(this, args); } catch (error) { nativeError = error; throw error; }
  });
  try {
    await expect(f.repo.commit(manyRecords(5000))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(nativeError).toMatchObject({ errcode: 13 }); // SQLITE_FULL, not an unrelated packing/input refusal.
    expect(await f.repo.receipt('many-records')).toBeUndefined();
    expect(controlBytes(f.directory)).toBeLessThanOrEqual(f.options.metadataLimits.maxBytes);
    expect(await f.repo.findOperation('many-records')).toBeUndefined();
    expect((await f.repo.commit(candidate('small-after-full'))).status).toBe('committed');
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});

it.each(['retention.sqlite', 'retention-v2.sqlite'])('refuses a missing required control file %s on an already open handle', async name => {
  const f = await fixture();
  try {
    await f.repo.commit(candidate('a'));
    await unlink(join(f.directory, name));
    await expect(f.repo.commit(candidate('b'))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});

it('exports the capability through the existing filesystem entry and validates capacity requests', async () => {
  const module = await import('../src/storage/filesystem.js');
  expect(module).toHaveProperty('RETAINED_REPOSITORY_SAFETY_VERSION', 1);
  const f = await fixture();
  try {
    for (const request of [{ operations: -1, references: 1 }, { operations: 1, references: 0.5 }]) {
      await expect(f.repo.metadataCapacity(request)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
    }
    expect(await f.repo.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0, available: true });
  } finally { await f.close(); }
});

it('keeps control files bounded during dirty transactions and repeated refusals without forgetting history', async () => {
  const f = await fixture(20, 1000); let peak = 0;
  const original = RepositoryLedger.prototype.intent;
  vi.spyOn(RepositoryLedger.prototype, 'intent').mockImplementation(function (this: RepositoryLedger, ...args) {
    original.apply(this, args); peak = Math.max(peak, controlBytes(f.directory));
  });
  try {
    for (let i = 0; i < 20; i++) await f.repo.commit(candidate('run-' + i));
    const before = controlBytes(f.directory);
    for (let i = 0; i < 100; i++) await expect(f.repo.commit(candidate('refused-' + i))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(controlBytes(f.directory)).toBe(before);
    expect(peak).toBeGreaterThan(0); expect(peak).toBeLessThanOrEqual(f.options.metadataLimits.maxBytes);
    expect(await f.repo.metadataCapacity()).toMatchObject({ available: false, operations: 20, references: 60 });
    f.repo.close(); const restarted = createRetainedFilesystemRepository(f.options)!;
    try { expect(await restarted.metadataCapacity()).toMatchObject({ available: false, operations: 20, references: 60 }); }
    finally { restarted.close(); }
    console.info(JSON.stringify({ fixture: '20 accepted /100 refused, three refs per run', settledControlBytes: before, observedDirtyPeakBytes: peak,
      maxMetadataBytes: f.options.metadataLimits.maxBytes, retainedOperationIdentities: 20, retainedReferenceMemberships: 60 }));
  } finally { await f.close(); }
});

it.each([{ maxBytes: 1 }, { maxBytes: Infinity }, { maxOperations: 0 }, { maxReferences: -1 }, { maxReferences: 1.5 }])('refuses invalid metadata limits %j', async part => {
  const f = await fixture();
  try { expect(() => createRetainedFilesystemRepository({ ...f.options, metadataLimits: { ...f.options.metadataLimits, ...part } })).toThrow(); }
  finally { await f.close(); }
});
