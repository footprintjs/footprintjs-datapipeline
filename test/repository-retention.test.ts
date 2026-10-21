import { mkdtemp, readdir, rm, realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { afterEach, expect, it } from 'vitest';
import { createFilesystemRepository } from '../src/storage/filesystem.js';
import { createRetainedFilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';
const paths: string[] = [];
afterEach(async () => { for (const p of paths.splice(0)) await rm(p, { recursive: true, force: true }); });
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'repository-retention-'))); paths.push(directory);
  let time = 1000;
  const options = { enabled: true, directory, retentionMs: 100, clock: () => time, requireSeparateFilesystem: false, onHealth() {} };
  return { directory, options, at: (value: number) => { time = value; } };
}
it('is absent by default, without creating a namespace', async () => {
  const f = await fixture();
  expect(createRetainedFilesystemRepository({ ...f.options, enabled: false })).toBeUndefined();
  expect(await readdir(f.directory)).toEqual([]);
});
it('reopens exact packed records and receipts, then expires them by the fixed clock without rewriting them', async () => {
  const f = await fixture(), write = candidate();
  const repo = createRetainedFilesystemRepository(f.options)!;
  const saved = await repo.commit(write);
  expect(saved.status).toBe('committed');
  expect(await repo.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
  const before = await repo.receipt(write.operationId); repo.close();
  const reader = createRetainedFilesystemRepository(f.options)!;
  expect((await reader.findOperation(write.operationId))?.receipt).toEqual(saved);
  expect(await reader.receipt(write.operationId)).toEqual(before);
  f.at(1100);
  await expect(reader.read(write.observations[0]!.ref)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
  await expect(reader.findOperation(write.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
  await expect(reader.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
  expect(await reader.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  expect(await reader.receipt(write.operationId)).toMatchObject({ status: 'expired', dataDeleted: true });
  reader.close();
  const restarted = createRetainedFilesystemRepository(f.options)!;
  await expect(restarted.read(write.observations[0]!.ref)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
  restarted.close();
});
it('bounds cleanup and preserves fresh records and unknown versus expired', async () => {
  const f = await fixture(), repo = createRetainedFilesystemRepository(f.options)!, a = candidate('a');
  await repo.commit(a); f.at(1050); const b = candidate('b'); await repo.commit(b); f.at(1100);
  expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  expect(await repo.read(b.batch.ref)).toEqual(b.batch);
  expect(await repo.read({kind:'observation',id:'never-collected'})).toBeUndefined();
  await expect(repo.sweep({ limit: 101 })).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
  repo.close();
});
it('does not invent ages for an unmanaged namespace or silently change policy', async () => {
  const f = await fixture(); await createFilesystemRepository({ directory: f.directory }).commit(candidate());
  const refused = createRetainedFilesystemRepository(f.options)!;
  await expect(refused.read(candidate().batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_UNMANAGED' }); refused.close();
  const g = await fixture(), original = createRetainedFilesystemRepository(g.options)!; await original.commit(candidate()); original.close();
  const changed = createRetainedFilesystemRepository({ ...g.options, retentionMs: 200 })!;
  await expect(changed.findOperation('collect:one')).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); changed.close();
});
it('never resurrects a fully expired reference in a different operation', async () => {
  const f = await fixture(), repo = createRetainedFilesystemRepository(f.options)!, a = candidate('a');
  await repo.commit(a); f.at(1100); await repo.sweep({ limit: 10 });
  const seed = candidate('b');
  const b = { ...seed, observations: [{ ...seed.observations[0]!, ref: a.observations[0]!.ref }],
    batch: { ...seed.batch, observationRefs: [a.observations[0]!.ref] },
    evidence: { ...seed.evidence, outputRefs: [seed.batch.ref, a.observations[0]!.ref] } };
  await expect(repo.commit(b)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' }); repo.close();
});
