import { mkdtemp, realpath, rm, symlink, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { afterEach, expect, it, vi } from 'vitest';
import { createSealedFilesystemHistory } from '../src/storage/filesystem.js';
import { operationHash } from '../src/storage/retainedRepository/commit.js';
import { digest } from '../src/validation.js';
const fault = vi.hoisted(() => ({ cut: '', linked: false, markerLinked: false }));
vi.mock('node:fs/promises', async original => {
  const fs = await original<typeof import('node:fs/promises')>();
  const fail = () => { throw Object.assign(Error('private path or credential'), { code: 'EIO' }); };
  return { ...fs,
    async link(...args: Parameters<typeof fs.link>) {
      const target = String(args[1]), catalogue = target.endsWith('/catalogue.json');
      const marker = target.endsWith('/retention.sqlite');
      if (fault.cut === 'marker-before' && marker) fail();
      if (fault.cut === 'snapshot-link' && target.endsWith('.bin')) fail();
      if (fault.cut === 'catalogue-before' && catalogue) fail();
      await fs.link(...args);
      if (marker) fault.markerLinked = true;
      if (fault.cut === 'marker-after' && marker) fail();
      if (catalogue) fault.linked = true;
      if (fault.cut === 'catalogue-after' && catalogue) fail();
    },
    async open(...args: Parameters<typeof fs.open>) {
      const handle = await fs.open(...args), path = String(args[0]);
      return new Proxy(handle, { get(target, key) {
        if (key === 'sync') return async () => {
          if (fault.cut === 'marker-sync' && path.includes('/.marker-')) fail();
          if (fault.cut === 'marker-directory' && fault.markerLinked && !path.includes('/.marker-')) fail();
          if (fault.cut === 'snapshot-sync' && path.includes('/snapshots/.stage-')) fail();
          if (fault.cut === 'catalogue-sync' && fault.linked && !path.includes('/snapshots') && !path.includes('/.stage-') && !path.includes('/.marker-')) fail();
          return target.sync();
        };
        const value = Reflect.get(target, key, target) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      } });
    },
  };
});
const roots: string[] = [];
afterEach(async () => { fault.cut = ''; fault.linked = false; fault.markerLinked = false; for (const path of roots.splice(0)) await rm(path, { recursive: true, force: true }); });
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'sealed-fault-'))); roots.push(directory);
  const store = createSealedFilesystemHistory({ enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false })!;
  const row = { operationHash: operationHash('one'), fingerprint: digest('fingerprint'), writeHash: digest('write'),
    acceptedAt: 0, expiresAt: 100, status: 'expired' as const, publication: 'committed' as const, dataDeleted: true };
  return { directory, store, snapshot: { operations: [row], references: [{ refHash: digest({ kind: 'batch', id: 'one' }), operationHash: row.operationHash }] } };
}
it.each(['marker-sync', 'marker-before', 'marker-after', 'marker-directory', 'snapshot-sync', 'snapshot-link', 'catalogue-before', 'catalogue-after', 'catalogue-sync'])('recovers a publication cut at %s without acknowledging uncertain durability', async cut => {
  const f = await fixture(); fault.cut = cut;
  await expect(f.store.publish(f.snapshot)).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNCONFIRMED', message: 'Sealed history operation was refused.' });
  fault.cut = '';
  if (cut === 'catalogue-after' || cut === 'catalogue-sync') expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
  else await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  const saved = await f.store.publish(f.snapshot);
  expect(await f.store.publish(f.snapshot)).toEqual(saved);
  expect((await f.store.open()).lookupReference({ kind: 'batch', id: 'one' })).toEqual(f.snapshot.operations[0]);
});
it('refuses an unmarked orphan after a process crash, requiring operator inspection rather than adoption', async () => {
  const f = await fixture(); await writeFile(join(f.directory, '.marker-interrupted'), 'interrupted marker', { mode: 0o600 });
  await expect(f.store.publish(f.snapshot)).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
});
it('refuses missing published data as corrupt, not an empty snapshot', async () => {
  const f = await fixture(), saved = await f.store.publish(f.snapshot);
  await rm(join(f.directory, 'snapshots', saved.sha256 + '.bin'));
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_CORRUPT' });
});
it('refuses linked catalogues and snapshots, leaving outside data intact', async () => {
  const f = await fixture(), saved = await f.store.publish(f.snapshot);
  const outside = await realpath(await mkdtemp(join(tmpdir(), 'sealed-outside-'))); roots.push(outside);
  const path = join(f.directory, 'snapshots', saved.sha256 + '.bin'), bytes = await readFile(path);
  const target = join(outside, 'private'); await writeFile(target, bytes, { mode: 0o600 });
  await rm(path); await symlink(target, path);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_CORRUPT' });
  expect(await readFile(target)).toEqual(bytes);
});
