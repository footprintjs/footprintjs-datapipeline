import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { afterEach, expect, it, vi } from 'vitest';
import { createFilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

const fault = vi.hoisted(() => ({ corruptWrite: false, rename: false, syncAfterRename: false, publishedParent: '' }));
vi.mock('node:fs/promises', async importOriginal => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return {
    ...actual,
    rename: async (...args: Parameters<typeof actual.rename>) => {
      if (fault.rename) throw Object.assign(new Error('Injected publish failure'), { code: 'EIO' });
      await actual.rename(...args);
      fault.publishedParent = dirname(String(args[1]));
    },
    open: async (...args: Parameters<typeof actual.open>) => {
      const handle = await actual.open(...args), path = String(args[0]);
      return new Proxy(handle, { get(target, key) {
        if (key === 'writeFile' && path.endsWith('/operation.json.gz') && fault.corruptWrite) return () => target.writeFile('damaged');
        if (key === 'sync' && fault.syncAfterRename && path === fault.publishedParent) return async () => { throw Error('Injected directory sync failure'); };
        const value = Reflect.get(target, key, target) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      } });
    },
  };
});
const paths: string[] = [];
async function directory() { const path = await mkdtemp(join(tmpdir(), 'packed-publish-')); paths.push(path); return path; }
afterEach(async () => {
  Object.assign(fault, { corruptWrite: false, rename: false, syncAfterRename: false, publishedParent: '' });
  for (const path of paths.splice(0)) await rm(path, { recursive: true, force: true });
});

it.each(['corruptWrite', 'rename'] as const)('does not publish or acknowledge a %s failure', async mode => {
  const path = await directory(), write = candidate();
  const repo = createFilesystemRepository({ directory: path, packing: 'gzip' });
  fault[mode] = true;
  await expect(repo.commit(write)).rejects.toThrow();
  fault[mode] = false;
  expect(await repo.findOperation(write.operationId)).toBeUndefined();
  expect(await repo.read(write.batch.ref)).toBeUndefined();
  expect(await readdir(join(path, 'operations'))).toEqual([]);
  expect((await repo.commit(write)).status).toBe('committed');
});

it('publication followed by sync failure is unknown; restart reconciles the durable candidate', async () => {
  const path = await directory(), write = candidate();
  fault.syncAfterRename = true;
  const uncertain = await createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write);
  expect(uncertain).toMatchObject({ status: 'unknown', error: { code: 'commit_sync_unknown' } });
  fault.syncAfterRename = false;
  const restarted = createFilesystemRepository({ directory: path });
  const saved = await restarted.findOperation(write.operationId);
  expect(saved?.receipt.status).toBe('committed');
  expect(await restarted.commit(candidate(write.operationId, write.fingerprint, 999))).toEqual(saved?.receipt);
  expect(await restarted.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
});
