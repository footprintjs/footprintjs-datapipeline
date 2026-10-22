import { chmod, mkdtemp, readFile, realpath, rename, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { createSealedFilesystemHistory } from '../src/storage/sealedHistory/index.js';
import { sealedSnapshot } from './helpers/sealed-history-fixture.js';

const fault = vi.hoisted(() => ({ path: '', code: '', owner: false, swapped: false, short: false, at: 'open', markerSource: '', directory: '' }));
vi.mock('node:fs/promises', async original => {
  const fs = await original<typeof import('node:fs/promises')>();
  return { ...fs,
    async readdir(...args: Parameters<typeof fs.readdir>) {
      if (String(args[0]) === fault.directory && fault.markerSource) {
        const source = fault.markerSource; fault.markerSource = '';
        await fs.link(source, join(fault.directory, 'retention.sqlite'));
      }
      return fs.readdir(...args);
    },
    async lstat(...args: Parameters<typeof fs.lstat>) {
      if (fault.at === 'lstat' && String(args[0]) === fault.path && fault.code) throw Object.assign(Error('private/path/token'), { code: fault.code });
      return fs.lstat(...args);
    },
    async open(...args: Parameters<typeof fs.open>) {
      if (fault.at === 'open' && String(args[0]) === fault.path && fault.code) throw Object.assign(Error('private/path/token'), { code: fault.code });
      const handle = await fs.open(...args);
      if (String(args[0]) !== fault.path || !(fault.owner || fault.swapped || fault.short)) return handle;
      return new Proxy(handle, { get(target, key) {
        if (key === 'stat') return async () => {
          const info = await target.stat();
          return Object.assign(info, { uid: info.uid + (fault.owner ? 1 : 0), ino: info.ino + (fault.swapped ? 1 : 0) });
        };
        if (key === 'read' && fault.short) return async () => ({ bytesRead: 0 });
        const value = Reflect.get(target, key, target) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      } });
    },
  };
});
const roots: string[] = [];
afterEach(async () => {
  fault.path = ''; fault.code = ''; fault.owner = false; fault.swapped = false; fault.short = false;
  fault.at = 'open'; fault.markerSource = ''; fault.directory = ''; vi.restoreAllMocks();
  for (const path of roots.splice(0)) await rm(path, { recursive: true, force: true });
});
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'sealed-io-'))); roots.push(directory);
  const store = createSealedFilesystemHistory({ enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false })!;
  const saved = await store.publish(sealedSnapshot());
  return { directory, store, path: join(directory, 'snapshots', saved.sha256 + '.bin') };
}
it.each(['EIO', 'EMFILE'])('reports a read refused by %s as unavailable with fixed text', async code => {
  const f = await fixture(); fault.path = f.path; fault.code = code;
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE', message: 'Sealed history operation was refused.' });
});
it.each(['EACCES', 'EPERM'])('reports a file read denied by %s as unmanaged with fixed text', async code => {
  const f = await fixture(); fault.path = f.path; fault.code = code;
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED', message: 'Sealed history operation was refused.' });
});
it.each(['EACCES', 'EPERM'])('reports a directory boundary denied by %s as unmanaged', async code => {
  const f = await fixture(); fault.path = f.directory; fault.code = code; fault.at = 'lstat';
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
});
it('reports unreadable snapshot, group-readable catalogue and inaccessible directory permissions as unmanaged', async () => {
  const f = await fixture(); await chmod(f.path, 0o000);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  await chmod(f.path, 0o600); await chmod(join(f.directory, 'catalogue.json'), 0o640);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  await chmod(join(f.directory, 'catalogue.json'), 0o600); await chmod(f.directory, 0o000);
  try { await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' }); }
  finally { await chmod(f.directory, 0o700); }
});
it('reports changed file ownership as unmanaged', async () => {
  const f = await fixture(); fault.path = f.path; fault.owner = true;
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
});
it('reports a swapped file inode as unavailable before interpreting its bytes', async () => {
  const f = await fixture(); fault.path = f.path; fault.swapped = true;
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
});
it('reports a short read as unavailable rather than corrupt', async () => {
  const f = await fixture(); fault.path = f.path; fault.short = true;
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
});
it.each([3, 23])('reports SQLite permission/auth error %s as unmanaged', async errcode => {
  const f = await fixture();
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  vi.spyOn(DatabaseSync.prototype, 'prepare').mockImplementation(() => { throw Object.assign(Error('private database error'), { errcode }); });
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED', message: 'Sealed history operation was refused.' });
});
it('rechecks a competing marker linked between the missing read and folder listing', async () => {
  const f = await fixture(), outside = await realpath(await mkdtemp(join(tmpdir(), 'sealed-marker-race-'))); roots.push(outside);
  const source = join(outside, 'marker.sqlite'); await rename(join(f.directory, 'retention.sqlite'), source);
  fault.directory = f.directory; fault.markerSource = source;
  expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
  expect(fault.markerSource).toBe('');
});
it('reports a busy SQLite marker as unavailable', async () => {
  const f = await fixture();
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  const db = new DatabaseSync(join(f.directory, 'retention.sqlite'));
  try {
    db.exec('BEGIN EXCLUSIVE');
    await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  } finally { db.exec('ROLLBACK'); db.close(); }
});
it('reports invalid SQLite bytes and changed snapshot bytes as corrupt after reading them', async () => {
  const f = await fixture(), original = await readFile(f.path);
  const changed = Buffer.from(original); changed[64] = changed[64]! ^ 1;
  await writeFile(f.path, changed);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_CORRUPT' });
  await writeFile(f.path, original); await writeFile(join(f.directory, 'retention.sqlite'), 'invalid SQLite bytes');
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_CORRUPT' });
});
