import { mkdtemp, realpath, rm, readdir, symlink } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { expect, it, vi } from 'vitest';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { candidate } from './helpers/filesystem-candidate.js';
const fault = vi.hoisted(() => ({ remove: false, scans: 0, beforeDelete: undefined as (() => void) | undefined }));
vi.mock('node:fs/promises', async original => {
  const fs = await original<typeof import('node:fs/promises')>();
  return { ...fs, readdir: async (...args: Parameters<typeof fs.readdir>) => { if (String(args[0]).endsWith('/operations')) fault.scans++; return fs.readdir(...args); }, unlink: async (...args: Parameters<typeof fs.unlink>) => {
    fault.beforeDelete?.();
    if (fault.remove) throw Object.assign(Error('private path'), { code: 'EIO' });
    return fs.unlink(...args);
  } };
});
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'retained-fault-')));
  let now = 0; const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false, clock: () => now, onHealth: (event: unknown) => { alerts.push(event); } };
  const repo = createRetainedFilesystemRepository(options)!;
  return { directory, options, repo, alerts, at: (value: number) => { now = value; }, async close() { repo.close(); vi.restoreAllMocks(); fault.remove = false; fault.beforeDelete = undefined; await rm(directory, { recursive: true, force: true }); } };
}
it('recovers native publication after metadata acknowledgement fails, without writing again', async () => {
  const f = await fixture(), write = candidate();
  const original = FilesystemRepository.prototype.commit;
  vi.spyOn(FilesystemRepository.prototype, 'commit').mockImplementation(async function (this: FilesystemRepository, input) { await original.call(this, input); throw Error('crash after publication'); });
  try {
    await expect(f.repo.commit(write)).rejects.toMatchObject({code:'REPOSITORY_UNCONFIRMED'});
    expect(await f.repo.receipt(write.operationId)).toMatchObject({status:'pending',publication:'unconfirmed'});
    vi.restoreAllMocks(); f.repo.close();
    const restarted = createRetainedFilesystemRepository(f.options)!;
    expect((await restarted.findOperation(write.operationId))?.receipt.status).toBe('committed');
    expect(await restarted.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
    expect(await restarted.receipt(write.operationId)).toMatchObject({status:'stored',publication:'committed',acceptedAt:0,expiresAt:100});
    restarted.close();
  } finally { await f.close(); }
});
it('saves the expired tombstone before deletion, retains failures and retries cleanup safely', async () => {
  const f = await fixture(), write = candidate();
  try {
    await f.repo.commit(write); f.at(100);
    const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
    let checked = false;
    fault.beforeDelete = () => {
      const db = new DatabaseSync(join(f.directory,'retention.sqlite'));
      try { expect(db.prepare('SELECT status FROM operations').get()!.status).toBe('expired'); checked = true; } finally { db.close(); }
    };
    fault.remove = true;
    await expect(f.repo.sweep({limit:1})).rejects.toMatchObject({code:'REPOSITORY_UNCONFIRMED'});
    expect(checked).toBe(true);
    expect(await f.repo.receipt(write.operationId)).toMatchObject({status:'expired',dataDeleted:false});
    expect(f.alerts).toContainEqual({code:'REPOSITORY_UNCONFIRMED'});
    f.at(0); await expect(f.repo.read(write.batch.ref)).rejects.toMatchObject({code:'REPOSITORY_EXPIRED'});
    fault.remove = false; fault.beforeDelete = undefined;
    expect(await f.repo.sweep({limit:1})).toEqual({deleted:1});
    expect(await f.repo.sweep({limit:1})).toEqual({deleted:0});
  } finally { await f.close(); }
});
it('serializes cross-instance publication and refuses malformed retention metadata', async () => {
  const f = await fixture(), write = candidate();
  try {
    await f.repo.commit(write);
    const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
    const db = new DatabaseSync(join(f.directory,'retention.sqlite'));
    try {
      db.exec('BEGIN IMMEDIATE');
      const other = createRetainedFilesystemRepository(f.options)!;
      await expect(other.read(write.batch.ref)).rejects.toMatchObject({code:'REPOSITORY_BUSY'}); other.close();
      db.exec('ROLLBACK');
      db.exec("UPDATE operations SET expiresAt='invalid'");
      await expect(f.repo.read(write.batch.ref)).rejects.toMatchObject({code:'REPOSITORY_UNCONFIRMED'});
    } finally { db.close(); }
  } finally { await f.close(); }
});
it('refuses symlinked operations after initialization without deleting an outside target', async () => {
  const f = await fixture(), write = candidate(), outside = await realpath(await mkdtemp(join(tmpdir(),'retained-outside-')));
  try {
    await f.repo.commit(write); f.at(100);
    const hash = createHash('sha256').update(write.operationId).digest('hex');
    await rm(join(f.directory,'operations',hash),{recursive:true});
    await symlink(outside,join(f.directory,'operations',hash));
    await expect(f.repo.sweep({limit:1})).rejects.toMatchObject({code:'REPOSITORY_UNCONFIRMED'});
    expect(await readdir(outside)).toEqual([]);
  } finally { await f.close(); await rm(outside,{recursive:true}); }
});

it('uses the durable reference index instead of scanning historical operations for each point', async () => {
  const f = await fixture();
  try {
    const writes = Array.from({length:20},(_,i)=>candidate('indexed-'+i));
    for (const write of writes) await f.repo.commit(write);
    fault.scans = 0;
    expect(await f.repo.readMany(writes.map(write=>write.observations[0]!.ref))).toEqual(writes.map(write=>write.observations[0]));
    await f.repo.commit(candidate('indexed-new'));
    expect(fault.scans).toBe(0);
  } finally { await f.close(); }
});
it('does not acknowledge data that expires during publication, and excludes cleanup while it is in flight', async () => {
  const f = await fixture();
  let release!:()=>void, started!:()=>void;
  const gate = new Promise<void>(resolve=>{release=resolve;});
  const ready = new Promise<void>(resolve=>{started=resolve;});
  const original = FilesystemRepository.prototype.commit;
  vi.spyOn(FilesystemRepository.prototype,'commit').mockImplementation(async function(this:FilesystemRepository,write){started();await gate;return original.call(this,write);});
  try {
    const pending = f.repo.commit(candidate());
    await ready; f.at(100);
    const other = createRetainedFilesystemRepository(f.options)!;
    await expect(other.sweep({limit:1})).rejects.toMatchObject({code:'REPOSITORY_BUSY'}); other.close();
    release(); await expect(pending).rejects.toMatchObject({code:'REPOSITORY_EXPIRED'});
    expect(await f.repo.receipt('collect:one')).toMatchObject({status:'expired',dataDeleted:false});
  } finally { release(); await f.close(); }
});

it('expiry preserves publication uncertainty when native data was never acknowledged', async () => {
  const f = await fixture(), write = candidate();
  vi.spyOn(FilesystemRepository.prototype,'commit').mockRejectedValue(Error('crash before publication'));
  try {
    await expect(f.repo.commit(write)).rejects.toMatchObject({code:'REPOSITORY_UNCONFIRMED'});
    f.at(100); expect(await f.repo.sweep({limit:1})).toEqual({deleted:1});
    expect(await f.repo.receipt(write.operationId)).toMatchObject({status:'expired',publication:'unconfirmed',dataDeleted:true});
  } finally { await f.close(); }
});
