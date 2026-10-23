import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createRequire } from 'node:module';
import { setImmediate as yieldTurn } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { once } from 'node:events';
import { afterEach, expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { RepositoryLedger } from '../src/storage/retainedRepository/ledger.js';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

const deletion = vi.hoisted(() => ({ hold: undefined as (() => Promise<void>) | undefined }));
vi.mock('node:fs/promises', async original => {
  const fs = await original<typeof import('node:fs/promises')>();
  return { ...fs, unlink: async (...args: Parameters<typeof fs.unlink>) => {
    if (String(args[0]).endsWith('/operation.json.gz')) await deletion.hold?.();
    return fs.unlink(...args);
  } };
});
afterEach(() => { deletion.hold = undefined; vi.restoreAllMocks(); });

async function fixture(bounded = false) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'repository-contention-')));
  let now = 0;
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false,
    clock: () => now, onHealth: (event: unknown) => { alerts.push(event); },
    ...(bounded ? { expiryClock: 'verified-uptime' as const, monotonicClock: () => now,
      metadataLimits: { maxBytes: 2 * 1024 * 1024, maxOperations: 100, maxReferences: 1000 } } : {}) };
  const stores: ReturnType<typeof createRetainedFilesystemRepository>[] = [];
  function open() { const store = createRetainedFilesystemRepository(options)!; stores.push(store); return store; }
  return { directory, options, alerts, open, at: (value: number) => { now = value; },
    async close() { for (const store of stores) store!.close(); await rm(directory, { recursive: true, force: true }); } };
}

it.each([false, true])('publishes seven polls while real deletion is still blocked (bounded=%s)', async bounded => {
  const f = await fixture(bounded), cleanup = f.open(), pollers = Array.from({ length: 7 }, () => f.open());
  const old = candidate('expired-before-polls');
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const deleting = new Promise<void>(resolve => { entered = resolve; });
  let sweep: Promise<unknown> | undefined;
  const polls: Promise<unknown>[] = [];
  try {
    await cleanup.commit(old);
    for (const poller of pollers) await poller.receipt(old.operationId);
    f.at(100);
    deletion.hold = async () => { entered(); await gate; };
    sweep = cleanup.sweep({ limit: 1 });
    await deleting;
    for (let i = 0; i < 7; i++) polls.push(pollers[i]!.commit(candidate('poll-' + i)));
    // Do not release deletion to make admission pass. Disk I/O can exceed any
    // fixed admission budget; completed poll receipts must precede its release.
    const observed = polls.map(poll => poll.then(value => ({ value }), error => ({ error })));
    const outcomes = await Promise.all(observed);
    expect(outcomes.every(result => 'value' in result)).toBe(true);
    for (let i = 0; i < 7; i++) {
      const write = candidate('poll-' + i);
      expect(await pollers[i]!.receipt(write.operationId)).toMatchObject({ status: 'stored', publication: 'committed' });
      expect(await pollers[i]!.read(write.batch.ref)).toEqual(write.batch);
    }
    expect(await pollers[0]!.receipt(old.operationId)).toMatchObject({ status: 'expired', dataDeleted: false });
    const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
    const observer = new DatabaseSync(join(f.directory, bounded ? 'retention-v2.sqlite' : 'retention.sqlite'));
    try { observer.exec('BEGIN IMMEDIATE; ROLLBACK'); } finally { observer.close(); }
    release();
    expect(await sweep).toEqual({ deleted: 1 });
    expect(await cleanup.receipt(old.operationId)).toMatchObject({ status: 'expired', dataDeleted: true });
    expect(f.alerts).toEqual([]);
  } finally {
    release(); await Promise.allSettled([...polls, ...(sweep ? [sweep] : [])]); await f.close();
  }
});

it('keeps close and overlapping cleanup bounded while allowing a same-handle publication', async () => {
  const f = await fixture(), repo = f.open();
  let release!: () => void, entered!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  const deleting = new Promise<void>(resolve => { entered = resolve; });
  let sweep: Promise<unknown> | undefined, overlap: Promise<unknown> | undefined, saved: Promise<unknown> | undefined;
  try {
    await repo.commit(candidate('old')); f.at(100);
    deletion.hold = async () => { entered(); await gate; };
    sweep = repo.sweep({ limit: 1 }); await deleting;
    expect(() => repo.close()).toThrow(expect.objectContaining({ code: 'REPOSITORY_BUSY' }));
    let refused = false;
    overlap = repo.sweep({ limit: 1 }).then(() => {}, error => {
      expect(error).toMatchObject({ code: 'REPOSITORY_BUSY' }); refused = true;
    });
    await yieldTurn(); expect(refused).toBe(true);
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_BUSY' });
    saved = repo.commit(candidate('same-handle'));
    expect(await saved).toMatchObject({ status: 'committed' });
    release(); expect(await sweep).toEqual({ deleted: 1 });
    expect(await repo.receipt('same-handle')).toMatchObject({ status: 'stored', publication: 'committed' });
  } finally {
    release(); await Promise.allSettled([...(sweep ? [sweep] : []), ...(overlap ? [overlap] : []), ...(saved ? [saved] : [])]); await f.close();
  }
});

it('also waits during first-open schema admission and lets the event loop release an external lock', async () => {
  const f = await fixture(), first = f.open(), write = candidate('initial');
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let db: InstanceType<typeof DatabaseSync> | undefined, timer: NodeJS.Timeout | undefined;
  try {
    await first.commit(write);
    db = new DatabaseSync(join(f.directory, 'retention.sqlite'));
    db.exec('BEGIN IMMEDIATE');
    let released = false;
    timer = setTimeout(() => { db!.exec('ROLLBACK'); released = true; }, 40);
    expect(await f.open().read(write.batch.ref)).toEqual(write.batch);
    expect(released).toBe(true);
    expect(f.alerts).toEqual([]);
  } finally { clearTimeout(timer); if (db?.isTransaction) db.exec('ROLLBACK'); db?.close(); await f.close(); }
});

it('does not retry native publication after it has begun, even when the error is labelled busy', async () => {
  const f = await fixture(), repo = f.open();
  const native = vi.spyOn(FilesystemRepository.prototype, 'commit').mockRejectedValue(Object.assign(Error('private'), { errcode: 5 }));
  try {
    await expect(repo.commit(candidate('uncertain'))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(native).toHaveBeenCalledTimes(1);
    expect(await repo.receipt('uncertain')).toMatchObject({ status: 'pending', publication: 'unconfirmed' });
  } finally { await f.close(); }
});

it('allows a different worker holding the database to release it while a save waits', async () => {
  const f = await fixture(), repo = f.open();
  let worker: Worker | undefined;
  try {
    await repo.commit(candidate('before-worker'));
    worker = new Worker(`
      const { parentPort, workerData } = require('node:worker_threads');
      const { DatabaseSync } = require('node:sqlite');
      const db = new DatabaseSync(workerData);
      db.exec('BEGIN IMMEDIATE'); parentPort.postMessage('locked');
      parentPort.once('message', () => { db.exec('ROLLBACK'); db.close(); parentPort.close(); });
    `, { eval: true, workerData: join(f.directory, 'retention.sqlite') });
    expect((await once(worker, 'message'))[0]).toBe('locked');
    let blocked!: () => void;
    const busy = new Promise<void>(resolve => { blocked = resolve; });
    const begin = RepositoryLedger.prototype.begin;
    vi.spyOn(RepositoryLedger.prototype, 'begin').mockImplementation(function (this: RepositoryLedger) {
      try { begin.call(this); } catch (error) { blocked(); throw error; }
    });
    const save = repo.commit(candidate('while-worker-owns-lock'));
    const outcome = save.then(value => ({ value }), error => ({ error }));
    await busy;
    worker.postMessage('release');
    expect(await outcome).toMatchObject({ value: { status: 'committed' } });
    expect(await repo.receipt('while-worker-owns-lock')).toMatchObject({ publication: 'committed' });
  } finally { await worker?.terminate(); await f.close(); }
});

it('bounds a persistent lock and reports a fixed busy alert without publishing or inventing a receipt', async () => {
  const f = await fixture(), repo = f.open();
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let db: InstanceType<typeof DatabaseSync> | undefined;
  try {
    await repo.commit(candidate('before-persistent-lock'));
    db = new DatabaseSync(join(f.directory, 'retention.sqlite')); db.exec('BEGIN IMMEDIATE');
    const native = vi.spyOn(FilesystemRepository.prototype, 'commit');
    await expect(repo.commit(candidate('not-admitted'))).rejects.toMatchObject({ code: 'REPOSITORY_BUSY' });
    expect(native).not.toHaveBeenCalled();
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_BUSY' }]);
    db.exec('ROLLBACK');
    expect(await repo.receipt('not-admitted')).toBeUndefined();
    expect((await repo.commit(candidate('after-release'))).status).toBe('committed');
  } finally { if (db?.isTransaction) db.exec('ROLLBACK'); db?.close(); await f.close(); }
});
