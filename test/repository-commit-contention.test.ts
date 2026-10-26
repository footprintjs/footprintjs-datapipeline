import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { RepositoryLedger } from '../src/storage/retainedRepository/ledger.js';
import { ElapsedAge } from '../src/storage/retainedRepository/elapsedAge.js';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

afterEach(() => { vi.restoreAllMocks(); });

async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'repository-commit-busy-')));
  let elapsed = 0;
  const alerts: unknown[] = [];
  const repo = createRetainedFilesystemRepository({ enabled: true, directory,
    retentionMs: 100, expiryClock: 'verified-uptime', requireSeparateFilesystem: false,
    clock: () => 1000, monotonicClock: () => elapsed, onHealth: event => { alerts.push(event); } })!;
  await repo.commit(candidate('seed'));
  // A real second connection's SHARED lock permits BEGIN IMMEDIATE and writes,
  // but prevents DELETE-journal COMMIT from acquiring its final exclusive lock.
  const reader = new DatabaseSync(join(directory, 'retention-v2.sqlite'));
  const hold = () => { reader.exec('BEGIN'); reader.prepare('SELECT * FROM elapsed_clock').all(); };
  const release = () => { if (reader.isTransaction) reader.exec('ROLLBACK'); };
  return { directory, repo, reader, hold, release, alerts, at: (age: number) => { elapsed = age; },
    async close() { release(); reader.close(); repo.close(); await rm(directory, { recursive: true, force: true }); } };
}

it.each(['intent', 'publication', 'expiry', 'deletion'] as const)(
  'waits for a real final-COMMIT lock during %s without replaying work', async phase => {
    const f = await fixture();
    let armed = true, busy = 0;
    const arm = () => { if (armed) { armed = false; f.hold(); } };
    const exec = DatabaseSync.prototype.exec;
    vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: DatabaseSync, sql: string) {
      try { return exec.call(this, sql); }
      catch (error) {
        if (sql === 'COMMIT' && (error as { errcode?: number }).errcode === 5) {
          busy++;
          queueMicrotask(f.release);
        }
        throw error;
      }
    });
    const native = FilesystemRepository.prototype.commit;
    const publish = vi.spyOn(FilesystemRepository.prototype, 'commit').mockImplementation(async function (this: FilesystemRepository, write) {
      const result = await native.call(this, write);
      if (phase === 'publication') arm();
      return result;
    });
    const intent = RepositoryLedger.prototype.intent;
    const intents = vi.spyOn(RepositoryLedger.prototype, 'intent').mockImplementation(function (this: RepositoryLedger, ...args) {
      intent.apply(this, args); if (phase === 'intent') arm();
    });
    const expired = RepositoryLedger.prototype.expired;
    const expires = vi.spyOn(RepositoryLedger.prototype, 'expired').mockImplementation(function (this: RepositoryLedger, hash) {
      expired.call(this, hash); if (phase === 'expiry') arm();
    });
    const deleted = RepositoryLedger.prototype.deleted;
    const deletes = vi.spyOn(RepositoryLedger.prototype, 'deleted').mockImplementation(function (this: RepositoryLedger, hash) {
      deleted.call(this, hash); if (phase === 'deletion') arm();
    });
    const checkpoints = vi.spyOn(ElapsedAge.prototype, 'checkpoint');
    try {
      if (phase === 'intent' || phase === 'publication') {
        expect(await f.repo.commit(candidate('new'))).toMatchObject({ status: 'committed' });
        expect(intents).toHaveBeenCalledTimes(1);
        expect(publish).toHaveBeenCalledTimes(1);
        expect(checkpoints).toHaveBeenCalledTimes(2); // one per transaction, not per retry
        expect(await f.repo.read(candidate('new').batch.ref)).toEqual(candidate('new').batch);
      } else {
        f.at(100);
        if (phase === 'expiry') {
          await expect(f.repo.readMany([candidate('seed').batch.ref])).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
          expect(expires).toHaveBeenCalledTimes(1);
          expect(checkpoints).toHaveBeenCalledTimes(1);
        }
        expect(await f.repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
        expect(deletes).toHaveBeenCalledTimes(1);
        expect(await f.repo.receipt('seed')).toMatchObject({ status: 'expired', dataDeleted: true });
        expect(publish).not.toHaveBeenCalled();
      }
      expect(busy).toBeGreaterThan(0);
      expect(f.alerts).toEqual([]);
    } finally { await f.close(); }
  });

it('rolls back an expired-mark transaction when final COMMIT stays busy, preserving the reason and handle', async () => {
  const f = await fixture();
  const expired = RepositoryLedger.prototype.expired;
  vi.spyOn(RepositoryLedger.prototype, 'expired').mockImplementation(function (this: RepositoryLedger, hash) {
    expired.call(this, hash); f.hold();
  });
  try {
    f.at(100);
    await expect(f.repo.readMany([candidate('seed').batch.ref])).rejects.toMatchObject({ code: 'REPOSITORY_BUSY' });
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_BUSY' }]);
    f.release(); vi.restoreAllMocks();
    expect(await f.repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
    expect(await f.repo.receipt('seed')).toMatchObject({ status: 'expired', dataDeleted: true });
  } finally { await f.close(); }
});

it('does not retry or expose a non-busy final-COMMIT error', async () => {
  const f = await fixture();
  const exec = DatabaseSync.prototype.exec;
  let commits = 0;
  vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: DatabaseSync, sql: string) {
    if (sql === 'COMMIT') { commits++; throw Object.assign(Error('private disk failure'), { errcode: 10 }); }
    return exec.call(this, sql);
  });
  const publish = vi.spyOn(FilesystemRepository.prototype, 'commit');
  try {
    await expect(f.repo.commit(candidate('failed'))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(commits).toBe(1); expect(publish).not.toHaveBeenCalled();
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
    vi.restoreAllMocks();
    expect(await f.repo.receipt('failed')).toBeUndefined();
    expect(await f.repo.commit(candidate('healthy'))).toMatchObject({ status: 'committed' });
  } finally { await f.close(); }
});

it.each(['intent', 'publication'] as const)(
  'bounds a persistent COMMIT lock after %s, reports BUSY, and rolls back without replay', async phase => {
    const f = await fixture();
    const native = FilesystemRepository.prototype.commit;
    const publish = vi.spyOn(FilesystemRepository.prototype, 'commit').mockImplementation(async function (this: FilesystemRepository, write) {
      const result = await native.call(this, write); if (phase === 'publication') f.hold(); return result;
    });
    const intent = RepositoryLedger.prototype.intent;
    const intents = vi.spyOn(RepositoryLedger.prototype, 'intent').mockImplementation(function (this: RepositoryLedger, ...args) {
      intent.apply(this, args); if (phase === 'intent') f.hold();
    });
    try {
      await expect(f.repo.commit(candidate('blocked'))).rejects.toMatchObject({ code: 'REPOSITORY_BUSY' });
      expect(f.alerts).toEqual([{ code: 'REPOSITORY_BUSY' }]);
      expect(intents).toHaveBeenCalledTimes(1);
      expect(publish).toHaveBeenCalledTimes(phase === 'intent' ? 0 : 1);
      f.release();
      const receipt = await f.repo.receipt('blocked');
      if (phase === 'publication') expect(receipt).toMatchObject({ status: 'pending', publication: 'unconfirmed' });
      else expect(receipt).toBeUndefined();
      publish.mockRestore(); intents.mockRestore();
      if (phase === 'publication') {
        // Files may already be durable. Reconcile the saved intent; never call
        // an uncertain acknowledgement proof that nothing was published.
        expect(await f.repo.findOperation('blocked')).toMatchObject({ receipt: { status: 'committed' } });
      }
      expect(await f.repo.commit(candidate('after-release'))).toMatchObject({ status: 'committed' });
    } finally { await f.close(); }
  });
