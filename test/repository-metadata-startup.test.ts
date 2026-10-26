import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

afterEach(() => vi.restoreAllMocks());

async function fixture(bounded = true) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'metadata-startup-')));
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100,
    expiryClock: 'verified-uptime' as const, clock: () => 1_790_000_000_000,
    monotonicClock: () => 0, requireSeparateFilesystem: false,
    ...(bounded ? { metadataLimits: { maxBytes: 2 * 1024 * 1024, maxOperations: 10, maxReferences: 30 } } : {}),
    onHealth: (event: unknown) => { alerts.push(event); } };
  const repo = createRetainedFilesystemRepository(options)!;
  return { directory, options, repo, alerts,
    async close() { repo.close(); await rm(directory, { recursive: true, force: true }); } };
}

type StartupStage = 'fence' | 'schema' | 'policy' | 'initial-clock' | 'constructor-commit' | 'budget-activation' | 'authority-activation';

/** The engine produces SQLITE_FULL; no synthetic SQLite errors or physical disk fill. */
function nativeFullOnce(stage: StartupStage) {
  const exec = DatabaseSync.prototype.exec, prepare = DatabaseSync.prototype.prepare;
  let armed = true, nativeCode: number | undefined, failedConnection: DatabaseSync | undefined;
  const fail = (db: DatabaseSync, write?: string): never => {
    armed = false; failedConnection = db;
    try {
      if (write) {
        // The original fence/schema statement itself needs more than one page.
        exec.call(db, 'PRAGMA max_page_count=1');
        exec.call(db, write);
      } else {
        // Small writes/COMMIT need not allocate pages. Force a real allocation
        // failure at their boundary to exercise native automatic rollback.
        const pages = prepare.call(db, 'PRAGMA page_count').get()!.page_count;
        exec.call(db, `PRAGMA max_page_count=${pages}; CREATE TABLE startup_full(payload)`);
      }
    } catch (error) { nativeCode = (error as { errcode?: number }).errcode; throw error; }
    throw new Error('Expected native SQLite FULL');
  };
  vi.spyOn(DatabaseSync.prototype, 'prepare').mockImplementation(function (this: DatabaseSync, sql: string) {
    const statement = prepare.call(this, sql);
    if (armed && stage === 'policy' && sql === 'INSERT INTO policy VALUES(1,5,?,?)') {
      vi.spyOn(statement, 'run').mockImplementationOnce(() => fail(this));
    }
    if (armed && stage === 'authority-activation' && sql.startsWith("UPDATE clock_authority SET state='active'")) {
      vi.spyOn(statement, 'run').mockImplementationOnce(() => fail(this));
    }
    return statement;
  });
  vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: DatabaseSync, sql: string) {
    if (armed) {
      if ((stage === 'fence' && sql.startsWith('CREATE TABLE policy') && sql.includes('clock_authority')) ||
        (stage === 'schema' && sql.startsWith('CREATE TABLE policy') && sql.includes('CREATE TABLE operations'))) return fail(this, sql);
      if (stage === 'initial-clock' && sql === 'INSERT INTO elapsed_clock VALUES(1,0,0)') return fail(this);
      if (stage === 'constructor-commit' && sql === 'COMMIT' &&
        prepare.call(this, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='elapsed_clock'").get()) return fail(this);
      if (stage === 'budget-activation' && sql === 'PRAGMA cache_spill=OFF') return fail(this);
    }
    return exec.call(this, sql);
  });
  return () => {
    expect(nativeCode).toBe(13);
    // Startup must not retain a failed connection or its transaction lock.
    expect(failedConnection).toBeDefined();
    expect(() => failedConnection!.prepare('SELECT 1')).toThrow();
  };
}

it.each<StartupStage>(['fence', 'schema', 'policy', 'initial-clock', 'constructor-commit', 'budget-activation', 'authority-activation'])(
  'maps native FULL during startup %s, closes the failed connection and permits a clean retry', async stage => {
    const f = await fixture();
    const assertFailure = nativeFullOnce(stage);
    const publication = vi.spyOn(FilesystemRepository.prototype, 'commit');
    const write = candidate('startup-full');
    try {
      await expect(f.repo.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
      assertFailure();
      expect(f.alerts).toEqual([{ code: 'REPOSITORY_METADATA_LIMIT' }]);
      expect(publication).not.toHaveBeenCalled();
      expect(await f.repo.receipt(write.operationId)).toBeUndefined();
      expect(await f.repo.metadataCapacity()).toMatchObject({ operations: 0, references: 0 });
      expect(publication).not.toHaveBeenCalled();
      expect((await f.repo.commit(write)).status).toBe('committed');
      expect(publication).toHaveBeenCalledTimes(1);
      expect(await f.repo.metadataCapacity()).toMatchObject({ operations: 1, references: 3 });
      f.repo.close();
      expect(await f.repo.read(write.batch.ref)).toEqual(write.batch);
      expect(await f.repo.metadataCapacity()).toMatchObject({ operations: 1, references: 3 });
      expect(f.alerts).toEqual([{ code: 'REPOSITORY_METADATA_LIMIT' }]);
    } finally { await f.close(); }
  });

it.each<StartupStage>(['fence', 'schema'])('keeps unbounded startup %s FULL unconfirmed', async stage => {
  const f = await fixture(false);
  const assertFailure = nativeFullOnce(stage);
  const publication = vi.spyOn(FilesystemRepository.prototype, 'commit');
  try {
    await expect(f.repo.commit(candidate('unbounded-full'))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    assertFailure();
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
    expect(publication).not.toHaveBeenCalled();
  } finally { await f.close(); }
});

it.each(['retention.sqlite', 'retention-v2.sqlite'])(
  'preserves native startup BUSY for a bounded namespace locked at %s', async name => {
    const f = await fixture();
    let blocker: DatabaseSync | undefined;
    const publication = vi.spyOn(FilesystemRepository.prototype, 'commit');
    try {
      await f.repo.metadataCapacity(); f.repo.close();
      blocker = new DatabaseSync(join(f.directory, name));
      blocker.exec('BEGIN IMMEDIATE');
      await expect(f.repo.commit(candidate('startup-busy'))).rejects.toMatchObject({ code: 'REPOSITORY_BUSY' });
      expect(f.alerts).toEqual([{ code: 'REPOSITORY_BUSY' }]);
      expect(publication).not.toHaveBeenCalled();
      blocker.exec('ROLLBACK');
      expect((await f.repo.commit(candidate('startup-busy'))).status).toBe('committed');
      expect(publication).toHaveBeenCalledTimes(1);
    } finally {
      if (blocker?.isTransaction) blocker.exec('ROLLBACK');
      blocker?.close(); await f.close();
    }
  });
