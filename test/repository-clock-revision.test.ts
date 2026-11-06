import { access, mkdtemp, realpath, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { RepositoryLedger } from '../src/storage/retainedRepository/ledger.js';
import { ElapsedAge } from '../src/storage/retainedRepository/elapsedAge.js';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
afterEach(() => { vi.restoreAllMocks(); });
async function fixture(retentionMs = 100, defaultClock = false) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'clock-revision-')));
  let elapsed = 0, wall = 1_790_000_000_000;
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs, expiryClock: 'verified-uptime' as const,
    clock: () => wall, requireSeparateFilesystem: false,
    onHealth: (event: unknown) => { alerts.push(event); } };
  const stores: ReturnType<typeof createRetainedFilesystemRepository>[] = [];
  function open(clock = () => elapsed) {
    const store = createRetainedFilesystemRepository({ ...options, ...(defaultClock ? {} : { monotonicClock: clock }) })!;
    stores.push(store); return store;
  }
  const database = () => new DatabaseSync(join(directory, 'retention-v2.sqlite'));
  const saved = () => {
    const db = database();
    try { return db.prepare('SELECT ageMs,revision FROM elapsed_clock').get()!; }
    finally { db.close(); }
  };
  const file = (id: string) => join(directory, 'operations', createHash('sha256').update(id).digest('hex'), 'operation.json.gz');
  return { open, database, saved, file, alerts, at: (value: number) => { elapsed = value; },
    wall: (value: number) => { wall = value; },
    async close() { for (const store of stores) store!.close(); await rm(directory, { recursive: true, force: true }); } };
}

it('drops old idle time before data accepted by a later handle, even with different clock origins', async () => {
  const f = await fixture(), old = f.open(), write = candidate('fresh-after-idle');
  let freshElapsed = 0;
  try {
    await old.receipt('initialize-old');
    f.at(260);
    const fresh = f.open(() => freshElapsed);
    await fresh.commit(write);
    expect(await fresh.receipt(write.operationId)).toMatchObject({ acceptedAgeMs: 0, expiresAgeMs: 100 });
    f.at(261); freshElapsed = 1;
    expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    await access(f.file(write.operationId));
    expect(await old.read(write.batch.ref)).toEqual(write.batch);
    f.at(360); expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    f.at(361); expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});

it('does not charge pre-acceptance idle time through an older handle using the default performance clock', async () => {
  let elapsed = 0;
  const performanceClock = vi.spyOn(performance, 'now').mockImplementation(() => elapsed);
  // Omit monotonicClock: exercise the actual performance.now default while
  // real filesystem and SQLite work cannot silently consume the short TTL.
  const f = await fixture(300, true), old = f.open(), fresh = f.open(), write = candidate('default-clock-idle');
  try {
    await old.receipt('initialize-old');
    elapsed = 350;
    await fresh.commit(write);
    expect(await fresh.receipt(write.operationId)).toMatchObject({ acceptedAgeMs: 0, expiresAgeMs: 300 });
    const samples = performanceClock.mock.calls.length;
    elapsed = 351; f.wall(9_000_000_000_000);
    expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    await access(f.file(write.operationId));
    expect(await fresh.read(write.batch.ref)).toEqual(write.batch);
    expect(performanceClock.mock.calls.length).toBeGreaterThan(samples);
    elapsed = 649; expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    elapsed = 650; expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    elapsed = 651; expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 1 });
    expect(await fresh.receipt(write.operationId)).toMatchObject({ status: 'expired', dataDeleted: true });
  } finally { await f.close(); }
});

it('expires a default-clock intent when a full TTL elapses after its durable acceptance and before publication', async () => {
  let elapsed = 0;
  vi.spyOn(performance, 'now').mockImplementation(() => elapsed);
  const f = await fixture(300, true), repo = f.open(), write = candidate('default-clock-publication-gap');
  const commit = RepositoryLedger.prototype.commit;
  let gap = true;
  vi.spyOn(RepositoryLedger.prototype, 'commit').mockImplementation(async function (this: RepositoryLedger) {
    await commit.call(this);
    if (gap) { gap = false; elapsed += 300; }
  });
  const publication = vi.spyOn(FilesystemRepository.prototype, 'commit');
  try {
    await expect(repo.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
    expect(publication).not.toHaveBeenCalled();
    expect(await repo.receipt(write.operationId)).toMatchObject({ status: 'expired', publication: 'unconfirmed',
      dataDeleted: false, acceptedAgeMs: 0, expiresAgeMs: 300 });
    await expect(access(f.file(write.operationId))).rejects.toMatchObject({ code: 'ENOENT' });
  } finally { await f.close(); }
});

it('drops idle intervals between alternating new acceptances, then progresses while one handle continues', async () => {
  const f = await fixture(), a = f.open(), b = f.open(), write = candidate('alternating');
  try {
    await a.commit(write); await b.commit(candidate('alternating-peer'));
    for (let i = 1; i <= 6; i++) {
      f.at(i * 200);
      await (i % 2 ? a : b).commit(candidate('alternating-new-' + i));
    }
    expect(f.saved().ageMs).toBe(0);
    f.at(1299); expect(await b.sweep({ limit: 10 })).toEqual({ deleted: 0 });
    f.at(1300); expect(await b.sweep({ limit: 10 })).toEqual({ deleted: 8 });
  } finally { await f.close(); }
});

it('reopens only committed age and drops a surviving handle interval spanning the restarted writer', async () => {
  const f = await fixture(), survivor = f.open(), write = candidate('restart-with-survivor');
  let restartedElapsed = 0;
  try {
    await survivor.commit(write); f.at(40); await survivor.read(write.batch.ref);
    const first = f.open(); await first.read(write.batch.ref); first.close();
    f.at(10_000); f.wall(9_000_000_000_000);
    const restarted = f.open(() => restartedElapsed), fresh = candidate('accepted-after-restart');
    await restarted.commit(fresh);
    expect(await restarted.receipt(fresh.operationId)).toMatchObject({ acceptedAgeMs: 40, expiresAgeMs: 140 });
    expect(await survivor.sweep({ limit: 2 })).toEqual({ deleted: 0 });
    f.at(10_059); expect(await survivor.sweep({ limit: 2 })).toEqual({ deleted: 0 });
    f.at(10_060); expect(await survivor.sweep({ limit: 2 })).toEqual({ deleted: 1 });
    await access(f.file(fresh.operationId));
    restartedElapsed = 60;
    expect(await restarted.read(fresh.batch.ref)).toEqual(fresh.batch);
  } finally { await f.close(); }
});

it('retains earlier verified time after rollback without committing failed work or resetting backward-clock checks', async () => {
  const f = await fixture(), repo = f.open(), write = candidate('rollback-interval');
  try {
    await repo.commit(write); f.at(30); await repo.read(write.batch.ref);
    f.at(60);
    vi.spyOn(RepositoryLedger.prototype, 'row').mockImplementationOnce(() => { throw Error('transaction failed'); });
    await expect(repo.receipt(write.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(f.saved().ageMs).toBe(30);
    f.at(59);
    await expect(repo.sweep({ limit: 1 })).rejects.toMatchObject({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    f.at(99); expect(await repo.read(write.batch.ref)).toEqual(write.batch);
    expect(f.saved().ageMs).toBe(99);
    f.at(100); expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});

it.each([false, true])('rechecks authority after a new-intent COMMIT throws (durable COMMIT happened: %s)', async durable => {
  const f = await fixture(), repo = f.open(), write = candidate('before-failed-intent');
  const failed = candidate('failed-intent');
  try {
    await repo.commit(write); f.at(30); await repo.read(write.batch.ref);
    const before = f.saved(), exec = DatabaseSync.prototype.exec;
    const native = vi.spyOn(FilesystemRepository.prototype, 'commit');
    let failCommit = true;
    const fault = vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: InstanceType<typeof DatabaseSync>, sql) {
      if (sql === 'COMMIT' && failCommit) {
        failCommit = false;
        if (durable) exec.call(this, sql);
        throw Object.assign(Error('uncertain final COMMIT'), { errcode: 10 });
      }
      return exec.call(this, sql);
    });
    f.at(60);
    await expect(repo.commit(failed)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    fault.mockRestore();
    expect(native).not.toHaveBeenCalled();
    if (durable) {
      expect(f.saved()).toEqual({ ageMs: 60, revision: Number(before.revision) + 1 });
      f.at(260);
      // The pretransaction anchor has the old generation. It must not charge
      // its unpublished elapsed time to this uncertain, durable acceptance.
      expect(await repo.receipt(failed.operationId)).toMatchObject({ status: 'pending', acceptedAgeMs: 60, expiresAgeMs: 160 });
      expect(await repo.sweep({ limit: 2 })).toEqual({ deleted: 0 });
    } else {
      expect(f.saved()).toEqual(before);
      expect(await repo.receipt(failed.operationId)).toBeUndefined();
      f.at(99); expect(await repo.read(write.batch.ref)).toEqual(write.batch);
      f.at(100); expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
    }
  } finally { await f.close(); }
});

it.each([false, true])('drops a busy final COMMIT interval before a foreign acceptance (COMMIT abandoned: %s)', async abandon => {
  const f = await fixture(), old = f.open(), write = candidate('after-commit-wait');
  let observer: InstanceType<typeof DatabaseSync> | undefined, pending: Promise<unknown> | undefined;
  try {
    await old.receipt('initialize-old');
    const before = f.saved();
    observer = f.database();
    observer.exec('BEGIN'); observer.prepare('SELECT * FROM elapsed_clock').all();
    let reached!: () => void;
    const checkpointed = new Promise<void>(resolve => { reached = resolve; });
    const checkpoint = ElapsedAge.prototype.checkpoint;
    const calls = vi.spyOn(ElapsedAge.prototype, 'checkpoint').mockImplementation(function (this: ElapsedAge) {
      checkpoint.call(this); reached();
    });
    pending = old.receipt('blocked-final-commit');
    const outcome = pending.then(value => ({ value }), error => ({ error }));
    await checkpointed; f.at(260);
    if (abandon) expect(await outcome).toMatchObject({ error: { code: 'REPOSITORY_BUSY' } });
    observer.exec('COMMIT');
    if (!abandon) expect(await outcome).toEqual({ value: undefined });
    expect(calls).toHaveBeenCalledTimes(1); calls.mockRestore();
    expect(f.saved().revision).toBe(before.revision);
    expect(f.saved().ageMs).toBe(before.ageMs);
    const fresh = f.open(); await fresh.commit(write);
    f.at(261);
    expect(await old.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    await access(f.file(write.operationId));
    expect(await old.read(write.batch.ref)).toEqual(write.batch);
  } finally {
    if (observer?.isTransaction) observer.exec('ROLLBACK');
    await pending?.catch(() => {}); observer?.close(); await f.close();
  }
});

it('counts verified time during a long publication without acknowledging data already expired', async () => {
  const f = await fixture(), repo = f.open(), write = candidate('long-publication');
  const publish = FilesystemRepository.prototype.commit;
  vi.spyOn(FilesystemRepository.prototype, 'commit').mockImplementation(async function (this: FilesystemRepository, input) {
    const receipt = await publish.call(this, input); f.at(100); return receipt;
  });
  try {
    await expect(repo.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
    expect(await repo.receipt(write.operationId)).toMatchObject({ status: 'expired', publication: 'unconfirmed' });
    await access(f.file(write.operationId));
  } finally { await f.close(); }
});

it('does not charge the fractional millisecond preceding acceptance to the new record', async () => {
  const f = await fixture(), repo = f.open(), write = candidate('fractional-acceptance');
  try {
    await repo.receipt('initialize'); f.at(0.75); await repo.commit(write);
    expect(await repo.receipt(write.operationId)).toMatchObject({ acceptedAgeMs: 1, expiresAgeMs: 101 });
    f.at(100.25); expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    f.at(101); expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});

it('preserves fractional progress across consecutive successful transactions by one handle', async () => {
  const f = await fixture(), repo = f.open();
  try {
    await repo.receipt('initialize');
    for (let i = 1; i <= 10; i++) { f.at(i / 10); await repo.receipt('fractional-progress'); }
    expect(f.saved().ageMs).toBe(1);
  } finally { await f.close(); }
});

it('refuses an exhausted revision before an expired candidate can be deleted', async () => {
  const f = await fixture(), repo = f.open(), write = candidate('revision-exhausted');
  try {
    await repo.commit(write);
    const db = f.database();
    try { db.prepare('UPDATE elapsed_clock SET revision=?,ageMs=100').run(Number.MAX_SAFE_INTEGER); }
    finally { db.close(); }
    f.at(100);
    await expect(repo.sweep({ limit: 1 })).rejects.toMatchObject({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    await access(f.file(write.operationId));
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
  } finally { await f.close(); }
});
