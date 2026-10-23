import { access, mkdtemp, readFile, realpath, rm, unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { RepositoryLedger as LegacyLedger } from './fixtures/retained-reader-0.9/ledger.js';
import { candidate } from './helpers/filesystem-candidate.js';

const epoch = 1_790_000_000_000;
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'retention-clock-')));
  let wall = epoch, monotonic = 0;
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, expiryClock: 'verified-uptime' as const,
    clock: () => wall, monotonicClock: () => monotonic, requireSeparateFilesystem: false,
    onHealth: (event: unknown) => { alerts.push(event); } };
  const stores: ReturnType<typeof createRetainedFilesystemRepository>[] = [];
  const open = () => { const store = createRetainedFilesystemRepository(options)!; stores.push(store); return store; };
  return { options, directory, open, alerts, wall: (value: number) => { wall = value; },
    elapsed: (value: number) => { monotonic = value; },
    async close() { for (const store of stores) store!.close(); await rm(directory, { recursive: true, force: true }); } };
}

it('ignores a large forward wall-clock jump for reads and deletion, while preserving exact saved records', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    const saved = await repo.commit(write);
    f.wall(epoch + 365 * 86400_000); f.elapsed(99);
    expect(await repo.read(write.batch.ref)).toEqual(write.batch);
    expect(await repo.findOperation(write.operationId)).toMatchObject({ receipt: saved });
    expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    f.elapsed(100);
    expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
    expect(await repo.receipt(write.operationId)).toMatchObject({ status: 'expired', dataDeleted: true,
      expiryClock: 'verified-uptime', acceptedAgeMs: 0, expiresAgeMs: 100 });
    expect(f.alerts).toEqual([]);
  } finally { await f.close(); }
});

it('backward wall time does not pause verified age or make a fresh record invalid', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    await repo.commit(write); f.wall(epoch - 86400_000); f.elapsed(50);
    expect(await repo.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
    f.elapsed(100);
    await expect(repo.read(write.batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
    expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});

it('restart credits only persisted verified age, never a wall-clock jump or unobserved downtime', async () => {
  const f = await fixture(), first = f.open(), write = candidate();
  try {
    await first.commit(write); f.elapsed(40); await first.read(write.batch.ref); first.close();
    f.wall(epoch + 10 * 365 * 86400_000); f.elapsed(0);
    const restarted = f.open();
    expect(await restarted.read(write.batch.ref)).toEqual(write.batch);
    f.elapsed(59); expect(await restarted.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    f.elapsed(60); expect(await restarted.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});

it('preserves committed elapsed age across an actual killed process without crediting future wall time', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  let child: ReturnType<typeof fork> | undefined;
  try {
    await repo.commit(write); repo.close();
    child = fork(fileURLToPath(new URL('./helpers/retained-clock-child.ts', import.meta.url)), [f.directory],
      { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
    for (const elapsed of [0, 40]) {
      const response = once(child, 'message');
      child.send({ elapsed, wall: epoch + 10 * 365 * 86400_000, id: write.batch.ref.id });
      expect((await response)[0]).toEqual({ value: write.batch });
    }
    const stopped = once(child, 'exit'); child.kill('SIGKILL');
    expect((await stopped)[1]).toBe('SIGKILL'); child = undefined;
    f.wall(epoch + 20 * 365 * 86400_000); f.elapsed(0);
    const restarted = f.open(); expect(await restarted.read(write.batch.ref)).toEqual(write.batch);
    f.elapsed(59); expect(await restarted.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    f.elapsed(60); expect(await restarted.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally {
    if (child) { const stopped = once(child, 'exit'); child.kill('SIGKILL'); await stopped; }
    await f.close();
  }
});

it('independent handles do not double count the same elapsed interval', async () => {
  const f = await fixture(), a = f.open(), write = candidate();
  try {
    await a.commit(write); f.elapsed(30); await a.read(write.batch.ref);
    const b = f.open(); await b.read(write.batch.ref);
    f.elapsed(70); await a.read(write.batch.ref); await b.read(write.batch.ref);
    expect(await b.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    f.elapsed(99); expect(await a.read(write.batch.ref)).toEqual(write.batch);
    f.elapsed(100); expect(await b.sweep({ limit: 1 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});

it('a broken monotonic clock cannot expire data and emits a fixed independent alert', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    f.elapsed(10); await repo.commit(write);
    f.elapsed(9);
    await expect(repo.sweep({ limit: 1 })).rejects.toMatchObject({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    await access(join(f.directory, 'operations', createHash('sha256').update(write.operationId).digest('hex'), 'operation.json.gz'));
    f.elapsed(20); expect(await repo.read(write.batch.ref)).toEqual(write.batch);
  } finally { await f.close(); }
});

it('fences legacy readers without changing actual elapsed-age storage or logical references', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    for (const [file, hash] of [['ledger.ts', '20ecb724cdee597d936bffd5f7e64d2594ade90bf467b2bc242c3979767b5fac'],
      ['contracts.ts', '544b8c464912dc0b953fb7a0230d0445ed135c4e231c15cfff828ba4a01105e9']]) {
      expect(createHash('sha256').update(await readFile(new URL('./fixtures/retained-reader-0.9/' + file, import.meta.url))).digest('hex')).toBe(hash);
    }
    await repo.commit(write);
    expect(() => new LegacyLedger(f.directory, 100)).toThrow();
    expect(await repo.read(write.batch.ref)).toEqual(write.batch);
    const { expiryClock: _clock, monotonicClock: _mono, ...legacyOptions } = f.options;
    const legacy = createRetainedFilesystemRepository(legacyOptions)!;
    try { await expect(legacy.read(write.batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); }
    finally { legacy.close(); }
  } finally { await f.close(); }
});

it('does not silently migrate an existing wall-clock namespace', async () => {
  const f = await fixture();
  try {
    const legacyOptions = { ...f.options }; delete (legacyOptions as { expiryClock?: string }).expiryClock;
    delete (legacyOptions as { monotonicClock?: unknown }).monotonicClock;
    const legacy = createRetainedFilesystemRepository(legacyOptions)!;
    await legacy.commit(candidate()); legacy.close();
    const guarded = f.open();
    await expect(guarded.read(candidate().batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
  } finally { await f.close(); }
});

it.each([NaN, Infinity, -1, Number.MAX_SAFE_INTEGER + 1])('refuses an invalid elapsed-clock value %s without deleting data', async value => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    await repo.commit(write); f.elapsed(value);
    await expect(repo.sweep({ limit: 1 })).rejects.toMatchObject({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    f.elapsed(0); expect(await repo.read(write.batch.ref)).toEqual(write.batch);
  } finally { await f.close(); }
});

it('refuses a lost format fence rather than silently recreating it over retained identities', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    await repo.commit(write); repo.close(); await unlink(join(f.directory, 'retention.sqlite'));
    await expect(f.open().read(write.batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});

it('refuses using the wall clock as the elapsed clock', async () => {
  const f = await fixture();
  try {
    expect(() => createRetainedFilesystemRepository({ ...f.options, monotonicClock: f.options.clock })).toThrow();
    expect(() => createRetainedFilesystemRepository({ ...f.options, monotonicClock: Date.now })).toThrow();
  } finally { await f.close(); }
});
