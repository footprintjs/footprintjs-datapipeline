import { access, mkdtemp, realpath, rm, unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { expect, it, vi } from 'vitest';
import { clockFence } from '../src/storage/retainedRepository/clockFence.js';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { candidate } from './helpers/filesystem-candidate.js';

// Exercise the Node 22.13 API surface even when the test runner is newer.
class MinimumDatabase extends DatabaseSync {
  constructor(...args: ConstructorParameters<typeof DatabaseSync>) {
    super(...args);
    return new Proxy(this, { get(target, key) {
      if (key === 'isTransaction') return undefined;
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    } });
  }
}
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'clock-minimum-')));
  let elapsed = 0;
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false,
    expiryClock: 'verified-uptime' as const, clock: () => 1_790_000_000_000, monotonicClock: () => elapsed,
    onHealth: (event: unknown) => { alerts.push(event); } };
  const stores: ReturnType<typeof createRetainedFilesystemRepository>[] = [];
  const open = () => { const repo = createRetainedFilesystemRepository(options)!; stores.push(repo); return repo; };
  return { directory, options, alerts, open, elapsed: (value: number) => { elapsed = value; },
    async close() { vi.restoreAllMocks(); for (const repo of stores) repo!.close(); await rm(directory, { recursive: true, force: true }); } };
}

it('supports guarded startup, publication, restart and cleanup on the running minimum-compatible runtime', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    expect(await repo.receipt('absent')).toBeUndefined();
    expect((await repo.commit(write)).status).toBe('committed');
    expect(await repo.read(write.batch.ref)).toEqual(write.batch);
    repo.close();
    const restarted = f.open();
    expect(await restarted.read(write.batch.ref)).toEqual(write.batch);
    f.elapsed(100);
    expect(await restarted.sweep({ limit: 1 })).toEqual({ deleted: 1 });
    expect(await restarted.receipt(write.operationId)).toMatchObject({ status: 'expired', dataDeleted: true });
    restarted.close();
    await unlink(join(f.directory, 'retention-v2.sqlite'));
    await expect(f.open().commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    await expect(access(join(f.directory, 'retention-v2.sqlite'))).rejects.toMatchObject({ code: 'ENOENT' });
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
  } finally { await f.close(); }
});

it('preserves the existing write transaction and refuses activation before its commit without isTransaction', async () => {
  const f = await fixture(), repo = f.open();
  let authority: ReturnType<typeof clockFence> | undefined, db: DatabaseSync | undefined;
  try {
    await repo.receipt('absent'); repo.close();
    authority = clockFence(MinimumDatabase, f.directory, 100);
    db = new MinimumDatabase(join(f.directory, 'retention-v2.sqlite'));
    db.exec('BEGIN IMMEDIATE; UPDATE elapsed_clock SET ageMs=7');
    expect(authority.requireAuthority(db)).toBe(false);
    expect(() => authority!.activate()).toThrow(expect.objectContaining({ code: 'REPOSITORY_UNCONFIRMED' }));
    expect(db.prepare('SELECT ageMs FROM elapsed_clock').get()?.ageMs).toBe(7);
    const observer = new DatabaseSync(join(f.directory, 'retention-v2.sqlite'));
    try { expect(observer.prepare('SELECT ageMs FROM elapsed_clock').get()?.ageMs).toBe(0); }
    finally { observer.close(); }
    db.exec('COMMIT');
    authority.activate();
    expect(db.prepare('SELECT ageMs FROM elapsed_clock').get()?.ageMs).toBe(7);
  } finally { db?.close(); authority?.close(); await f.close(); }
});

it('requires a live initialization transaction both before setup and before binding without isTransaction', async () => {
  const f = await fixture();
  const authority = clockFence(MinimumDatabase, f.directory, 100);
  const db = new MinimumDatabase(join(f.directory, 'retention-v2.sqlite'));
  try {
    expect(() => authority.requireAuthority(db)).toThrow(expect.objectContaining({ code: 'REPOSITORY_UNCONFIRMED' }));
    db.exec('BEGIN IMMEDIATE');
    expect(authority.requireAuthority(db)).toBe(true);
    db.exec('COMMIT');
    expect(() => authority.bind(db)).toThrow(expect.objectContaining({ code: 'REPOSITORY_UNCONFIRMED' }));
    expect(db.prepare("SELECT name FROM sqlite_master WHERE name='authority'").get()).toBeUndefined();
    // The rejected probe must not leave its own transaction open.
    db.exec('BEGIN IMMEDIATE; ROLLBACK');
  } finally { db.close(); authority.close(); await f.close(); }
});

it.each([
  { code: 'ERR_SQLITE_ERROR', errcode: 1, message: 'some other SQL failure' },
  { code: 'ERR_SQLITE_ERROR', errcode: 5, message: 'cannot start a transaction within a transaction' },
  { code: 'OTHER_ERROR', errcode: 1, message: 'cannot start a transaction within a transaction' },
])('propagates an unrelated BEGIN failure rather than treating it as transaction evidence: $code/$errcode/$message', async detail => {
  const f = await fixture();
  const authority = clockFence(MinimumDatabase, f.directory, 100);
  const db = new MinimumDatabase(join(f.directory, 'retention-v2.sqlite'));
  const failure = Object.assign(Error(detail.message), { code: detail.code, errcode: detail.errcode });
  const exec = vi.spyOn(db, 'exec').mockImplementationOnce(() => { throw failure; });
  try { expect(() => authority.requireAuthority(db)).toThrow(failure); }
  finally { exec.mockRestore(); db.close(); authority.close(); await f.close(); }
});

it('propagates failure to roll back its own empty probe transaction', async () => {
  const f = await fixture();
  const authority = clockFence(MinimumDatabase, f.directory, 100);
  const db = new MinimumDatabase(join(f.directory, 'retention-v2.sqlite'));
  const original = db.exec.bind(db), failure = Error('probe rollback failed');
  const exec = vi.spyOn(db, 'exec').mockImplementation(sql => {
    if (sql === 'ROLLBACK') throw failure;
    original(sql);
  });
  try { expect(() => authority.requireAuthority(db)).toThrow(failure); }
  finally { exec.mockRestore(); db.close(); authority.close(); await f.close(); }
});
