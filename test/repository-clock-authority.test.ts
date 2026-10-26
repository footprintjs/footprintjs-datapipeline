import { access, mkdtemp, realpath, rm, unlink, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { ClockAuthority, clockFence } from '../src/storage/retainedRepository/clockFence.js';
import { candidate } from './helpers/filesystem-candidate.js';

async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'clock-authority-')));
  let elapsed = 0;
  const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false,
    expiryClock: 'verified-uptime' as const, clock: () => 1_790_000_000_000, monotonicClock: () => elapsed,
    onHealth: (event: unknown) => { alerts.push(event); } };
  const stores: ReturnType<typeof createRetainedFilesystemRepository>[] = [];
  const open = () => { const repo = createRetainedFilesystemRepository(options)!; stores.push(repo); return repo; };
  function inspect<T>(name: string, action: (db: DatabaseSync) => T): T {
    const db = new DatabaseSync(join(directory, name));
    try { return action(db); } finally { db.close(); }
  }
  return { directory, options, alerts, open, inspect, elapsed: (value: number) => { elapsed = value; },
    async close() { vi.restoreAllMocks(); for (const repo of stores) repo!.close(); await rm(directory, { recursive: true, force: true }); } };
}

it('recovers a fence file opened before its first initialization transaction committed', async () => {
  const f = await fixture();
  try {
    await writeFile(join(f.directory, 'retention.sqlite'), '');
    await f.open().commit(candidate());
    expect(f.inspect('retention.sqlite', db => db.prepare('SELECT state FROM clock_authority').get())?.state).toBe('active');
    expect(f.alerts).toEqual([]);
  } finally { await f.close(); }
});

it.each(['absent', 'empty', 'rolled-back'] as const)('recovers interrupted initialization with a durable marker and %s ledger', async state => {
  const f = await fixture();
  try {
    const first = clockFence(DatabaseSync, f.directory, 100); first.close();
    const marker = f.inspect('retention.sqlite', db => db.prepare('SELECT state,token FROM clock_authority').get()!);
    expect(marker.state).toBe('initializing');
    if (state === 'empty') await writeFile(join(f.directory, 'retention-v2.sqlite'), '');
    if (state === 'rolled-back') f.inspect('retention-v2.sqlite', db => db.exec('BEGIN IMMEDIATE; CREATE TABLE incomplete(id INTEGER); ROLLBACK'));
    const write = candidate();
    await f.open().commit(write);
    expect(f.inspect('retention.sqlite', db => db.prepare('SELECT state,token FROM clock_authority').get())).toEqual({ state: 'active', token: marker.token });
    expect(f.inspect('retention-v2.sqlite', db => db.prepare('SELECT token FROM authority').get())?.token).toBe(marker.token);
    expect(f.alerts).toEqual([]);
  } finally { await f.close(); }
});

it('recovers a committed ledger interrupted before activation without replacing its identity', async () => {
  const f = await fixture();
  const interrupted = vi.spyOn(ClockAuthority.prototype, 'activate').mockImplementationOnce(() => { throw Error('interrupted activation'); });
  try {
    await expect(f.open().receipt('absent')).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    interrupted.mockRestore();
    const marker = f.inspect('retention.sqlite', db => db.prepare('SELECT state,token FROM clock_authority').get()!);
    expect(marker.state).toBe('initializing');
    expect(f.inspect('retention-v2.sqlite', db => db.prepare('SELECT token FROM authority').get())?.token).toBe(marker.token);
    const repo = f.open(), write = candidate();
    await repo.commit(write);
    expect(await repo.read(write.batch.ref)).toEqual(write.batch);
    expect(f.inspect('retention.sqlite', db => db.prepare('SELECT state,token FROM clock_authority').get())).toEqual({ state: 'active', token: marker.token });
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
  } finally { await f.close(); }
});

it('recovers a clock failure before the first ledger commit', async () => {
  const f = await fixture();
  try {
    f.elapsed(NaN);
    await expect(f.open().receipt('absent')).rejects.toMatchObject({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    expect(f.inspect('retention.sqlite', db => db.prepare('SELECT state FROM clock_authority').get())?.state).toBe('initializing');
    f.elapsed(0);
    const repo = f.open(); await repo.commit(candidate());
    expect(f.inspect('retention.sqlite', db => db.prepare('SELECT state FROM clock_authority').get())?.state).toBe('active');
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_CLOCK_UNAVAILABLE' }]);
  } finally { await f.close(); }
});

it('refuses a lost active ledger after cleanup instead of allowing an expired identity to be reused', async () => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    await repo.commit(write); f.elapsed(100);
    expect(await repo.sweep({ limit: 1 })).toEqual({ deleted: 1 });
    repo.close(); await unlink(join(f.directory, 'retention-v2.sqlite'));
    await expect(f.open().commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    await expect(access(join(f.directory, 'retention-v2.sqlite'))).rejects.toMatchObject({ code: 'ENOENT' });
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
  } finally { await f.close(); }
});

it.each([
  ['policy table', 'DROP TABLE policy'], ['operations table', 'DROP TABLE operations'],
  ['reference table', 'DROP TABLE refs'], ['clock table', 'DROP TABLE elapsed_clock'],
  ['identity table', 'DROP TABLE authority'], ['policy row', 'DELETE FROM policy'],
  ['clock row', 'DELETE FROM elapsed_clock'], ['identity row', 'DELETE FROM authority'],
  ['clock revision column', 'ALTER TABLE elapsed_clock DROP COLUMN revision'],
  ['clock revision value', 'UPDATE elapsed_clock SET revision=-1'],
  ['mismatched identity', "UPDATE authority SET token='00000000-0000-4000-8000-000000000000'"],
  ['empty ledger', 'DROP TABLE policy; DROP TABLE operations; DROP TABLE refs; DROP TABLE elapsed_clock; DROP TABLE authority'],
])('refuses an active ledger with a missing or invalid %s and reports fixed health', async (_label, damage) => {
  const f = await fixture(), repo = f.open(), write = candidate();
  try {
    await repo.commit(write); repo.close();
    f.inspect('retention-v2.sqlite', db => db.exec(damage!));
    const before = f.inspect('retention-v2.sqlite', db => db.prepare('SELECT name,sql FROM sqlite_master ORDER BY name').all());
    await expect(f.open().readMany([write.batch.ref])).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(f.inspect('retention-v2.sqlite', db => db.prepare('SELECT name,sql FROM sqlite_master ORDER BY name').all())).toEqual(before);
    await access(join(f.directory, 'operations', createHash('sha256').update(write.operationId).digest('hex'), 'operation.json.gz'));
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
  } finally { await f.close(); }
});

it.each(['DROP TABLE policy', 'DELETE FROM policy', 'DROP TABLE clock_authority', 'DELETE FROM clock_authority'])('refuses damaged active fence metadata: %s', async damage => {
  const f = await fixture(), repo = f.open();
  try {
    await repo.commit(candidate()); repo.close();
    f.inspect('retention.sqlite', db => db.exec(damage));
    await expect(f.open().receipt('absent')).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_UNCONFIRMED' }]);
  } finally { await f.close(); }
});

it.each([false, true])('refuses the previous unpublished format without adopting it (ledger exists: %s)', async ledgerExists => {
  const f = await fixture();
  try {
    f.inspect('retention.sqlite', db => db.exec('CREATE TABLE policy(id INTEGER PRIMARY KEY,version INTEGER,retentionMs INTEGER); INSERT INTO policy VALUES(1,2,100)'));
    if (ledgerExists) f.inspect('retention-v2.sqlite', db => db.exec('CREATE TABLE policy(id INTEGER PRIMARY KEY,version INTEGER,retentionMs INTEGER,settings TEXT); INSERT INTO policy VALUES(1,2,100,\'{"expiryClock":"verified-uptime"}\')'));
    await expect(f.open().receipt('absent')).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
    expect(f.inspect('retention.sqlite', db => db.prepare('SELECT version FROM policy').get())?.version).toBe(2);
    expect(f.inspect('retention.sqlite', db => db.prepare("SELECT name FROM sqlite_master WHERE name='clock_authority'").get())).toBeUndefined();
  } finally { await f.close(); }
});

it('refuses a committed partial ledger while initializing instead of filling in missing authority', async () => {
  const f = await fixture();
  try {
    clockFence(DatabaseSync, f.directory, 100).close();
    f.inspect('retention-v2.sqlite', db => db.exec('CREATE TABLE policy(id INTEGER PRIMARY KEY)'));
    await expect(f.open().receipt('absent')).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(f.inspect('retention-v2.sqlite', db => db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all())).toEqual([{ name: 'policy' }]);
  } finally { await f.close(); }
});

it('never adopts existing operations under an initializing marker', async () => {
  const f = await fixture(), repo = f.open();
  try {
    await repo.commit(candidate()); repo.close();
    f.inspect('retention.sqlite', db => db.exec("UPDATE clock_authority SET state='initializing'"));
    await expect(f.open().receipt('absent')).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});

it('serializes simultaneous process initialization into one active identity', async () => {
  const f = await fixture();
  const children: ReturnType<typeof fork>[] = [];
  try {
    const results = await Promise.all([0, 1].map(async () => {
      const child = fork(fileURLToPath(new URL('./helpers/retained-clock-child.ts', import.meta.url)), [f.directory],
        { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
      children.push(child);
      const response = once(child, 'message');
      child.send({ elapsed: 0, wall: 1_790_000_000_000, id: 'absent' });
      return (await response)[0];
    }));
    expect(results).toEqual([{}, {}]);
    const marker = f.inspect('retention.sqlite', db => db.prepare('SELECT state,token FROM clock_authority').get()!);
    expect(marker.state).toBe('active');
    expect(f.inspect('retention-v2.sqlite', db => db.prepare('SELECT token FROM authority').get())?.token).toBe(marker.token);
    await f.open().commit(candidate());
  } finally {
    await Promise.all(children.map(async child => { const stopped = once(child, 'exit'); child.kill('SIGKILL'); await stopped; }));
    await f.close();
  }
});
