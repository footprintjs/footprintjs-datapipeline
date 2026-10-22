import { spawn, type ChildProcess } from 'node:child_process';
import { chmod, lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout } from 'node:timers/promises';
import { afterEach, expect, it, vi } from 'vitest';
import { createSealedFilesystemHistory } from '../src/storage/sealedHistory/index.js';
import { sealedSnapshot } from './helpers/sealed-history-fixture.js';

const stageFault = vi.hoisted(() => ({ path: '' }));
vi.mock('node:fs/promises', async original => {
  const fs = await original<typeof import('node:fs/promises')>();
  return { ...fs, async lstat(...args: Parameters<typeof fs.lstat>) {
    const info = await fs.lstat(...args);
    return String(args[0]) === stageFault.path ? new Proxy(info, { get(target, key) {
      if (key === 'uid') return Number(target.uid) + 1;
      const value = Reflect.get(target, key, target) as unknown;
      return typeof value === 'function' ? value.bind(target) : value;
    } }) : info;
  } };
});
const roots: string[] = [], children = new Set<ChildProcess>();
afterEach(async () => {
  stageFault.path = ''; vi.restoreAllMocks();
  for (const child of children) if (child.exitCode === null && child.signalCode === null) {
    const exited = once(child, 'exit'); child.kill('SIGKILL'); await exited;
  }
  children.clear();
  for (const path of roots.splice(0)) await rm(path, { recursive: true, force: true });
});
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'sealed-process-'))); roots.push(directory);
  const store = createSealedFilesystemHistory({ enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false })!;
  return { directory, store };
}
function child(directory: string, mode: string, point: number | string = 0) {
  const process = spawn(globalThis.process.execPath, ['--import', 'tsx', fileURLToPath(new URL('./helpers/sealed-history-child.ts', import.meta.url)), directory, mode, String(point)],
    { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  children.add(process);
  return process;
}
async function message(process: ChildProcess, state: string) {
  const [value] = await once(process, 'message', { signal: AbortSignal.timeout(10_000) });
  expect(value).toMatchObject({ state });
  return value as { state: string; phase?: string; code?: string; publication?: { sha256: string } };
}
async function kill(process: ChildProcess, delay: number) {
  await setTimeout(delay);
  const exited = once(process, 'exit');
  expect(process.kill('SIGKILL')).toBe(true);
  expect(await exited).toEqual([null, 'SIGKILL']);
}
it('recovers real SIGKILLs at sampled SQLite initialization boundaries without adopting unknown files', async () => {
  // These are committed statement boundaries; a delay cannot move the frozen child.
  for (const point of [4, 1, 7, 2, 6, 3, 5]) {
    const f = await fixture(), writer = child(f.directory, 'checkpoint', point);
    await message(writer, 'initializing');
    await kill(writer, 0);
    await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
    expect(await readdir(join(f.directory, '.staging'))).toHaveLength(1);
    expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
    expect(await readdir(join(f.directory, '.staging'))).toEqual([]);
    expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
  }
}, 30_000);
it('retries early process kills at varied delays during first publication', async () => {
  for (const delay of [0, 3, 1, 5, 2, 4]) {
    const f = await fixture(), writer = child(f.directory, 'race');
    await message(writer, 'ready'); writer.send('publish');
    await kill(writer, delay);
    expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
  }
}, 30_000);
it.each(['marker-link', 'snapshot-stage', 'snapshot-link', 'catalogue-link'])('recovers a real SIGKILL after %s with the expected leftover kinds', async phase => {
  const f = await fixture(), writer = child(f.directory, 'phase', phase);
  expect((await message(writer, 'phase')).phase).toBe(phase);
  await kill(writer, 0);
  const rootNames = await readdir(f.directory), marker = join(f.directory, 'retention.sqlite');
  expect(rootNames).toContain('retention.sqlite');
  if (phase === 'marker-link') {
    const stages = await readdir(join(f.directory, '.staging'));
    expect(stages).toHaveLength(1);
    expect((await lstat(join(f.directory, '.staging', stages[0]!, 'retention.sqlite'))).ino).toBe((await lstat(marker)).ino);
    expect(rootNames).not.toContain('snapshots');
  } else {
    const names = await readdir(join(f.directory, 'snapshots'));
    expect(names.filter(name => name.startsWith('.stage-'))).toHaveLength(phase === 'catalogue-link' ? 0 : 1);
    expect(names.filter(name => /^[a-f0-9]{64}\.bin$/.test(name))).toHaveLength(phase === 'snapshot-stage' ? 0 : 1);
    expect(rootNames.includes('catalogue.json')).toBe(phase === 'catalogue-link');
    expect(rootNames.filter(name => name.startsWith('.stage-'))).toHaveLength(phase === 'catalogue-link' ? 1 : 0);
  }
  const saved = await f.store.publish(sealedSnapshot());
  expect(saved.status).toBe('stored');
  expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
  if (phase === 'marker-link') expect(await readdir(join(f.directory, '.staging'))).toHaveLength(1);
  else if (phase === 'catalogue-link') expect((await readdir(f.directory)).filter(name => name.startsWith('.stage-'))).toHaveLength(1);
  else expect((await readdir(join(f.directory, 'snapshots'))).filter(name => name.startsWith('.stage-'))).toHaveLength(1);
}, 15_000);
it('reclaims a real SQLite journal left by SIGKILL during an open transaction only when publishing', async () => {
  const f = await fixture(), writer = child(f.directory, 'journal');
  await message(writer, 'journal-open'); await kill(writer, 0);
  const staging = join(f.directory, '.staging'), [name] = await readdir(staging);
  const stage = join(staging, name!), journal = join(stage, 'retention.sqlite-journal');
  expect((await readFile(journal)).length).toBeGreaterThan(0);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  expect(await readdir(stage)).toContain('retention.sqlite-journal');
  expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
  expect(await readdir(staging)).toEqual([]);
}, 15_000);
it.each(['same', 'different'])('preserves a live publisher staging marker while another process publishes %s content', async content => {
  const f = await fixture(), writer = child(f.directory, 'live', content === 'different' ? 1 : 0);
  await message(writer, 'staged-marker');
  const before = await readdir(join(f.directory, '.staging'));
  expect(before).toHaveLength(1);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  expect(await readdir(join(f.directory, '.staging'))).toEqual(before);
  const publication = await f.store.publish(sealedSnapshot());
  writer.send('resume');
  if (content === 'same') expect((await message(writer, 'stored')).publication?.sha256).toBe(publication.sha256);
  else expect((await message(writer, 'refused')).code).toBe('SEALED_HISTORY_CONFLICT');
  expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
}, 15_000);
it('does not reclaim a live stage when a reader cannot see its publisher PID', async () => {
  const f = await fixture(), writer = child(f.directory, 'live');
  await message(writer, 'staged-marker');
  const staging = join(f.directory, '.staging'), [name] = await readdir(staging);
  const stage = join(staging, name!), before = await readFile(join(stage, 'retention.sqlite'));
  const visibility = vi.spyOn(process, 'kill').mockImplementation(() => { throw Object.assign(Error('outside reader PID namespace'), { code: 'ESRCH' }); });
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  expect(visibility).not.toHaveBeenCalled();
  expect(await readFile(join(stage, 'retention.sqlite'))).toEqual(before);
  visibility.mockRestore();
  writer.send('resume'); await message(writer, 'stored');
  expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
}, 15_000);
async function interruptedStage() {
  const f = await fixture(), writer = child(f.directory, 'checkpoint', 4);
  await message(writer, 'initializing'); await kill(writer, 0);
  const staging = join(f.directory, '.staging'), [name] = await readdir(staging);
  return { ...f, staging, stage: join(staging, name!), marker: join(staging, name!, 'retention.sqlite') };
}
it.each(['permissions', 'owner'])('preserves a dead stage file with unmanaged %s on the delete path', async change => {
  const f = await interruptedStage(), before = await readFile(f.marker);
  if (change === 'permissions') await chmod(f.marker, 0o640); else stageFault.path = f.marker;
  await expect(f.store.publish(sealedSnapshot())).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  expect(await readFile(f.marker)).toEqual(before);
}, 15_000);
it.each(['file', 'directory'])('preserves an unknown %s directly under staging on the delete path', async kind => {
  const f = await fixture(), staging = join(f.directory, '.staging'); await mkdir(staging, { mode: 0o700 });
  const entry = join(staging, 'notes.txt');
  if (kind === 'directory') await mkdir(entry, { mode: 0o700 });
  const notes = kind === 'directory' ? join(entry, 'retention.sqlite') : entry;
  await writeFile(notes, 'operator notes', { mode: 0o600 });
  await expect(f.store.publish(sealedSnapshot())).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  expect(await readFile(notes, 'utf8')).toBe('operator notes');
});
it('preserves a dead stage folder reporting another owner on the delete path', async () => {
  const f = await interruptedStage(), before = await readFile(f.marker); stageFault.path = f.stage;
  await expect(f.store.publish(sealedSnapshot())).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  expect(await readFile(f.marker)).toEqual(before);
}, 15_000);
it('preserves unknown staging contents and outside symlink targets, reclaiming only recognized dead-owner files', async () => {
  const f = await fixture(), writer = child(f.directory, 'checkpoint', 4);
  await message(writer, 'initializing'); await kill(writer, 0);
  const staging = join(f.directory, '.staging'), [name] = await readdir(staging);
  const stage = join(staging, name!), marker = join(stage, 'retention.sqlite'), original = await readFile(marker);
  const unknown = join(stage, 'operator-file'); await writeFile(unknown, 'keep this file', { mode: 0o600 });
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  await expect(f.store.publish(sealedSnapshot())).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  expect(await readFile(unknown, 'utf8')).toBe('keep this file');
  expect(await readFile(marker)).toEqual(original);
  await rm(unknown);
  const outside = await fixture(), target = join(outside.directory, 'outside'); await writeFile(target, 'outside bytes', { mode: 0o600 });
  const journal = join(stage, 'retention.sqlite-journal'); await symlink(target, journal);
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  expect(await readFile(target, 'utf8')).toBe('outside bytes');
  expect(await readFile(marker)).toEqual(original);
  // This synthetic file tests the reserved name; the journal test above uses SQLite.
  await rm(journal); await writeFile(journal, 'reserved-name fixture', { mode: 0o600 });
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  expect(await readdir(staging)).toHaveLength(1);
  expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
  expect(await readdir(staging)).toEqual([]);
}, 15_000);
