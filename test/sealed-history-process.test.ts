import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout } from 'node:timers/promises';
import { afterEach, expect, it } from 'vitest';
import { createSealedFilesystemHistory } from '../src/storage/sealedHistory/index.js';
import { sealedSnapshot } from './helpers/sealed-history-fixture.js';

const roots: string[] = [], children = new Set<ChildProcess>();
afterEach(async () => {
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
function child(directory: string, mode: string, point = 0) {
  const process = spawn(globalThis.process.execPath, ['--import', 'tsx', fileURLToPath(new URL('./helpers/sealed-history-child.ts', import.meta.url)), directory, mode, String(point)],
    { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  children.add(process);
  return process;
}
async function message(process: ChildProcess, state: string) {
  const [value] = await once(process, 'message', { signal: AbortSignal.timeout(10_000) });
  expect(value).toMatchObject({ state });
  return value as { state: string; code?: string; publication?: { sha256: string } };
}
async function kill(process: ChildProcess, delay: number) {
  await setTimeout(delay);
  const exited = once(process, 'exit');
  expect(process.kill('SIGKILL')).toBe(true);
  expect(await exited).toEqual([null, 'SIGKILL']);
}
it('recovers real SIGKILLs at sampled SQLite initialization boundaries without adopting unknown files', async () => {
  // A fixed shuffled sample keeps timing coverage reproducible in CI.
  for (const [index, point] of [4, 1, 7, 2, 6, 3, 5].entries()) {
    const f = await fixture(), writer = child(f.directory, 'checkpoint', point);
    await message(writer, 'initializing');
    await kill(writer, (index * 7 + 3) % 5);
    await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
    expect(await readdir(join(f.directory, '.staging'))).toEqual([]);
    expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
    expect((await f.store.open()).counts).toEqual({ operations: 1, references: 1 });
  }
}, 30_000);
it('retries genuine process kills at varied delays during first publication', async () => {
  for (const delay of [0, 3, 1, 5, 2, 4]) {
    const f = await fixture(), writer = child(f.directory, 'race');
    await message(writer, 'ready'); writer.send('publish');
    await kill(writer, delay);
    expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
  }
}, 30_000);
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
  await rm(journal); await writeFile(journal, 'interrupted SQLite journal', { mode: 0o600 });
  await expect(f.store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  expect(await readdir(staging)).toEqual([]);
  expect(await f.store.publish(sealedSnapshot())).toMatchObject({ status: 'stored' });
}, 15_000);
