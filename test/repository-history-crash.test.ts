import { fork } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { readdir, access } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { join } from 'node:path';
import { expect, it } from 'vitest';
import { DAY, fixture, scopedWrite, bytes, request, drainHistory } from './helpers/history-repository.js';
async function killAt(directory: string, phase: string) {
  const child = fork(fileURLToPath(new URL('./helpers/history-crash-child.ts', import.meta.url)), [directory, phase],
    { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  try {
    const response = (await once(child, 'message'))[0];
    expect(response).toEqual({ ready: true });
    const stopped = once(child, 'exit'); child.kill('SIGKILL');
    expect((await stopped)[1]).toBe('SIGKILL');
  } finally { if (child.exitCode === null && child.signalCode === null) { const stopped = once(child, 'exit'); child.kill('SIGKILL'); await stopped; } }
}
it('resumes a killed first initialization without deleting a lock or resetting an active namespace', async () => {
  const f = await fixture();
  const child = fork(fileURLToPath(new URL('./helpers/history-crash-child.ts', import.meta.url)), [f.directory, 'initialization'],
    { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  try {
    expect((await once(child, 'exit'))[1]).toBe('SIGKILL');
    const fence = new DatabaseSync(join(f.directory, 'retention.sqlite'));
    try { expect(fence.prepare('SELECT state FROM history_namespace WHERE id=1').get()).toMatchObject({ state: 'initializing' }); }
    finally { fence.close(); }
    await expect(access(join(f.directory, 'retention-v4.sqlite'))).rejects.toMatchObject({ code: 'ENOENT' });
    const resumed = f.open(0);
    expect((await resumed.pipeline.collect(request('after-kill'))).commit.status).toBe('committed');
  } finally { if (child.exitCode === null && child.signalCode === null) { const exit = once(child, 'exit'); child.kill('SIGKILL'); await exit; } await f.close(); }
});
it.each(['before-commit', 'after-commit'])('keeps one durable fence after a real SIGKILL %s', async phase => {
  const f = await fixture(), first = f.open(0);
  try {
    const write = await scopedWrite(first.repository, 'saved'); await first.repository.commit(write);
    f.at(DAY); await first.repository.sweep({ limit: 100 });
    f.at(8 * DAY); await first.repository.historyStatus(); first.repository.close();
    await killAt(f.directory, phase);
    const reopened = f.open(8);
    if (phase === 'before-commit') {
      expect(await reopened.repository.historyStatus()).toMatchObject({ retireBefore: 0, generation: 0 });
      expect(await reopened.repository.receipt(write.operationId)).toMatchObject({ dataDeleted: true });
      await drainHistory(reopened.repository, 1);
    }
    expect(await reopened.repository.historyStatus()).toMatchObject({ retireBefore: 1, generation: 1 });
    await expect(reopened.repository.findOperation(write.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    await drainHistory(reopened.repository, 1);
    expect(await reopened.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
  } finally { await f.close(); }
});
it('two killed retries retain one bounded stage, one intent, and the same frozen operation', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const write = await scopedWrite(first.repository, 'retry');
    const measured: number[] = [];
    for (let i = 0; i < 2; i++) {
      await killAt(f.directory, 'staging');
      expect((await readdir(join(f.directory, 'operations'))).filter(name => name.startsWith('.stage-'))).toHaveLength(1);
      expect(await first.repository.receipt(write.operationId)).toMatchObject({ status: 'pending', publication: 'unconfirmed' });
      expect(await first.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 1, references: 3 });
      measured.push(await bytes(f.directory));
    }
    expect(Math.max(...measured) - Math.min(...measured)).toBeLessThanOrEqual(8192);
    expect(await first.repository.commit(write)).toMatchObject({ status: 'committed', operationId: write.operationId });
    expect((await readdir(join(f.directory, 'operations'))).filter(name => name.startsWith('.stage-'))).toEqual([]);
    expect(await first.repository.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
  } finally { await f.close(); }
});

it('resumes a committed verification cursor after SIGKILL without double-counting it', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const write = await scopedWrite(first.repository, 'cursor'); await first.repository.commit(write);
    f.at(DAY); await first.repository.sweep({ limit: 100 }); f.at(8 * DAY);
    await first.repository.historyStatus(); first.repository.close();
    await killAt(f.directory, 'after-progress');
    const resumed = f.open(8); expect(await resumed.repository.historyStatus()).toMatchObject({ retireBefore: 0, retiredOperations: 0 });
    await drainHistory(resumed.repository, 1);
    expect(await resumed.repository.historyStatus()).toMatchObject({ retireBefore: 1, retiredOperations: 1, retiredReferences: 3 });
    await expect(resumed.repository.findOperation(write.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
  } finally { await f.close(); }
});
