import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, realpathSync, rmSync, lstatSync, statfsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { createRetainedFilesystemRepository } from '../src/storage/filesystem.js';
import { seedDay, expiredFilesGone, originalId } from './historyVolume/seed.js';
import { options, DAY } from './historyVolume/fixture.js';
const daily = Number(process.argv[2] ?? 241920), days = Number(process.argv[3] ?? 12), refs = Number(process.argv[4] ?? 3);
if (![daily, days, refs].every(Number.isSafeInteger) || daily < 1 || daily > 241920 || (days !== 1 && (days < 9 || days > 18)) || refs < 1 || refs > 16) throw Error('Use 1–241920 operations/day, 1 or 9–18 days, 1–16 references.');
const directory = realpathSync(mkdtempSync(join(tmpdir(), 'history-volume-'))), start = performance.now();
let age = 0, phase = 'setup';
const cancellation = new AbortController();
process.once('SIGTERM', () => cancellation.abort()); process.once('SIGINT', () => cancellation.abort());
const health: string[] = [], locks: number[] = [], samples: unknown[] = [], deadlines = new WeakMap<DatabaseSync, number>();
const repo = createRetainedFilesystemRepository(options(directory, daily, refs, () => age, event => health.push(event.code)))!;
let db: DatabaseSync | undefined, child: ReturnType<typeof fork> | undefined, probe: unknown;
const exec = DatabaseSync.prototype.exec;
DatabaseSync.prototype.exec = function (sql: string) {
  const result = exec.call(this, sql);
  if (phase === 'retirement') {
    if (sql === 'BEGIN IMMEDIATE') deadlines.set(this, performance.now());
    else if (sql === 'COMMIT' && deadlines.has(this)) { locks.push(performance.now() - deadlines.get(this)!); deadlines.delete(this); }
  }
  return result;
};
try {
  await repo.historyStatus();
  db = new DatabaseSync(join(directory, 'retention-v4.sqlite'));
  db.exec('PRAGMA foreign_keys=ON; PRAGMA synchronous=EXTRA; PRAGMA cache_spill=OFF; PRAGMA cache_size=-8192');
  for (let day = 0; day < (days === 1 ? 2 : days); day++) {
    cancellation.signal.throwIfAborted();
    const period = days === 1 && day === 1 ? 8 : day; age = period * DAY;
    await repo.historyStatus();
    if (period >= 8) {
      phase = 'setup'; expiredFilesGone(db, period - 8);
      if (period === 8) {
        child = fork(fileURLToPath(new URL('./historyVolume/publisher.ts', import.meta.url)), [], { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
        child.send({ directory, daily, references: refs, period });
        const ready = (await once(child, 'message'))[0];
        if (!(ready as { ready?: boolean }).ready) throw Error('Concurrent publisher did not start');
      }
      phase = 'retirement'; const at = locks.length, tick = performance.now(); let passes = 0;
      for (;;) {
        cancellation.signal.throwIfAborted();
        const batch = await repo.retireHistory({ limit: 100 }); passes++;
        if (batch.blocked) throw Error('Terminal metadata fixture unexpectedly blocked');
        if (!batch.pending) break;
        if (passes > 1000000) throw Error('Maintenance did not converge');
      }
      phase = 'setup';
      if (child) {
        const done = once(child, 'message'), exited = once(child, 'exit'); child.send({ stop: true }); probe = (await done)[0]; await exited; child = undefined;
        if ((probe as { failed?: number }).failed !== 0) throw Error('Concurrent publications failed');
      }
      const batchLocks = locks.slice(at);
      console.error(JSON.stringify({ retiredDay: period - 8, passes, elapsedMs: performance.now() - tick, maximumLockMs: Math.max(...batchLocks) }));
      await repo.findOperation(originalId(period - 8, 0)).then(() => { throw Error('Retired ID revived'); }, error => {
        if (error.code !== 'REPOSITORY_HISTORY_UNAVAILABLE') throw error;
      });
    }
    phase = 'setup';
    if (!(days === 1 && day === 1)) seedDay(db, directory, period, daily, refs, cancellation.signal);
    const info = lstatSync(join(directory, 'retention-v4.sqlite')), free = statfsSync(directory, { bigint: true });
    const sample = { period, operations: await repo.metadataCapacity({ operations: 0, references: 0 }), checkpoint: await repo.historyStatus(),
      mainLogicalBytes: info.size, mainAllocatedBytes: info.blocks * 512, freeBytes: Number(free.bavail * free.bsize), rss: process.memoryUsage().rss };
    samples.push(sample); console.error(JSON.stringify({ period, operations: sample.operations.operations, references: sample.operations.references, mainBytes: info.size, elapsedMs: performance.now() - start }));
  }
  locks.sort((a, b) => a - b);
  if (Math.max(0, ...locks) >= 1000 || health.length) throw Error('Retirement exceeded unchanged admission window or emitted health failures');
  console.log(JSON.stringify({ kind: 'synthetic-repository-metadata-volume', bulkSetup: { transactionOperations: 10000, connectionCacheKiB: 8192, productionConnectionUnchanged: true }, dailyOperations: daily, seededDays: days, referencesPerOperation: refs,
    keepDays: 7, transactionWorkLimit: { operations: 100, references: 1600, targetWorkMs: 10 }, elapsedMs: performance.now() - start,
    lock: { transactions: locks.length, p95Ms: locks[Math.max(0, Math.ceil(locks.length * .95) - 1)] ?? 0, maxMs: locks.at(-1) ?? 0 }, probe, samples,
    scope: 'Actual SQLite metadata and bounded retirement, bulk seeded to the stated volume. Files-gone states are seeded, not a payload-deletion/real-device/fleet-throughput benchmark. A separate process publishes real native records concurrently during the first retirement.' }, null, 2));
} finally {
  DatabaseSync.prototype.exec = exec;
  if (child && child.exitCode === null && child.signalCode === null) { const exit = once(child, 'exit'); child.kill('SIGKILL'); await exit; }
  db?.close(); repo.close(); rmSync(directory, { recursive: true, force: true });
}
