import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createRetainedFilesystemRepository } from '../../src/storage/filesystem.js';
import { RepositoryLedger } from '../../src/storage/retainedRepository/ledger.js';
import { IndexedData } from '../../src/storage/retainedRepository/indexedData.js';
import { DatabaseSync } from 'node:sqlite';
import { scopedWrite, DAY, namespaceId } from './history-repository.js';
const [directory, phase] = process.argv.slice(2);
const repo = createRetainedFilesystemRepository({ enabled: true, directory: directory!, retentionMs: DAY,
  expiryClock: 'verified-uptime', clock: () => 1_790_000_000_000, monotonicClock: () => 0,
  requireSeparateFilesystem: false, metadataLimits: { maxBytes: 2 * 1024 * 1024, maxOperations: 80, maxReferences: 240 },
  historyRetirement: { profile: 'daily-periods-v1', keepDays: 7, namespaceId, period: ['staging', 'initialization'].includes(phase!) ? 0 : 8, maxOperationBytes: 65536 }, onHealth() {} })!;
const pause = () => { process.send!({ ready: true }); return new Promise<never>(() => {}); };
try {
  if (phase === 'initialization') {
    const exec = DatabaseSync.prototype.exec;
    DatabaseSync.prototype.exec = function (sql: string) {
      exec.call(this, sql);
      if (sql === 'COMMIT') process.kill(process.pid, 'SIGKILL'); // committed initializing fence, before actual ledger
    };
    await repo.historyStatus();
  } else if (phase === 'before-commit') {
    const retire = RepositoryLedger.prototype.retireHistory;
    Object.defineProperty(RepositoryLedger.prototype, 'retireHistory', { value: async function (this: RepositoryLedger, limit: number) {
      const result = retire.call(this, limit);
      return result.retireBefore > 0 ? pause() : result; // only the final floor transaction is paused
    } });
    while ((await repo.retireHistory({ limit: 1 })).retireBefore === 0) { /* bounded committed progress */ }
  } else if (phase === 'after-commit') {
    while ((await repo.retireHistory({ limit: 1 })).retireBefore === 0) { /* bounded committed progress */ }
    await pause();
  } else if (phase === 'after-progress') {
    await repo.retireHistory({ limit: 1 }); await pause();
  } else {
    // A process-kill cut at stable stage creation, with a bounded partial file.
    // Publication has not run and the original durable intent remains uncertain.
    const stage = IndexedData.prototype['createStagingDirectory'];
    Object.defineProperty(IndexedData.prototype, 'createStagingDirectory', { value: async function (this: IndexedData, id: string) {
      const path = await stage.call(this, id);
      await writeFile(join(path, 'operation.json.gz'), Buffer.alloc(512, 1));
      return pause();
    } });
    await repo.commit(await scopedWrite(repo, 'retry'));
  }
} catch (error) { process.send!({ failed: true, error: error instanceof Error ? error.message : 'unknown' }, () => { process.exitCode = 1; process.disconnect(); }); }
