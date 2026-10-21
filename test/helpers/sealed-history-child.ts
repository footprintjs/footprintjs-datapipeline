/** Timing controls live only in this child: all cuts are actual parent SIGKILLs. */
import { createRequire, syncBuiltinESMExports } from 'node:module';
import { sealedSnapshot } from './sealed-history-fixture.js';

const [directory, mode, pointText] = process.argv.slice(2);
if (!directory) throw Error('Missing child namespace');
const require = createRequire(import.meta.url);
if (mode === 'checkpoint') {
  const { DatabaseSync } = require('node:sqlite') as typeof import('node:sqlite');
  const exec = DatabaseSync.prototype.exec;
  let point = 0;
  DatabaseSync.prototype.exec = function (sql) {
    exec.call(this, sql);
    if (++point === Number(pointText)) {
      process.send!({ state: 'initializing', point });
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
    }
  };
}
if (mode === 'live') {
  const fs = require('node:fs/promises') as typeof import('node:fs/promises');
  const link = fs.link;
  fs.link = async (source, target) => {
    if (String(target).endsWith('/retention.sqlite')) {
      process.send!({ state: 'staged-marker' });
      await new Promise<void>(resolve => process.once('message', () => resolve()));
    }
    return link(source, target);
  };
  syncBuiltinESMExports();
}
const { createSealedFilesystemHistory } = await import('../../src/storage/sealedHistory/index.js');
const store = createSealedFilesystemHistory({ enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false })!;
if (mode === 'race') {
  process.send!({ state: 'ready' });
  await new Promise<void>(resolve => process.once('message', () => resolve()));
}
try {
  const snapshot = sealedSnapshot();
  if (mode === 'live' && pointText === '1') snapshot.operations[0]!.fingerprint = '0'.repeat(64);
  const publication = await store.publish(snapshot);
  process.send!({ state: 'stored', publication });
  if (mode === 'race') setInterval(() => {}, 1000);
  else process.disconnect!();
} catch (error) {
  process.send!({ state: 'refused', code: (error as { code?: string }).code });
  process.exitCode = 1;
  process.disconnect!();
}
