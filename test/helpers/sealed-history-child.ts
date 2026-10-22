/** Timing controls live only in this child: all cuts are actual parent SIGKILLs. */
import { createRequire, syncBuiltinESMExports } from 'node:module';
import { sealedSnapshot } from './sealed-history-fixture.js';

const [directory, mode, pointText] = process.argv.slice(2);
if (!directory) throw Error('Missing child namespace');
const require = createRequire(import.meta.url);
if (mode === 'checkpoint' || mode === 'journal') {
  const { DatabaseSync } = require('node:sqlite') as typeof import('node:sqlite');
  const exec = DatabaseSync.prototype.exec;
  let point = 0;
  DatabaseSync.prototype.exec = function (sql) {
    if (mode === 'journal' && sql.startsWith('CREATE TABLE policy')) {
      exec.call(this, 'BEGIN IMMEDIATE');
      exec.call(this, sql);
      process.send!({ state: 'journal-open' });
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
      return;
    }
    exec.call(this, sql);
    if (++point === Number(pointText)) {
      process.send!({ state: 'initializing', point });
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30_000);
    }
  };
}
if (mode === 'live' || mode === 'phase') {
  const fs = require('node:fs/promises') as typeof import('node:fs/promises');
  const link = fs.link, open = fs.open;
  const pause = async (phase: string) => {
    process.send!({ state: 'phase', phase });
    await new Promise<void>(resolve => process.once('message', () => resolve()));
  };
  fs.link = async (source, target) => {
    if (mode === 'live' && String(target).endsWith('/retention.sqlite')) {
      process.send!({ state: 'staged-marker' });
      await new Promise<void>(resolve => process.once('message', () => resolve()));
    }
    await link(source, target);
    if (mode === 'phase' && ((pointText === 'marker-link' && String(target).endsWith('/retention.sqlite')) ||
        (pointText === 'snapshot-link' && String(target).endsWith('.bin')) ||
        (pointText === 'catalogue-link' && String(target).endsWith('/catalogue.json')))) await pause(pointText);
  };
  fs.open = async (...args) => {
    const handle = await open(...args);
    if (mode !== 'phase' || pointText !== 'snapshot-stage' || !String(args[0]).includes('/snapshots/.stage-')) return handle;
    return new Proxy(handle, { get(target, key) {
      if (key === 'writeFile') return async (...writeArgs: Parameters<typeof handle.writeFile>) => {
        await target.writeFile(...writeArgs); await pause('snapshot-stage');
      };
      const value = Reflect.get(target, key, target) as unknown;
      return typeof value === 'function' ? value.bind(target) : value;
    } });
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
