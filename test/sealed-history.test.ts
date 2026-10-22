import { chmod, mkdtemp, readdir, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it } from 'vitest';
import * as filesystem from '../src/storage/filesystem.js';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { digest } from '../src/validation.js';
import { operationHash } from '../src/storage/retainedRepository/commit.js';

const roots: string[] = [];
const create = (options: unknown) => filesystem.createSealedFilesystemHistory(options as filesystem.SealedHistoryOptions);
function active(options: unknown): filesystem.SealedFilesystemHistory {
  const store = create(options);
  if (!store) throw new Error('Expected an explicitly enabled test store');
  return store;
}
afterEach(async () => { for (const path of roots.splice(0)) await rm(path, { recursive: true, force: true }); });
async function fixture() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'sealed-history-'))); roots.push(directory);
  return { directory, options: { enabled: true, directory, retentionMs: 100, requireSeparateFilesystem: false } };
}
function snapshot(count = 2, references = 3) {
  const operations = Array.from({ length: count }, (_, i) => ({ operationHash: operationHash(`operation-${i}`),
    fingerprint: digest(`request-${i}`), writeHash: digest(`write-${i}`), acceptedAt: i * 10,
    expiresAt: i * 10 + 100, status: 'expired' as const, publication: 'committed' as const, dataDeleted: true }));
  const refs = Array.from({ length: count }, (_, i) => Array.from({ length: references }, (_, j) => ({
    refHash: digest({ kind: 'observation', id: `item-${i}-${j}` }), operationHash: operations[i]!.operationHash,
  }))).flat();
  return { operations, references: refs };
}

it('is absent when disabled without validation or namespace I/O', async () => {
  const f = await fixture();
  expect(create({ enabled: false })).toBeUndefined();
  expect(create({})).toBeUndefined();
  expect(await readdir(f.directory)).toEqual([]);
  expect(() => create({ ...f.options, requireSeparateFilesystem: 'no' })).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_INVALID_OPTIONS' }));
});
it('refuses altered marker ownership, policy, schema and nonempty guard tables', async () => {
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  for (const sql of [
    'CREATE TABLE unexpected(value TEXT)',
    "INSERT INTO refs VALUES('private-ref','private-operation')",
    'UPDATE policy SET retentionMs=200',
    'PRAGMA ignore_check_constraints=ON; INSERT INTO policy VALUES(2,2,100)',
    'UPDATE sealed_history SET version=99',
  ]) {
    const f = await fixture(), store = active(f.options); await store.publish(snapshot());
    const db = new DatabaseSync(join(f.directory, 'retention.sqlite'));
    try { db.exec(sql); } finally { db.close(); }
    await expect(store.open()).rejects.toBeInstanceOf(filesystem.SealedHistoryError);
  }
});
it('publishes a detached exact view and reconciles identical ordering changes', async () => {
  const f = await fixture(), store = active(f.options), input = snapshot();
  const saved = await store.publish(input);
  expect(saved).toMatchObject({ status: 'stored', operations: 2, references: 6 });
  expect(await store.publish({ operations: [...input.operations].reverse(), references: [...input.references].reverse() })).toEqual(saved);
  const view = await store.open();
  expect(view.lookupOperation('operation-0')).toEqual({ state: 'found', operation: input.operations[0] });
  expect(view.lookupReference({ kind: 'observation', id: 'item-1-2' })).toEqual({ state: 'found', operation: input.operations[1] });
  expect(view.receipt('operation-0')).toEqual({ state: 'found', receipt: { operationHash: input.operations[0]!.operationHash,
    acceptedAt: 0, expiresAt: 100, status: 'expired' as const, publication: 'committed' as const, dataDeleted: true } });
  expect(view.lookupOperation('foreign-operation')).toEqual({ state: 'not-in-snapshot' });
  expect(view.lookupReference({ kind: 'batch', id: 'item-1-2' })).toEqual({ state: 'not-in-snapshot' });
  expect(view.receipt('foreign-operation')).toEqual({ state: 'not-in-snapshot' });
  expect(Object.isFrozen(view)).toBe(true);
  expect(Object.isFrozen(view.lookupOperation('operation-0'))).toBe(true);
  input.operations[0]!.fingerprint = digest('different-request');
  expect(view.lookupOperation('operation-0')).toMatchObject({ state: 'found', operation: { acceptedAt: 0 } });
  await expect(store.publish(input as filesystem.SealedHistorySnapshot)).rejects.toMatchObject({ code: 'SEALED_HISTORY_CONFLICT' });
});
it('carries 6000 operation rows and 21000 exact native memberships without changing flags or hashes', async () => {
  const f = await fixture(), store = active(f.options), input = snapshot(6000, 3);
  for (let i = 0; i < 3000; i++) input.references.push({ refHash: digest({ kind: 'observation', id: `extra-${i}` }), operationHash: input.operations[i]!.operationHash });
  await store.publish(input);
  const view = await store.open();
  expect(view.counts).toEqual({ operations: 6000, references: 21000 });
  for (let i = 0; i < 6000; i++) expect(view.lookupOperation(`operation-${i}`)).toEqual({ state: 'found', operation: input.operations[i] });
  for (let i = 0; i < 6000; i++) for (let j = 0; j < 3; j++) expect(view.lookupReference({ kind: 'observation', id: `item-${i}-${j}` })).toEqual({ state: 'found', operation: input.operations[i] });
  for (let i = 0; i < 3000; i++) expect(view.lookupReference({ kind: 'observation', id: `extra-${i}` })).toEqual({ state: 'found', operation: input.operations[i] });
});
it('uses a durable v2 marker rejected by the byte-identical real 0.9 reader, never adopts a v1 namespace', async () => {
  const f = await fixture(), store = active(f.options); await store.publish(snapshot());
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  const db = new DatabaseSync(join(f.directory, 'retention.sqlite'), { readOnly: true });
  try { expect(db.prepare('SELECT version,retentionMs FROM policy').get()).toEqual({ version: 2, retentionMs: 100 }); }
  finally { db.close(); }
  const before = await readFile(join(f.directory, 'catalogue.json'));
  const metadataBefore = (await store.open()).lookupOperation('operation-0');
  const snapshotPath = join(f.directory, 'snapshots', JSON.parse(before.toString('utf8')).snapshot.sha256 + '.bin');
  const snapshotBefore = await readFile(snapshotPath);
  const markerPath = join(f.directory, 'retention.sqlite'), markerBefore = await readFile(markerPath);
  expect([...markerBefore.subarray(18, 20)]).toEqual([1, 1]);
  const journalMode = () => {
    const marker = new DatabaseSync(markerPath, { readOnly: true });
    try { return marker.prepare('PRAGMA journal_mode').get()?.journal_mode; } finally { marker.close(); }
  };
  expect(journalMode()).toBe('delete');
  await chmod(f.directory, 0o500);
  try { expect((await store.open()).counts).toEqual({ operations: 2, references: 6 }); }
  finally { await chmod(f.directory, 0o700); }
  const script = `import {RepositoryLedger} from ${JSON.stringify(new URL('./fixtures/retained-reader-0.9/ledger.ts', import.meta.url).href)};
    try { new RepositoryLedger(${JSON.stringify(f.directory)},100).close(); process.exitCode=1; }
    catch(error) { process.stdout.write(JSON.stringify({code:error.code})); }`;
  const child = await promisify(execFile)(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', script]);
  expect(JSON.parse(child.stdout)).toEqual({ code: 'REPOSITORY_INVALID_OPTIONS' });
  expect(await readFile(join(f.directory, 'catalogue.json'))).toEqual(before);
  expect(await readFile(snapshotPath)).toEqual(snapshotBefore);
  const markerAfter = await readFile(markerPath);
  expect(markerAfter.equals(markerBefore)).toBe(false);
  expect([...markerAfter.subarray(18, 20)]).toEqual([2, 2]);
  expect((await readdir(f.directory)).filter(name => /^retention\.sqlite-(wal|shm)$/.test(name))).toEqual([]);
  const reopened = await store.open();
  expect(reopened.counts.operations).toBe(2);
  expect(reopened.lookupOperation('operation-0')).toEqual(metadataBefore);
  expect((await readdir(f.directory)).filter(name => /^retention\.sqlite-(wal|shm)$/.test(name)).sort()).toEqual(['retention.sqlite-shm', 'retention.sqlite-wal']);
  expect(journalMode()).toBe('wal');
  await rm(markerPath + '-wal'); await rm(markerPath + '-shm'); await chmod(f.directory, 0o500);
  try { await expect(store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE', message: 'Sealed history operation was refused.' }); }
  finally { await chmod(f.directory, 0o700); }
  expect(await readFile(markerPath)).toEqual(markerAfter);
  expect(await readFile(snapshotPath)).toEqual(snapshotBefore);
  const g = await fixture(), legacy = createRetainedFilesystemRepository({ ...g.options, onHealth() {} })!;
  await legacy.findOperation('initialize-only'); legacy.close();
  await expect(active(g.options).publish(snapshot())).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
});
it('pins the legacy witness to the exact reviewed source, not a newly written imitation', async () => {
  for (const [name, hash] of [['ledger.ts', '20ecb724cdee597d936bffd5f7e64d2594ade90bf467b2bc242c3979767b5fac'], ['contracts.ts', '544b8c464912dc0b953fb7a0230d0445ed135c4e231c15cfff828ba4a01105e9']]) {
    const bytes = await readFile(new URL('./fixtures/retained-reader-0.9/' + name, import.meta.url));
    expect(createHash('sha256').update(bytes).digest('hex')).toBe(hash);
  }
});
it('refuses live, pending, uncertain and not-deleted inputs before making a namespace', async () => {
  const f = await fixture();
  for (const part of [{ status: 'stored' }, { status: 'pending' }, { publication: 'unconfirmed' }, { dataDeleted: false }]) {
    const input = snapshot(); Object.assign(input.operations[0]!, part);
    await expect(active(f.options).publish(input as filesystem.SealedHistorySnapshot)).rejects.toMatchObject({ code: 'SEALED_HISTORY_INVALID_SNAPSHOT' });
    expect(await readdir(f.directory)).toEqual([]);
  }
});
it('refuses duplicate, foreign, malformed and oversized metadata without dropping a prefix', async () => {
  const f = await fixture(), a = snapshot();
  const bad = [
    { ...a, operations: [...a.operations, a.operations[0]] },
    { ...a, references: [...a.references, a.references[0]] },
    { ...a, references: [{ refHash: digest('foreign'), operationHash: digest('absent-owner') }] },
    { ...a, operations: [{ ...a.operations[0], expiresAt: 999 }] },
    { ...a, operations: [{ ...a.operations[0], fingerprint: 'private error text' }] },
  ];
  for (const input of bad) await expect(active(f.options).publish(input as filesystem.SealedHistorySnapshot)).rejects.toMatchObject({ code: 'SEALED_HISTORY_INVALID_SNAPSHOT' });
  await expect(active({ ...f.options, maxSnapshotBytes: 128 }).publish(a)).rejects.toMatchObject({ code: 'SEALED_HISTORY_LIMIT' });
  expect(await readdir(f.directory)).toEqual([]);
});
it('refuses sparse and accessor array entries without inventing memberships or invoking accessors', async () => {
  const f = await fixture(), a = snapshot();
  const indexed = [...a.references]; let invoked = 0;
  Object.defineProperty(indexed, '0', { get() { invoked++; return a.references[0]; } });
  for (const input of [
    { ...a, references: Array(1) },
    { ...a, operations: Array(1) },
    { ...a, references: indexed },
  ]) await expect(active(f.options).publish(input as filesystem.SealedHistorySnapshot)).rejects.toMatchObject({ code: 'SEALED_HISTORY_INVALID_SNAPSHOT' });
  expect(invoked).toBe(0);
  expect(await readdir(f.directory)).toEqual([]);
});
it('refuses nonempty unmarked paths, unavailable snapshots and changed or unsupported saved formats with fixed errors', async () => {
  const f = await fixture(); await writeFile(join(f.directory, 'unmanaged'), 'private/path/token');
  await expect(active(f.options).publish(snapshot())).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNMANAGED' });
  const g = await fixture(), store = active(g.options);
  await expect(store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNAVAILABLE' });
  const saved = await store.publish(snapshot());
  const cataloguePath = join(g.directory, 'catalogue.json');
  const catalogue = JSON.parse(await readFile(cataloguePath, 'utf8'));
  const file = join(g.directory, 'snapshots', catalogue.snapshot.sha256 + '.bin');
  const original = await readFile(file); const changed = Buffer.from(original); changed[64] = changed[64]! ^ 1;
  await writeFile(file, changed);
  await expect(store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_CORRUPT', message: 'Sealed history operation was refused.' });
  await writeFile(file, original);
  await writeFile(cataloguePath, JSON.stringify({ ...catalogue, version: 999 }));
  await expect(store.open()).rejects.toMatchObject({ code: 'SEALED_HISTORY_UNSUPPORTED_FORMAT' });
  expect(saved.status).toBe('stored');
});
