/** Offline exact replay only. Inputs are closed synthetic diagnostic exports. */
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdir, readFile, readdir, stat, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { deepStrictEqual, strictEqual } from 'node:assert';
import type { RecordRef } from '../src/contracts.js';
import { createSealedFilesystemHistory, type SealedHistoryOperation } from '../src/storage/filesystem.js';

interface OldRow extends Omit<SealedHistoryOperation, 'publication' | 'dataDeleted'> { confirmed: number; dataDeleted: number }
interface Namespace {
  path: string; sourceSha256: string; sourceBytes: number; policy: { retentionMs: number };
  operations: OldRow[]; refs: { refHash: string; operationHash: string }[];
}
interface Export { cases: { name: string; namespaces: Record<string, Namespace> }[] }
interface Native { cases: { name: string; rows: { namespace: string; operationId: string; operationHash: string; refs: RecordRef[] }[] }[] }
const sha256 = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex');
function metadata(row: OldRow): SealedHistoryOperation {
  const { confirmed, dataDeleted, ...fields } = row;
  if (![0, 1].includes(confirmed) || ![0, 1].includes(dataDeleted)) throw Error('Invalid closed diagnostic flags');
  return { ...fields, publication: confirmed ? 'committed' : 'unconfirmed', dataDeleted: dataDeleted === 1 };
}
async function input(path: string): Promise<Buffer> {
  if ((await stat(path)).size > 32 * 1024 * 1024) throw Error('Diagnostic export exceeds 32 MiB');
  const bytes = await readFile(path);
  if (bytes.length > 32 * 1024 * 1024) throw Error('Diagnostic export grew beyond 32 MiB');
  return bytes;
}
async function files(directory: string, prefix = ''): Promise<{ name: string; bytes: number; allocatedBytes: number }[]> {
  const result: { name: string; bytes: number; allocatedBytes: number }[] = [];
  for (const entry of await readdir(join(directory, prefix), { withFileTypes: true })) {
    const name = join(prefix, entry.name);
    if (entry.isDirectory()) result.push(...await files(directory, name));
    else {
      const info = await stat(join(directory, name));
      result.push({ name, bytes: info.size, allocatedBytes: info.blocks * 512 });
    }
  }
  return result.sort((a, b) => a.name < b.name ? -1 : 1);
}
function pages(path: string): Record<string, unknown> {
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  const db = new DatabaseSync(path, { readOnly: true });
  try { return { ...db.prepare('PRAGMA page_size').get(), ...db.prepare('PRAGMA page_count').get(), ...db.prepare('PRAGMA freelist_count').get() }; }
  finally { db.close(); }
}
const [exportPath, nativePath, outputPath] = process.argv.slice(2);
if (!exportPath || !nativePath || !outputPath) throw Error('Usage: measure-sealed-history.ts closed-ledgers.json public-id-capture.json NEW_OUTPUT_DIRECTORY');
const exported = await input(exportPath), captured = await input(nativePath);
const history = JSON.parse(exported.toString('utf8')) as Export, native = JSON.parse(captured.toString('utf8')) as Native;
const output = resolve(outputPath); await mkdir(output, { mode: 0o700 });
const cases: unknown[] = [];
for (const sample of history.cases) {
  if (!/^[a-z0-9-]+$/.test(sample.name)) throw Error('Invalid local sample name');
  const identities = native.cases.find(item => item.name === sample.name);
  if (!identities) throw Error('Missing public API identity witness');
  const measurements = [];
  for (const [name, source] of Object.entries(sample.namespaces)) {
    if (!/^[a-z0-9-]+$/.test(name)) throw Error('Invalid local namespace');
    const original = await input(source.path);
    strictEqual(sha256(original), source.sourceSha256); strictEqual(original.length, source.sourceBytes);
    const directory = join(output, sample.name + '-' + name); await mkdir(directory, { mode: 0o700 });
    const operations = source.operations.map(metadata), expected = new Map(operations.map(row => [row.operationHash, row]));
    const store = createSealedFilesystemHistory({ enabled: true, directory, retentionMs: source.policy.retentionMs, requireSeparateFilesystem: false })!;
    const saved = await store.publish({ operations, references: source.refs });
    const view = await store.open(); let operationChecks = 0, referenceChecks = 0;
    for (const row of identities.rows.filter(row => row.namespace === name)) {
      const actual = view.lookupOperation(row.operationId); deepStrictEqual(actual, expected.get(row.operationHash)); operationChecks++;
      for (const ref of row.refs) {
        // Native refs only; logical-key/test-helper additions are not hashed.
        deepStrictEqual(view.lookupReference({ kind: ref.kind, id: ref.id }), actual); referenceChecks++;
      }
    }
    strictEqual(operationChecks, operations.length); strictEqual(referenceChecks, source.refs.length);
    strictEqual(view.lookupOperation('foreign-operation'), undefined);
    strictEqual(view.lookupReference({ kind: 'observation', id: 'altered-or-foreign' }), undefined);
    const stored = await files(directory);
    measurements.push({ namespace: name, originalBytes: source.sourceBytes, originalSha256: source.sourceSha256,
      retentionMs: source.policy.retentionMs, saved, operationChecks, referenceChecks, files: stored,
      totalBytes: stored.reduce((sum, file) => sum + file.bytes, 0),
      fileAllocatedBytes: stored.reduce((sum, file) => sum + file.allocatedBytes, 0), markerPages: pages(join(directory, 'retention.sqlite')) });
    strictEqual(sha256(await input(source.path)), source.sourceSha256);
  }
  cases.push({ name: sample.name, namespaces: measurements });
}
const runtimePaths = [
  ...((await readdir(new URL('../src/storage/sealedHistory/', import.meta.url))).filter(name => name.endsWith('.ts'))
    .map(name => 'src/storage/sealedHistory/' + name)),
  'src/storage/filesystem.ts', 'src/storage/filesystemRecords.ts', 'src/storage/retainedRepository/commit.ts', 'src/validation.ts',
  'examples/measure-sealed-history.ts',
].sort();
const codeHashes: { path: string; sha256: string }[] = [];
for (const path of runtimePaths) codeHashes.push({ path, sha256: sha256(await readFile(new URL('../' + path, import.meta.url))) });
const report = { scope: 'LOCAL SEED; exact terminal metadata snapshots only; no compaction or native cause replay',
  nodeVersion: process.version, codeHashes,
  inputs: { closedLedgersSha256: sha256(exported), publicIdentitiesSha256: sha256(captured) },
  format: { headerBytes: 64, operationBytes: 115, referenceBytes: 36, footerBytes: 32, historyHorizon: null },
  cases, exclusions: ['No source payloads or native traces recovered', 'No hot routing, migration or deletion', 'No fleet capacity or storage rate measured'] };
await writeFile(join(output, 'measurement.json'), JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
console.log(JSON.stringify({ output: join(output, 'measurement.json'), cases: cases.length }));
