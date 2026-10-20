import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { gzipSync, gunzipSync } from 'node:zlib';
import { afterEach, expect, it } from 'vitest';
import { createFilesystemTraceStore } from '../src/storage/filesystem.js';
import { flowChart, decide, type RuntimeSnapshot } from 'footprintjs';
import { keysReadFromExecutionTree, findLastWriter, type ControlDependency } from 'foottrace';
import { createRecordGraph, explain } from 'foottrace/cause';
import { runWindowFlow } from '../src/index.js';

const dirs: string[] = [];
async function directory() { const path = await mkdtemp(join(tmpdir(), 'trace-compression-')); dirs.push(path); return path; }
afterEach(async () => { for (const path of dirs.splice(0)) await rm(path, { recursive: true, force: true }); });
const trace = { operationId: 'synthetic', recordedAt: '2026-10-20T12:00:00.000Z', value: { source: 'LOCAL SEED', rows: Array.from({ length: 1000 }, (_, i) => ({ i, text: 'repeated evidence '.repeat(20) })) } };

it('gzip stores the same immutable uncompressed fingerprint and reopens the exact JSON in a fresh default reader', async () => {
  const plainPath = await directory(), compressedPath = await directory();
  const plain = createFilesystemTraceStore({ directory: plainPath });
  const compressed = createFilesystemTraceStore({ directory: compressedPath, compression: 'gzip' });
  const ref = await plain.store(trace);
  expect(await compressed.store(trace)).toEqual(ref);
  const original = await readFile(join(plainPath, 'traces', `${ref.id}.json`));
  const smaller = await readFile(join(compressedPath, 'traces', `${ref.id}.json.gz`));
  expect(gunzipSync(smaller)).toEqual(original);
  expect(smaller.length).toBeLessThan(original.length);
  expect(await createFilesystemTraceStore({ directory: compressedPath }).read(ref)).toEqual(trace);
  expect(await compressed.store(trace)).toEqual(ref);
  expect(await readdir(join(compressedPath, 'traces'))).toEqual([`${ref.id}.json.gz`]);
});

it('new compression settings read existing plain traces without rewriting them', async () => {
  const path = await directory();
  const ref = await createFilesystemTraceStore({ directory: path }).store(trace);
  const bytes = await readFile(join(path, 'traces', `${ref.id}.json`));
  const compressed = createFilesystemTraceStore({ directory: path, compression: 'gzip' });
  expect(await compressed.read(ref)).toEqual(trace);
  expect(await compressed.store(trace)).toEqual(ref);
  expect(await readdir(join(path, 'traces'))).toEqual([`${ref.id}.json`]);
  expect(await readFile(join(path, 'traces', `${ref.id}.json`))).toEqual(bytes);
});

it('rejects damaged gzip and well-formed compressed bytes with the wrong original fingerprint', async () => {
  const path = await directory(), store = createFilesystemTraceStore({ directory: path, compression: 'gzip' });
  const ref = await store.store(trace), file = join(path, 'traces', `${ref.id}.json.gz`);
  const bytes = await readFile(file);
  await writeFile(file, bytes.subarray(0, bytes.length - 4));
  await expect(store.read(ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
  await writeFile(file, gzipSync('{}'));
  await expect(store.read(ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
  await expect(store.store(trace)).rejects.toMatchObject({ code: 'storage_corrupt' });
});

it('bounds decompression even for tiny compressed inputs and refuses oversized writes before publication', async () => {
  const path = await directory();
  const ref = await createFilesystemTraceStore({ directory: path, compression: 'gzip' }).store(trace);
  const bounded = createFilesystemTraceStore({ directory: path, compression: 'gzip', maxTraceBytes: 128 });
  await expect(bounded.read(ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
  const empty = await directory();
  await expect(createFilesystemTraceStore({ directory: empty, compression: 'gzip', maxTraceBytes: 128 }).store(trace)).rejects.toMatchObject({ code: 'trace_limit' });
  await expect(readdir(join(empty, 'traces'))).rejects.toMatchObject({ code: 'ENOENT' });
});

it('rejects invalid codec settings and keeps missing distinct from corrupt', async () => {
  const path = await directory();
  for (const maximum of [0, NaN, 1.5, 16 * 1024 * 1024 + 1]) expect(() => createFilesystemTraceStore({ directory: path, maxTraceBytes: maximum })).toThrow();
  expect(() => createFilesystemTraceStore({ directory: path, compression: 'unknown' as 'gzip' })).toThrow();
  expect(await createFilesystemTraceStore({ directory: path }).read({ kind: 'trace', id: 'a'.repeat(64) })).toBeUndefined();
});

it('does not accept altered invalid UTF-8 as the same replacement character', async () => {
  const path = await directory(), store = createFilesystemTraceStore({ directory: path, compression: 'gzip' });
  const ref = await store.store({ ...trace, value: '\ufffd' });
  const file = join(path, 'traces', `${ref.id}.json.gz`);
  const raw = gunzipSync(await readFile(file));
  const index = raw.indexOf(Buffer.from('\ufffd'));
  expect(index).toBeGreaterThan(-1);
  await writeFile(file, gzipSync(Buffer.concat([raw.subarray(0, index), Buffer.from([255]), raw.subarray(index + 3)])));
  await expect(store.read(ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
});

it('rejects compressed input beyond the read ceiling before inflating it', async () => {
  const path = await directory(), store = createFilesystemTraceStore({ directory: path, compression: 'gzip' });
  const ref = await store.store(trace);
  await writeFile(join(path, 'traces', `${ref.id}.json.gz`), Buffer.alloc(70 * 1024));
  await expect(createFilesystemTraceStore({ directory: path, maxTraceBytes: 128 }).read(ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
});

it('reopens a compressed native window with exactly the same source-row cause explanation', async () => {
  const path = await directory();
  interface Scope { row_0?: { latency: number; sourceRef: string }; row_1?: { latency: number }; finding?: { value: number; sourceRef: string } }
  const chart = flowChart<Scope>('Import row zero', scope => { scope.row_0 = { latency: 20, sourceRef: 'LOCAL SEED/source/row/0' }; }, 'row-0')
    .addFunction('Import other row', scope => { scope.row_1 = { latency: 30 }; }, 'row-1')
    .addDeciderFunction('Compare measured latency', scope => decide(scope, [{ when: s => s.row_0!.latency > 10, then: 'high', label: 'Measured latency exceeds ten' }], { branch: 'low', label: 'No exceedance' }), 'threshold')
    .addFunctionBranch('high', 'Retain supported finding', scope => { scope.finding = { value: scope.row_0!.latency, sourceRef: scope.row_0!.sourceRef }; })
    .addFunctionBranch('low', 'No finding', () => {}).end().build();
  const result = await runWindowFlow({ operationId: 'compressed-native-window', chart,
    traceStore: createFilesystemTraceStore({ directory: path, compression: 'gzip' }) });
  expect(result.trace.status).toBe('stored');
  if (result.trace.status !== 'stored') throw Error('Trace not stored');
  const reopened = await createFilesystemTraceStore({ directory: path }).read(result.trace.ref);
  expect(reopened!.value).toEqual(result.record);
  async function why(record: RuntimeSnapshot) {
    const reads = keysReadFromExecutionTree(record.executionTree);
    const controlDeps = (record.recorders!.find(row => row.name === 'control-dependencies')!.data as { controlDeps: Record<string, ControlDependency> }).controlDeps;
    const address = { logRunId: record.runId, drillPath: [] };
    const graph = createRecordGraph([{ address, commitLog: record.commitLog,
      reads: Object.fromEntries(record.commitLog.map(commit => [commit.runtimeStageId, reads.lookup(commit.runtimeStageId)])), controlDeps, complete: true }]);
    const writer = findLastWriter(record.commitLog, 'finding')!;
    return explain(graph, graph.nodeId(address, { runtimeStageId: writer.runtimeStageId, commitIdx: record.commitLog.indexOf(writer), key: 'finding' }));
  }
  const before = await why(result.record), after = await why(reopened!.value as unknown as RuntimeSnapshot);
  expect(after).toEqual(before);
  expect(JSON.stringify(after)).toContain('row_0');
  expect(JSON.stringify(after)).not.toContain('row_1');
  expect(after.rootCause).toBeUndefined();
});
