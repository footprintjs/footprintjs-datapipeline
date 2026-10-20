import assert from 'node:assert/strict';
import { mkdir, readFile, readdir, stat, writeFile } from 'node:fs/promises';
import { join, resolve, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { gzipSync, gunzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { createFilesystemRepository, createFilesystemTraceStore } from '../../src/storage/filesystem.js';
import type { CommitWrite } from '../../src/ports.js';
import type { TraceRecord } from '../../src/contracts.js';

// Actual adapter execution against saved synthetic responses, never devices or a DB.
assert.ok(process.argv[2] && process.argv[3], 'Pass an installed Neo PowerScale package and a NEW output directory');
const pkg = resolve(process.argv[2]!), output = resolve(process.argv[3]!);
await mkdir(output, { recursive: false });
const { createPowerScaleClientWriter } = await import(pathToFileURL(join(pkg, 'src/index.ts')).href);
const { createInfluxDbSink } = await import(pathToFileURL(join(pkg, 'node_modules/footprintjs-datapipeline/dist/influxdb/index.js')).href);
const { whyWriter } = await import(pathToFileURL(join(pkg, 'test/helpers/system-cause.ts')).href);
const hash = (bytes: string | Uint8Array) => createHash('sha256').update(bytes).digest('hex');
async function files(path: string): Promise<string[]> {
  const result: string[] = [];
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const full = join(path, entry.name);
    result.push(...entry.isDirectory() ? await files(full) : [full]);
  }
  return result.sort();
}
async function sizes(path: string) {
  let bytes = 0, allocated = 0, count = 0;
  for (const file of await files(path)) { const value = await stat(file); bytes += value.size; allocated += value.blocks * 512; count++; }
  return { bytes, allocated, count };
}
const report: { provenance: string; limitations: string[]; profiles: unknown[]; limitFailure?: unknown } = {
  provenance: 'LOCAL SEED; actual inactive PowerScale writer, saved synthetic input, fixed poll clock, simulated acknowledged sink',
  limitations: ['Not production captures, sustained throughput or a retention forecast.', 'Each backend has its own physical commit-acknowledgement time; logical records, IDs and request fingerprints match exactly.', 'Cause comparisons preserve existing incomplete/gap outcomes; no physical cause is claimed.', 'Filesystem lookup still scans operations; packed lookup must inflate them. Benchmark the intended host/namespace size before service deployment.'], profiles: [],
};
for (const count of [50, 256]) {
  const root = join(output, String(count)), at = '2026-10-20T12:00:00.000Z';
  const bodyText = JSON.stringify({ client: Array.from({ length: count }, (_, i) => ({ node: i % 8, protocol: 'smb2', class: 'read', remote_addr: `192.0.2.${i}`, remote_name: `LOCAL SEED ${i} ${'x'.repeat(110)}`, user: { id: String(i), name: `test ${'u'.repeat(90)}` }, operation_rate: i, in: count - i, out: 0, time_avg: (i * 113) % count, time_max: (i * 47) % count, num_operations: 200, in_avg: 100, in_max: 200, out_avg: 10, out_max: 20, time_min: 1 })) });
  const plain = createFilesystemRepository({ directory: join(root, 'plain') });
  const packed = createFilesystemRepository({ directory: join(root, 'packed'), packing: 'gzip' });
  const traces = createFilesystemTraceStore({ directory: join(root, 'traces') });
  const writes: CommitWrite[] = [], originals: TraceRecord[] = [];
  let intentVerified = false, sends = 0, captures = 0;
  const repository = { read: packed.read.bind(packed), findOperation: packed.findOperation.bind(packed), async commit(write: CommitWrite) {
    const left = await plain.commit(write), right = await packed.commit(write);
    assert.deepEqual({ ...right, committedAt: 'physical-ack-time' }, { ...left, committedAt: 'physical-ack-time' });
    writes.push(structuredClone(write));
    if (write.observations.some(x => x.payload && !Array.isArray(x.payload) && typeof x.payload === 'object' && x.payload.kind === 'intent')) {
      assert.equal((await packed.findOperation(write.operationId))?.receipt.status, 'committed');
      intentVerified = true;
    }
    return right;
  } };
  const options = { repository, traceStore: { async store(trace: TraceRecord) { originals.push(structuredClone(trace)); return traces.store(trace); } }, sourceRef: { id: 'LOCAL-SEED', version: '1' }, destinationRef: { id: 'simulated', version: '1' }, config: { name: 'LOCAL SEED', prefix: 'ps', topN: 50, sentinels: ['25'] }, clock: () => at, pollClockNs: () => '1792497600000000000', client: { origin: 'https://synthetic.example', async readClients() { captures++; return { endpoint: 'https://synthetic.example/platform/3/statistics/summary/client', requestStartedAt: at, receivedAt: at, bodyText, provenance: 'LOCAL SEED' }; } }, sink: createInfluxDbSink({ url: 'http://synthetic.example', org: 'LOCAL SEED', bucket: 'simulated', getToken: () => 'synthetic', fetch: async () => { assert.ok(intentVerified, 'Both backends must durably acknowledge intent before HTTP'); sends++; return new Response(null, { status: 204 }); } }) };
  const result = await createPowerScaleClientWriter(options).run({ runId: `candidate-${count}`, attemptId: 'initial' });
  assert.equal(result.delivery.status, 'acknowledged'); assert.equal(result.receiptSaved, true); assert.equal(sends, 1);
  let reopenedRecords = 0;
  const fresh = createFilesystemRepository({ directory: join(root, 'packed') });
  for (const write of writes) {
    const op = await fresh.findOperation(write.operationId);
    assert.equal(op?.fingerprint, write.fingerprint);
    assert.deepEqual(op, await packed.findOperation(write.operationId));
    for (const record of [write.batch, ...write.observations, write.evidence]) {
      assert.deepEqual(await fresh.read(record.ref), await plain.read(record.ref)); reopenedRecords++;
    }
  }
  // Compare the exact canonical per-record bytes, not merely semantically equal JSON.
  for (const file of await files(join(root, 'packed'))) {
    const bundle = JSON.parse(gunzipSync(await readFile(file)).toString());
    for (const entry of bundle.records) {
      const original = await readFile(join(root, 'plain', 'operations', dirname(file).split('/').at(-1)!, entry.file), 'utf8');
      assert.equal(entry.bytes, original);
      assert.equal(hash(entry.bytes), bundle.manifest.records.find((x: { file: string }) => x.file === entry.file).sha256);
    }
  }
  const mapper = originals.find(trace => (trace.value as { commitLog?: { overwrite?: { point_0?: unknown } }[] }).commitLog?.some(c => c.overwrite?.point_0))!;
  assert.ok(mapper);
  const storedRef = await traces.store(mapper), reopenedTrace = await traces.read(storedRef);
  assert.deepEqual(reopenedTrace, mapper);
  const firstLast = [0, count - 1], cause = [];
  for (const index of firstLast) {
    const before = await whyWriter(mapper.value, `point_${index}`), after = await whyWriter(reopenedTrace!.value, `point_${index}`);
    assert.deepEqual(after, before); cause.push({ target: `point_${index}`, outputSha256: hash(JSON.stringify(before)), unchanged: true });
  }
  for (const file of await files(join(root, 'traces'))) { const bytes = await readFile(file); assert.deepEqual(gunzipSync(gzipSync(bytes)), bytes); assert.equal(hash(bytes), file.split('/').at(-1)!.replace('.json', '')); }
  const recovered = await createPowerScaleClientWriter({ ...options, repository: fresh }).run({ runId: `candidate-${count}`, attemptId: 'initial' });
  assert.equal(recovered.outcome, 'recovered'); assert.equal(sends, 1); assert.equal(captures, 1);
  report.profiles.push({ returnedRows: count, counts: result.counts, plain: await sizes(join(root, 'plain')), packed: await sizes(join(root, 'packed')), reopenedRecords, reopenedOperations: writes.length, originalRecordBytesAndFingerprintsUnchanged: true, recoveryWithoutCaptureOrSend: true, cause });
  if (count === 50) {
    intentVerified = false;
    const refused = await createPowerScaleClientWriter({ ...options, repository: createFilesystemRepository({ directory: join(root, 'refused'), packing: 'gzip', maxPackedBytes: 50_000 }) }).run({ runId: 'limit-refused', attemptId: 'initial' });
    assert.equal(refused.outcome, 'blocked'); assert.equal(sends, 1); assert.ok(refused.refs.capture);
    report.limitFailure = { outcome: refused.outcome, stage: refused.stage, code: refused.code, savedCapture: true, extraSends: 0 };
  }
}
await writeFile(join(output, 'report.json'), JSON.stringify(report, null, 2) + '\n');
console.log(JSON.stringify(report, null, 2));
