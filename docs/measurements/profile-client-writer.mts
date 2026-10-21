/** LOCAL SEED: one real inactive writer, no device/DB/model; fresh process per run. */
import assert from 'node:assert/strict';
import { Session } from 'node:inspector';
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { phaseRecorder, installNativeProbes, phaseTotals } from './profile-hooks.mjs';
const [neoArgument, outputArgument, rowArgument, mode] = process.argv.slice(2);
assert.ok(neoArgument && outputArgument && ['50', '256'].includes(rowArgument!) && ['published', 'candidate'].includes(mode!));
const probeSha256 = createHash('sha256').update(await readFile(new URL(import.meta.url))).update(await readFile(new URL('./profile-hooks.mts', import.meta.url))).digest('hex');
const neo = resolve(neoArgument), output = resolve(outputArgument!), rows = Number(rowArgument);
await mkdir(output, { recursive: true, mode: 0o700 }); assert.equal((await readdir(output)).length, 0, 'Use fresh private output');
const recorder = phaseRecorder(), restoreNative = installNativeProbes(recorder);
const load = (path: string) => import(pathToFileURL(join(neo, path)).href);
const { FlowChartExecutor } = await load('node_modules/footprintjs/dist/esm/index.js');
const { createPowerScaleClientWriter } = await load('src/clientWriter/index.ts');
const { createInfluxDbSink } = await load('node_modules/footprintjs-datapipeline/dist/influxdb/index.js');
const publishedVersion = JSON.parse(await readFile(join(neo, 'node_modules/footprintjs-datapipeline/package.json'), 'utf8')).version;
assert.equal(publishedVersion, '0.8.0', 'This experiment fixes its reviewed baseline at 0.8.0');
const published = await load('node_modules/footprintjs-datapipeline/dist/storage/filesystem.js');
const candidate = await import('../../src/storage/filesystem.js');
const { fixtureBody } = await load('examples/throughput/metrics.ts');
const { explainWriterKey } = await load('examples/traceCause.ts');
const repository = candidate.createRetainedFilesystemRepository({ enabled: true, directory: join(output, 'data'), retentionMs: 3600000, requireSeparateFilesystem: false, onHealth() {} })!;
const settings = { enabled: true, maxTraceBytesOnDisk: 512 * 1024 * 1024, warningAtBytes: 384 * 1024 * 1024, minFreeBytes: 2 * 1024 ** 3, receiptReserveBytes: 16 * 1024 * 1024, requireSeparateFilesystem: false, onHealth() {} };
const factory = mode === 'published' ? published : candidate;
const active = factory.createGuardedFilesystemTraceStore({ ...settings, directory: join(output, 'active') })!;
const peer = (mode === 'published' ? candidate : published).createGuardedFilesystemTraceStore({ ...settings, directory: join(output, 'peer') })!;
await repository.findOperation('warmup'); await active.usage(); new Response(null, { status: 204 });
let depth = 0;
const originalRun = FlowChartExecutor.prototype.run, originalSnapshot = FlowChartExecutor.prototype.getSnapshot;
FlowChartExecutor.prototype.run = async function (...args: any[]) {
  const level = ++depth;
  try { return await recorder.timed(`flow.run.depth${level}`, () => originalRun.apply(this, args)); } finally { depth--; }
};
FlowChartExecutor.prototype.getSnapshot = function (...args: any[]) { return recorder.sync('flow.snapshot', () => originalSnapshot.apply(this, args)); };
const traceRefs: any[] = [];
const measuredRepository = {
  read: (ref: any) => recorder.timed('repository.read', () => repository.read(ref)),
  readMany: (refs: any) => recorder.timed('repository.readMany', () => repository.readMany(refs)),
  findOperation: (id: string) => recorder.timed('repository.lookup', () => repository.findOperation(id)),
  commit: (write: any) => recorder.timed(`repository.commit.${write.batch.key}`, () => repository.commit(write)),
};
const bodyText = fixtureBody(rows, 0);
const sink = createInfluxDbSink({ url: 'http://synthetic.example', org: 'LOCAL SEED', bucket: 'simulated', getToken: () => 'synthetic', fetch: async () => new Response(null, { status: 204 }) });
const writer = createPowerScaleClientWriter({ repository: measuredRepository,
  traceStore: { async store(value: any) { const ref = await recorder.timed('trace.prepare-compress-save', () => active.store(value)); traceRefs.push(ref); return ref; } },
  sourceRef: { id: 'LOCAL-SEED', version: '1' }, destinationRef: { id: 'SIMULATED', version: '1' }, config: { name: 'LOCAL-SEED', prefix: 'ps', topN: 50, sentinels: ['25'] },
  client: { origin: 'https://synthetic.example', readClients: () => recorder.timed('fetch.synthetic', async () => { const at = new Date().toISOString(); return { endpoint: 'https://synthetic.example/platform/3/statistics/summary/client', requestStartedAt: at, receivedAt: at, bodyText, provenance: 'LOCAL SEED' }; }) },
  sink: { write: (...args: any[]) => recorder.timed('sink.encode-and-ack.synthetic', () => sink.write(...args)) },
});
const session = new Session(); session.connect();
const post = (method: string, params: any = {}) => new Promise<any>((accept, reject) => session.post(method as any, params, (error, value) => error ? reject(error) : accept(value)));
let result: any, cpuProfile: any, resources: any;
try {
  await post('Profiler.enable'); await post('Profiler.setSamplingInterval', { interval: 1000 }); await post('Profiler.start');
  const cpuBefore = process.cpuUsage(); recorder.start();
  result = await recorder.timed('writer.total', () => writer.run({ runId: 'single-profile', attemptId: 'initial' }));
  recorder.stop(); resources = { cpuMicroseconds: process.cpuUsage(cpuBefore), maxRssBytes: process.resourceUsage().maxRSS * 1024, memoryAtEnd: process.memoryUsage() };
  cpuProfile = (await post('Profiler.stop')).profile;
} finally { recorder.stop(); session.disconnect(); restoreNative(); FlowChartExecutor.prototype.run = originalRun; FlowChartExecutor.prototype.getSnapshot = originalSnapshot; }
assert.equal(result.delivery?.status, 'acknowledged');
const totals = phaseTotals(recorder.events);
const mappingRun = recorder.events.find(event => event.label === 'flow.run.depth3');
const mappingStore = mappingRun && recorder.events.find(event => event.label === 'trace.prepare-compress-save' && event.startMs >= mappingRun.startMs + mappingRun.durationMs);
const recordPreparationMs = mappingRun && mappingStore ? mappingStore.startMs - mappingRun.startMs - mappingRun.durationMs : null;
assert.ok(totals['flow.run.depth3'] && totals['native.gzip'] && totals['fs.writeFile'], 'Probes must cover actual ESM execution and native writes');
let causeChecks = 0;
try {
  for (const ref of traceRefs) {
    const stored = await active.read(ref); assert.ok(stored);
    assert.deepEqual(await peer.store(stored), ref); assert.deepEqual(await peer.read(ref), stored);
    const record = stored.value as any;
    const keys = record.commitLog.flatMap((commit: any) => Object.keys(commit.overwrite));
    const key = keys.reverse().find((value: string) => /^point_\d+$/.test(value)) ?? 'progress';
    assert.deepEqual(await explainWriterKey((await peer.read(ref))!.value, key), await explainWriterKey(record, key)); causeChecks++;
  }
} finally { repository.close(); active.close(); peer.close(); }
const report = { provenance: 'LOCAL SEED; one isolated actual inactive writer; synthetic fetch and HTTP acknowledgement', rows, mode, source: { probeSha256, publishedVersion, neoHead: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: neo, encoding: 'utf8' }).trim(), candidateHead: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(), candidateDiffSha256: createHash('sha256').update(execFileSync('git', ['diff'], { encoding: 'utf8' })).digest('hex') }, counts: result.counts, resources, totals, recordPreparationMs, events: recorder.events, proof: { traceCount: traceRefs.length, identicalReferencesAndRecords: true, causeChecks }, limitations: ['Isolated single collection, not 14-cluster capacity or host soak proof', 'Inclusive nested timing columns overlap and must not be summed', 'Main-thread CPU sampling excludes native compression worker CPU', 'Test-only prototype/native probes add overhead; fetching and HTTP acknowledgements are synthetic', 'Peer replay checks run after timing/resource capture; no new device fetch or send', 'Cause equivalence uses existing test-helper limits, not proof of default-limit completeness', 'Warm namespace setup and fixture generation happen before timing'] };
await writeFile(join(output, 'cpu.cpuprofile'), JSON.stringify(cpuProfile), { mode: 0o600 });
await writeFile(join(output, 'profile.json'), JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify(report));
