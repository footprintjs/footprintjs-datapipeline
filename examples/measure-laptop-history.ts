import { mkdtemp, realpath, readdir, lstat, rm } from 'node:fs/promises';
import { tmpdir, cpus, totalmem } from 'node:os';
import { join } from 'node:path';
import { createPipeline } from '../src/createPipeline.js';
import { createRetainedFilesystemRepository } from '../src/storage/filesystem.js';
const DAY = 86400_000, rows = Number(process.argv[2] ?? 50), days = Number(process.argv[3] ?? 32);
const metadataMiB = Number(process.argv[4] ?? 32);
if (![50, 256].includes(rows) || !Number.isSafeInteger(days) || days < 28 || days > 64 ||
  !Number.isSafeInteger(metadataMiB) || metadataMiB < 2 || metadataMiB > 64) throw Error('Use 50 or 256 rows, 28–64 simulated days and a 2–64 MiB metadata allowance.');
const directory = await realpath(await mkdtemp(join(tmpdir(), 'laptop-history-measure-')));
let age = 0, calls = 0;
const alerts: { readonly code: string }[] = [], handles: NonNullable<ReturnType<typeof createRetainedFilesystemRepository>>[] = [];
const policy = { profile: 'daily-periods-v1' as const, keepDays: 7, namespaceId: 'abcdef0123456789abcdef0123456789', maxOperationBytes: 1024 * 1024 };
const options = { enabled: true, directory, retentionMs: 7 * DAY, requireSeparateFilesystem: false,
  expiryClock: 'verified-uptime' as const, monotonicClock: () => age, clock: () => 1_790_000_000_000 + age,
  metadataLimits: { maxBytes: metadataMiB * 1024 * 1024, maxOperations: 80, maxReferences: 80 * (rows + 2) },
  onHealth: (event: { readonly code: string }) => { alerts.push(event); } };
function open(period: number) {
  const repository = createRetainedFilesystemRepository({ ...options, historyRetirement: { ...policy, period } })!;
  handles.push(repository); return repository;
}
async function usage(path: string): Promise<{ allocated: number; logical: number; files: number; controlAllocated: number; operationAllocated: number }> {
  const stat = await lstat(path);
  if (stat.isSymbolicLink()) throw Error('Unexpected link in measured namespace');
  const children = stat.isDirectory() ? await Promise.all((await readdir(path)).map(name => usage(join(path, name)))) : [];
  const allocated = stat.blocks * 512;
  return { allocated: allocated + children.reduce((n, child) => n + child.allocated, 0),
    logical: (stat.isFile() ? stat.size : 0) + children.reduce((n, child) => n + child.logical, 0),
    files: (stat.isFile() ? 1 : 0) + children.reduce((n, child) => n + child.files, 0),
    controlAllocated: (stat.isFile() && /\/retention[^/]*$/.test(path) ? allocated : 0) + children.reduce((n, child) => n + child.controlAllocated, 0),
    operationAllocated: (path.includes('/operations') ? allocated : 0) + children.reduce((n, child) => n + child.operationAllocated, 0) };
}
const maintenance = open(0), samples: { day: number; allocated: number; logical: number; files: number; controlAllocated: number; operationAllocated: number; operations: number; references: number; maintenanceMs: number }[] = [];
const start = performance.now(), rssStart = process.memoryUsage().rss;
try {
  await maintenance.historyStatus();
  for (let day = 0; day < days; day++) {
    age = day * DAY;
    const cleanupStart = performance.now(); await maintenance.sweep({ limit: 100 });
    for (let pass = 0; ; pass++) {
      const batch = await maintenance.retireHistory({ limit: 100 });
      if (batch.blocked) throw Error('Deletion incomplete in fixture');
      if (!batch.pending) break;
      if (pass > 10000) throw Error('Maintenance did not converge');
    }
    const maintenanceMs = performance.now() - cleanupStart;
    const repository = await maintenance.bindPeriod(day);
    const pipeline = createPipeline({ repository, sources: [{ ref: { id: 'synthetic', version: '1' }, adapterRef: { id: 'saved-data', version: '1' } }],
      adapters: [{ ref: { id: 'saved-data', version: '1' }, async collect() {
        calls++;
        return { key: 'poll', outcome: 'partial' as const, coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } },
          observations: Array.from({ length: rows }, (_, i) => ({ key: 'row-' + i, subject: { node: 7, client: i },
            schemaRef: { id: 'synthetic-latency', version: '1' }, time: { kind: 'source-time-unknown' as const },
            payload: { latency_us: (i % 20) * 100, testData: true } })) };
      } }], clock: () => new Date(1_790_000_000_000 + age).toISOString() });
    for (let poll = 0; poll < 8; poll++) {
      const result = await pipeline.collect({ sourceRef: { id: 'synthetic', version: '1' }, idempotencyKey: `day-${day}-poll-${poll}`, request: {} });
      if (result.commit.status !== 'committed') {
        console.error(JSON.stringify({ kind: 'refused-measurement', day, poll, rows, metadataMiB,
          capacity: await maintenance.metadataCapacity({ operations: 0, references: 0 }), usage: await usage(directory), alerts }));
        throw Error(`Write refused at day ${day}, poll ${poll}; this is not a completed measurement.`);
      }
    }
    const capacity = await maintenance.metadataCapacity({ operations: 0, references: 0 });
    samples.push({ day, ...await usage(directory), operations: capacity.operations, references: capacity.references, maintenanceMs });

  }
  if (alerts.some(event => event.code !== 'REPOSITORY_HISTORY_PENDING')) throw Error('Unexpected health failure');
  const plateauStart = Math.floor(days / 2), settled = samples.slice(plateauStart), allocated = settled.map(sample => sample.allocated);
  const range = Math.max(...allocated) - Math.min(...allocated);
  if (range > 128 * 1024) {
    console.error(JSON.stringify({ kind: 'failed-native-plateau', rows, range, samples }, null, 2));
    throw Error(`Allocated bytes did not plateau: range ${range}`);
  }
  console.log(JSON.stringify({ kind: 'synthetic-local-history-cycles', runtime: process.version, cpuCount: cpus().length,
    hostMemoryBytes: totalmem(), rowsPerOperation: rows, simulatedDays: days, operationsPerDay: 8, syntheticAdapterCalls: calls,
    elapsedMs: performance.now() - start, rssStart, rssEnd: process.memoryUsage().rss, metadataLimits: options.metadataLimits,
    pendingMaintenanceNotifications: alerts.length,
    plateau: { startsAtSimulatedDay: plateauStart, minimumAllocatedBytes: Math.min(...allocated), maximumAllocatedBytes: Math.max(...allocated), rangeBytes: range }, keepDays: 7,
    maximumMaintenanceMs: Math.max(...samples.map(sample => sample.maintenanceMs)), checkpoint: await maintenance.historyStatus(),
    samples, scope: 'Managed repository only, including files, directories and SQLite. No device, InfluxDB, external trace store, real polling cadence or fleet-capacity claim.' }, null, 2));
} finally { for (const handle of handles) handle.close(); await rm(directory, { recursive: true, force: true }); }
