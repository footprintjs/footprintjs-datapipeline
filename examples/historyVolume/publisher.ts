import { setTimeout as delay } from 'node:timers/promises';
import { createRetainedFilesystemRepository } from '../../src/storage/filesystem.js';
import { createPipeline } from '../../src/createPipeline.js';
import { options } from './fixture.js';
let stop = false;
process.on('message', message => {
  if ((message as { stop?: boolean }).stop) { stop = true; return; }
  void run(message as { directory: string; daily: number; references: number; period: number }).catch(error => {
    process.send!({ failed: true, errorType: error instanceof Error ? error.name : 'unknown' }, () => process.disconnect()); process.exitCode = 1;
  });
});
async function run(config: { directory: string; daily: number; references: number; period: number }) {
  const alerts: string[] = [], times: number[] = [], failures: string[] = [];
  const repo = createRetainedFilesystemRepository(options(config.directory, config.daily, config.references, () => performance.now(), event => alerts.push(event.code)))!;
  let attempted = 0, saved = 0;
  try {
    const scope = await repo.bindPeriod(config.period);
    const pipeline = createPipeline({ repository: scope, sources: [{ ref: { id: 'fixture', version: '1' }, adapterRef: { id: 'fixture', version: '1' } }],
      adapters: [{ ref: { id: 'fixture', version: '1' }, async collect() { return { key: 'poll', outcome: 'partial' as const,
        coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } }, observations: [{ key: 'sample', subject: {},
          schemaRef: { id: 'latency', version: '1' }, time: { kind: 'source-time-unknown' as const }, payload: { latency: 0 } }] }; } }] });
    process.send!({ ready: true });
    while (!stop) {
      const start = performance.now(), result = await pipeline.collect({ sourceRef: { id: 'fixture', version: '1' }, idempotencyKey: `probe-${attempted++}`, request: {} });
      times.push(performance.now() - start);
      if (result.commit.status === 'committed') saved++; else failures.push(result.commit.status === 'failed' ? result.commit.error?.code ?? 'failed' : result.commit.status);
      if (!stop) await delay(350);
    }
    times.sort((a, b) => a - b);
    process.send!({ attempted, saved, failed: failures.length, failures, alerts,
      p95Ms: times[Math.max(0, Math.ceil(times.length * .95) - 1)] ?? 0, maxMs: times.at(-1) ?? 0 }, () => process.disconnect());
  } finally { repo.close(); }
}
