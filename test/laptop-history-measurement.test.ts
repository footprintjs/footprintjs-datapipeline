import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

it('reruns the 256-row history-cycle measurement and checks actual allocated bytes', () => {
  // Aggregate fixture: 512 native publications across 64 simulated days with
  // real fsync/SQLite and deletion. Local runs take about 25 seconds in total;
  // this allowance is for historical setup, not a relaxed poll/window deadline.
  const childStartedAt = performance.now();
  const child = spawnSync(process.execPath, ['--import', 'tsx', fileURLToPath(new URL('../examples/measure-laptop-history.ts', import.meta.url)), '256', '64', '32'],
    { encoding: 'utf8', maxBuffer: 1024 * 1024, timeout: 80_000 });
  const childDiagnostic = {
    kind: 'history-measurement-child', runtime: process.version,
    elapsedMs: performance.now() - childStartedAt, timeoutMs: 80_000,
    status: child.status, signal: child.signal,
    error: child.error === undefined ? null : {
      name: child.error.name, message: child.error.message,
      code: 'code' in child.error ? child.error.code : null,
      errno: 'errno' in child.error ? child.error.errno : null,
      syscall: 'syscall' in child.error ? child.error.syscall : null,
    },
    stdoutBytes: typeof child.stdout === 'string' ? Buffer.byteLength(child.stdout) : null,
    stderrBytes: typeof child.stderr === 'string' ? Buffer.byteLength(child.stderr) : null,
    stdout: child.stdout ?? null, stderr: child.stderr ?? null,
  };
  // The existing spawn buffer bounds both synthetic streams. Keep them before
  // assertions so a null status cannot hide a completed result before teardown.
  console.error(JSON.stringify(childDiagnostic));
  expect(child.status, JSON.stringify(childDiagnostic)).toBe(0);
  const result = JSON.parse(child.stdout);
  expect(result.syntheticAdapterCalls).toBe(512); expect(result.keepDays).toBe(7);
  expect(result.pendingMaintenanceNotifications).toBeGreaterThan(0);
  expect(result.pendingMaintenanceNotifications).toBeLessThanOrEqual(56);
  expect(result.checkpoint).toMatchObject({ currentPeriod: 63, retireBefore: 56, retiredOperations: 448, retiredReferences: 115584 });
  expect(result.samples).toHaveLength(64);
  for (const sample of result.samples.slice(32)) {
    expect(sample.operations).toBe(64); expect(sample.references).toBe(16512);
    expect(sample.files).toBe(58);
  }
  expect(result.plateau.startsAtSimulatedDay).toBe(32);
  const allocated = result.samples.slice(32).map((sample: { allocated: number }) => sample.allocated);
  expect(result.plateau.rangeBytes).toBe(Math.max(...allocated) - Math.min(...allocated));
  expect(result.plateau.rangeBytes).toBeLessThanOrEqual(128 * 1024);
}, 90_000);
