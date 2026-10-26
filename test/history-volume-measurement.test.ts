import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
it('reproduces chunked metadata verification with a concurrent real publisher', () => {
  // Small mechanical fixture for CI. The explicit 241,920/day command is the
  // scale measurement; this result never stands in for that volume or cadence.
  const child = spawnSync(process.execPath, ['--import', 'tsx', fileURLToPath(new URL('../examples/measure-history-volume.ts', import.meta.url)), '1000', '1', '7'],
    { encoding: 'utf8', maxBuffer: 1024 * 1024, timeout: 15000 });
  expect(child.status, child.stderr).toBe(0);
  const report = JSON.parse(child.stdout);
  expect(report.lock.transactions).toBeGreaterThan(10);
  expect(report.pendingMaintenanceNotifications).toBe(1);
  expect(report.probe.failed).toBe(0); expect(report.probe.saved).toBeGreaterThan(0);
  expect(report.samples.at(-1).checkpoint).toMatchObject({ retireBefore: 1, retiredOperations: 1000, retiredReferences: 7000, retiredUnconfirmedOperations: 1 });
}, 20000);
