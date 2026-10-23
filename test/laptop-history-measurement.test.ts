import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

it('reruns the 256-row history-cycle measurement and checks actual allocated bytes', () => {
  // Aggregate fixture: 256 native publications across 32 simulated days with
  // real fsync/SQLite and deletion. Local runs take about 13 seconds in total;
  // this allowance is for historical setup, not a relaxed poll/window deadline.
  const child = spawnSync(process.execPath, ['--import', 'tsx', fileURLToPath(new URL('../examples/measure-laptop-history.ts', import.meta.url)), '256', '32', '32'],
    { encoding: 'utf8', maxBuffer: 1024 * 1024, timeout: 80_000 });
  expect(child.status, child.stderr).toBe(0);
  const result = JSON.parse(child.stdout);
  expect(result.syntheticAdapterCalls).toBe(256); expect(result.keepDays).toBe(7);
  expect(result.checkpoint).toMatchObject({ currentPeriod: 31, retireBefore: 24, retiredOperations: 192, retiredReferences: 49536 });
  expect(result.samples).toHaveLength(32);
  for (const sample of result.samples.slice(16)) {
    expect(sample.operations).toBe(64); expect(sample.references).toBe(16512);
    expect(sample.files).toBe(58);
  }
  expect(result.plateau.rangeBytes).toBeLessThanOrEqual(128 * 1024);
}, 90_000);
