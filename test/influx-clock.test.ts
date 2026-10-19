import { execFileSync } from 'node:child_process';
import { expect, test } from 'vitest';
import { encodeLineProtocol } from '../src/influxdb/index.js';

// For the declared seconds range, half an IEEE754 clock ULP plus half a
// nanosecond-product ULP is < 256 ns. This is a fixture comparison allowance,
// never a production rewrite/deduplication tolerance or a live-poll comparison.
const MAX_PYTHON_CLOCK_ERROR_NS = 256n;
const clocks = ['1792411200.000000001', '1792411200.125000000',
  '1792411200.123456789', '1792411200.999999999', '2000000000.987654321'];

test('same fixed clock permits Python rounding while keeping typed values and the stamped moment', () => {
  const python = JSON.parse(execFileSync('python3', ['-I', '-c',
    'import json,sys; print(json.dumps(["synthetic_clock value=5000.0 " + str(int(float(s)*1_000_000_000)) for s in json.load(sys.stdin)]))',
  ], { input: JSON.stringify(clocks), encoding: 'utf8', env: {}, timeout: 5000, maxBuffer: 8192 })) as string[];
  let observedRounding = false;
  for (const [index, clock] of clocks.entries()) {
    const [seconds, fraction] = clock.split('.') as [string, string];
    expect(BigInt(seconds)).toBeLessThan(2n ** 31n);
    const timestampNs = (BigInt(seconds) * 1_000_000_000n + BigInt(fraction)).toString();
    const node = encodeLineProtocol([{ measurement: 'synthetic_clock', tags: [],
      fields: [['value', { type: 'float', value: 5000 }]], timestampNs }]);
    const [measurement, field, stamp] = node.split(' ') as [string, string, string];
    const [pyMeasurement, pyField, pyStamp] = python[index]!.split(' ') as [string, string, string];
    expect(measurement).toBe(pyMeasurement);
    expect(field).toBe('value=5000'); // no `i`: both representations are floats
    expect(Number(field.split('=')[1])).toBe(Number(pyField.split('=')[1]));
    expect(stamp).toBe(timestampNs); // the encoder never chooses or changes time
    const delta = BigInt(stamp) - BigInt(pyStamp);
    expect(delta < 0n ? -delta : delta).toBeLessThanOrEqual(MAX_PYTHON_CLOCK_ERROR_NS);
    if (delta !== 0n) observedRounding = true;
    // A different event (for example response time) is not rounding tolerance.
    const later = BigInt(stamp) + 1_000_000n - BigInt(pyStamp);
    expect(later).toBeGreaterThan(MAX_PYTHON_CLOCK_ERROR_NS);
  }
  expect(observedRounding).toBe(true);
});
