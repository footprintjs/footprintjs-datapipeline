import { expect, it } from 'vitest';
import { createLiveFixture, liveEnabled } from './helpers/influx-live.js';
import { summarizeNumericSamples, type NumericMetricDefinition } from '../src/index.js';
import type { InfluxQueryTemplate } from '../src/influxdb/index.js';

const start = '2026-10-20T10:00:00.000Z';
const at = (ms: number) => new Date(Date.parse(start) + ms).toISOString();
const metric: NumericMetricDefinition = {
  sourceRef: { id: 'LOCAL-SEED-Influx', version: '1' }, metricRef: { id: 'sample-latency', version: '1' },
  unit: 'microseconds', grain: 'retained-sample-average', weighting: 'equal-per-sample', clock: 'source', dimensions: {},
  populationRef: { id: 'LOCAL-SEED', version: '1' }, retentionPolicyRef: { id: 'test-only', version: '1' }, gtThresholds: [],
};
// This generic fixture deliberately stores the address as a FIELD. Filter after pivot.
const base = `data = from(bucket: {{bucket}})
  |> range(start: time(v: {{start}}), stop: time(v: {{end}}))
  |> filter(fn: (r) => r._measurement == "reader_fixture")
  |> filter(fn: (r) => r._field == "latency_us" or r._field == "remote_addr")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => exists r.remote_addr and r.remote_addr == {{address}})
  |> group(columns: [])
`;
const operations = ['count', 'sum', 'min', 'max', 'mean'] as const;
const template: InfluxQueryTemplate = { ref: { id: 'fixture-aggregate-equivalence', version: '1' }, parameters: { address: 'string' },
  text: base + operations.map(operation => `data |> ${operation}(column: "latency_us")
    |> map(fn: (r) => ({operation: "${operation}", value: string(v: r.latency_us)}))
    |> group(columns: []) |> limit(n: {{limit}}) |> yield(name: "${operation}")`).join('\n') };
const rowTemplate: InfluxQueryTemplate = { ref: { id: 'fixture-rows', version: '1' }, parameters: { address: 'string' },
  text: base + 'data |> keep(columns: ["_time", "latency_us", "remote_addr"]) |> sort(columns: ["_time"]) |> limit(n: {{limit}})' };

it.skipIf(!liveEnabled)('matches database functions to existing reference statistics across boundaries, gaps, partial windows and mutable reads', async () => {
  const f = await createLiveFixture();
  try {
    const samples = [{ ms: -1, value: 999 }, { ms: 0, value: 0.1 }, { ms: 5000, value: 0.2 }, { ms: 10000, value: 0.3 }, { ms: 60000, value: 999 }];
    const stamp = (ms: number) => BigInt(Date.parse(at(ms))) * 1000000n;
    const lines = samples.map(sample => `reader_fixture,cluster=LOCAL-SEED,client=one latency_us=${sample.value},remote_addr="192.0.2.1" ${stamp(sample.ms)}`);
    lines.push(`reader_fixture,cluster=LOCAL-SEED,client=two latency_us=777.0,remote_addr="192.0.2.2" ${stamp(5000)}`);
    await f.rawWrite(lines.join('\n'));
    const reader = await f.reader([template, rowTemplate]);
    const request = (a: number, b: number) => ({ templateRef: template.ref, window: { basis: 'source', start: at(a), end: at(b) }, parameters: { address: '192.0.2.1' } });
    for (const [a, b] of [[0, 60000], [0, 7000], [10000, 60000], [20000, 30000], [60000, 120000]]) {
      const result = await reader.read(request(a!, b!));
      expect(result.outcome, JSON.stringify(result.receipt)).toBe('complete');
      const selected = samples.filter(sample => sample.ms >= a! && sample.ms < b!);
      const reference = summarizeNumericSamples(metric, selected.map((sample, i) => ({ status: 'valid', value: sample.value, at: at(sample.ms), inputRef: { kind: 'observation', id: `source-${i}` } })));
      const values = new Map(result.rows.map(row => [row.operation, Number(row.value)]));
      expect(values.get('count') ?? 0).toBe(reference.count);
      for (const [operation, expected] of [['min', reference.min?.value], ['max', reference.max?.value], ['mean', reference.mean], ['sum', selected.length ? selected.reduce((sum, sample) => sum + sample.value, 0) : undefined]] as const) {
        if (expected === null || expected === undefined) expect(values.has(operation)).toBe(false);
        else expect(values.get(operation)).toBeCloseTo(expected, 10); // absolute 1e-10 at these fixture magnitudes
      }
    }
    const first = await reader.read(request(0, 60000));
    const original = JSON.stringify(first);
    await f.rawWrite(`reader_fixture,cluster=LOCAL-SEED,client=one latency_us=0.4,remote_addr="192.0.2.1" ${stamp(15000)}`);
    const late = await reader.read(request(0, 60000));
    expect(late.receipt.resultSha256).not.toBe(first.receipt.resultSha256);
    expect(JSON.stringify(first)).toBe(original);
    expect(first.receipt.sourceRowsFingerprint).toBe('not-captured');
    const detail = await reader.read({ ...request(0, 60000), templateRef: rowTemplate.ref });
    expect(detail.rows).toHaveLength(4);
    expect(detail.receipt.sourceMutable).toBe(true);
    expect(detail.receipt.resultSha256).toMatch(/^[a-f0-9]{64}$/);
    // Even matching aggregate values do not certify unchanged contributing rows.
    expect(late.receipt.sourceRowsFingerprint).toBe('not-captured');
  } finally { await f.close(); }
});
