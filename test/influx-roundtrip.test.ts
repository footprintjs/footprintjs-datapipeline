import { expect, test } from 'vitest';
import type { InfluxPoint } from '../src/influxdb/index.js';
import { createLiveFixture, liveEnabled } from './helpers/influx-live.js';

const stamp = '1792411200125000000';
const point = (measurement: string, value: number): InfluxPoint => ({ measurement, tags: [],
  fields: [['value', { type: 'float', value }]], timestampNs: stamp });

test.skipIf(!liveEnabled)('disposable InfluxDB preserves typed values, escaping and exact supplied nanoseconds', async () => {
  const fixture = await createLiveFixture();
  try {
    const fields: InfluxPoint['fields'] = [
      ['float', { type: 'float', value: 5000 }], ['integer', { type: 'integer', value: '9223372036854775807' }],
      ['flag', { type: 'boolean', value: false }], ['text', { type: 'string', value: '00042 "quoted"\\folder' }],
      ['empty', { type: 'string', value: '' }],
    ];
    const measurement = 'LOCAL SEED,writer';
    const tags = [['site', 'lab,west=1'], ['path', 'synthetic\\client']] as const;
    expect((await fixture.sink().write([{ measurement, tags, fields, timestampNs: stamp }])).status).toBe('acknowledged');
    const rows = await fixture.rows();
    expect(rows).toHaveLength(fields.length);
    const expectedTypes = { float: 'double', integer: 'long', flag: 'boolean', text: 'string', empty: 'string' };
    for (const [name, field] of fields) {
      const row = rows.find(row => row.data._field === name)!;
      expect(row.type).toBe(expectedTypes[name as keyof typeof expectedTypes]);
      expect(row.data._value).toBe(String(field.value));
      expect(row.data._measurement).toBe(measurement);
      expect(row.data.site).toBe(tags[0][1]);
      expect(row.data.path).toBe(tags[1][1]);
      expect(row.data.stamp).toBe(stamp);
    }
  } finally { await fixture.close(); }
});

test.skipIf(!liveEnabled)('equivalent Python-style and Node-style float spellings have equal stored meaning', async () => {
  const fixture = await createLiveFixture();
  try {
    await fixture.rawWrite(`python_float value=5000.0 ${stamp}`);
    expect((await fixture.sink().write([point('node_float', 5000)])).status).toBe('acknowledged');
    const rows = await fixture.rows();
    expect(rows).toHaveLength(2);
    expect(rows.map(row => [row.type, row.data._value, row.data.stamp])).toEqual([
      ['double', '5000', stamp], ['double', '5000', stamp],
    ]);
  } finally { await fixture.close(); }
});

test.skipIf(!liveEnabled)('failed mixed batch can store rows, so its receipt reports uncertain rather than zero', async () => {
  const fixture = await createLiveFixture();
  try {
    await fixture.sink().write([point('conflict', 1)]);
    const conflict: InfluxPoint = { ...point('conflict', 1), fields: [['value', { type: 'integer', value: '1' }]] };
    const result = await fixture.sink().write([point('survives', 7), conflict]);
    expect(result).toMatchObject({ status: 'unknown', acknowledgedPoints: 0,
      uncertainPoints: 2, notAttemptedPoints: 0, code: 'http-error' });
    const rows = await fixture.rows();
    expect(rows.some(row => row.data._measurement === 'survives' && row.data._value === '7')).toBe(true);
    expect(rows.find(row => row.data._measurement === 'conflict')?.type).toBe('double');
  } finally { await fixture.close(); }
});

test.skipIf(!liveEnabled)('ordered batches preserve duplicate point overwrite behavior', async () => {
  const fixture = await createLiveFixture();
  try {
    const receipt = await fixture.sink({ maxBatchPoints: 1 }).write([point('ordered', 1), point('ordered', 2)]);
    expect(receipt).toMatchObject({ status: 'acknowledged', acknowledgedPoints: 2, uncertainPoints: 0 });
    const rows = await fixture.rows();
    expect(rows).toHaveLength(1);
    expect(rows[0]!.data._value).toBe('2');
  } finally { await fixture.close(); }
});

test.skipIf(!liveEnabled)('identifier backslashes round-trip without changing decoded names or labels', async () => {
  const fixture = await createLiveFixture();
  const atoms = ['synthetic\\name', 'synthetic\\\\name', 'synthetic\\,name', 'synthetic\\ name',
    'synthetic\\=name', 'synthetic\\"name'];
  try {
    const points = atoms.map((atom, index): InfluxPoint => ({ measurement: index < 2 ? `m${index}_${atom}` : `m${index}`,
      tags: [[`k${index}_${atom}`, atom]], fields: [[index < 2 ? `f${index}_${atom}` : 'field', { type: 'float', value: index }]],
      timestampNs: stamp }));
    for (const input of points) {
      expect((await fixture.sink().write([input])).status, input.measurement).toBe('acknowledged');
    }
    const rows = await fixture.rows();
    expect(rows).toHaveLength(points.length);
    for (const [index, input] of points.entries()) {
      const row = rows.find(row => row.data._value === String(index))!;
      expect(row.data._measurement).toBe(input.measurement);
      expect(row.data._field).toBe(input.fields[0]![0]);
      expect(row.data[input.tags[0]![0]]).toBe(atoms[index]);
    }
  } finally { await fixture.close(); }
});

test.skipIf(!liveEnabled)('non-lossless measurement or field spellings are refused before any database write', async () => {
  const fixture = await createLiveFixture();
  try {
    for (const atom of ['synthetic\\,name', 'synthetic\\ name', 'synthetic\\=name', 'synthetic\\"name']) {
      for (const part of ['measurement', 'field'] as const) {
        const input: InfluxPoint = { measurement: part === 'measurement' ? atom : 'synthetic', tags: [],
          fields: [[part === 'field' ? atom : 'value', { type: 'float', value: 1 }]], timestampNs: stamp };
        expect(await fixture.sink().write([input])).toMatchObject({ status: 'not-written',
          code: 'invalid-data', totalPoints: 1, acknowledgedPoints: 0, uncertainPoints: 0,
          notAttemptedPoints: 1, batches: [] });
      }
    }
    expect(await fixture.rows()).toEqual([]);
  } finally { await fixture.close(); }
});
