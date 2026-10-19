import { expect, test } from 'vitest';
import type { InfluxPoint } from '../src/influxdb/index.js';
import { createLiveFixture, liveEnabled } from './helpers/influx-live.js';

const point = (value: number): InfluxPoint => ({ measurement: 'synthetic_replay', tags: [['site', 'lab']],
  fields: [['value', { type: 'float', value }]], timestampNs: '1792411200125000000' });

test.skipIf(!liveEnabled)('replay of the exact ordered payload preserves stored meaning after a lost acknowledgement', async () => {
  const fixture = await createLiveFixture();
  try {
    const payload = [point(1), point(2)];
    const loseAcknowledgement: typeof globalThis.fetch = async (input, init) => {
      const response = await fetch(input, init);
      expect(response.status).toBe(204);
      await response.body?.cancel();
      throw new Error('Synthetic acknowledgement loss.');
    };
    const first = await fixture.sink({}, loseAcknowledgement).write(payload);
    expect(first).toMatchObject({ status: 'unknown', uncertainPoints: 2, acknowledgedPoints: 0 });
    const before = await fixture.rows();
    expect(before).toHaveLength(1);
    expect(before[0]!.data._value).toBe('2');
    const retried = await fixture.sink().write(payload);
    expect(retried).toMatchObject({ status: 'acknowledged', acknowledgedPoints: 2 });
    expect(await fixture.rows()).toEqual(before);
    expect(first.status).toBe('unknown'); // The earlier attempt is never rewritten.
  } finally { await fixture.close(); }
});

test.skipIf(!liveEnabled)('replaying an older payload can overwrite a later correction at the same point identity', async () => {
  const fixture = await createLiveFixture();
  try {
    expect((await fixture.sink().write([point(1)])).status).toBe('acknowledged');
    expect((await fixture.sink().write([point(9)])).status).toBe('acknowledged');
    expect((await fixture.rows())[0]!.data._value).toBe('9');
    expect((await fixture.sink().write([point(1)])).status).toBe('acknowledged');
    const rows = await fixture.rows();
    expect(rows).toHaveLength(1);
    expect(rows[0]!.data._value).toBe('1');
    expect(rows[0]!.data.stamp).toBe(point(1).timestampNs);
  } finally { await fixture.close(); }
});
