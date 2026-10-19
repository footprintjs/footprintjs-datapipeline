import { expect, test, vi } from 'vitest';
import { encodeLineProtocol, type InfluxPoint } from '../src/influxdb/index.js';
import { createInfluxDbSink } from '../src/influxdb/sink.js';
import { ok, options, point } from './influx-sink.helpers.js';

test.each([0, 1])('point-local refusal identifies input %i before credentials or HTTP', async rejectedPointIndex => {
  const privateName = 'private-client\nname';
  const points = [point(), point(), point()];
  points[rejectedPointIndex] = { ...point(), tags: [['client', privateName]] };
  const getToken = vi.fn(() => 'synthetic-token');
  const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
  const receipt = await createInfluxDbSink(options(fetch, { getToken })).write(points);
  expect(receipt).toMatchObject({ status: 'not-written', code: 'invalid-data', rejectedPointIndex,
    totalPoints: 3, acknowledgedPoints: 0, uncertainPoints: 0, notAttemptedPoints: 3, batches: [] });
  expect(getToken).not.toHaveBeenCalled();
  expect(fetch).not.toHaveBeenCalled();
  expect(JSON.stringify(receipt)).not.toContain('private-client');
});

test('the host can record one gap and explicitly submit the remaining points in their original order', async () => {
  const points = [point(1), { ...point(2), measurement: 'invalid\nmetric' }, point(3)];
  const bodies: string[] = [];
  const fetch = vi.fn<typeof globalThis.fetch>(async (_input, init) => { bodies.push(String(init?.body)); return ok(); });
  const sink = createInfluxDbSink(options(fetch));
  const refused = await sink.write(points);
  expect(refused.rejectedPointIndex).toBe(1);
  expect(fetch).not.toHaveBeenCalled();
  // Explicit app policy, not a sink retry or silent truncation. Keep the source observations.
  const gap = { sourceRef: 'fixture:row/1', code: refused.code, inputIndex: refused.rejectedPointIndex };
  const kept = points.filter((_point, index) => index !== gap.inputIndex);
  expect(gap).toEqual({ sourceRef: 'fixture:row/1', code: 'invalid-data', inputIndex: 1 });
  expect(await sink.write(kept)).toMatchObject({ status: 'acknowledged', acknowledgedPoints: 2 });
  expect(bodies).toEqual([encodeLineProtocol([points[0]!, points[2]!])]);
});

test('whole-call limits and malformed arrays do not blame any one point', async () => {
  const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
  const sink = createInfluxDbSink(options(fetch, { limits: { maxBytes: 10 } }));
  const totalBytes = await sink.write([point()]);
  expect(totalBytes).toMatchObject({ status: 'not-written', code: 'limit-exceeded' });
  expect(totalBytes).not.toHaveProperty('rejectedPointIndex');
  const count = await createInfluxDbSink(options(fetch, { limits: { maxPoints: 1 } })).write([point(), point()]);
  const malformed = await sink.write([, point()] as InfluxPoint[]);
  for (const receipt of [count, malformed]) expect(receipt).not.toHaveProperty('rejectedPointIndex');
  expect(fetch).not.toHaveBeenCalled();
});

test('an earlier encoder error cannot supply an index to credentials or transport failures', async () => {
  let earlier: unknown;
  try { encodeLineProtocol([point(), { ...point(), measurement: '\n' }]); } catch (error) { earlier = error; }
  const fetch = vi.fn<typeof globalThis.fetch>(async () => { throw earlier; });
  const credentials = await createInfluxDbSink(options(fetch, { getToken() { throw earlier; } })).write([point()]);
  const transport = await createInfluxDbSink(options(fetch)).write([point()]);
  expect(credentials.code).toBe('credentials-unavailable');
  expect(transport).toMatchObject({ status: 'unknown', code: 'transport-error' });
  for (const receipt of [credentials, transport]) expect(receipt).not.toHaveProperty('rejectedPointIndex');
});

test('a deadline during validation takes precedence and reports no rejected point', async () => {
  let elapsed = 0;
  const clock = vi.spyOn(performance, 'now').mockImplementation(() => elapsed);
  const bad = new Proxy(point(), { ownKeys() { elapsed = 11; throw new Error('private-reason'); } });
  try {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const result = await createInfluxDbSink(options(fetch, { timeoutMs: 10 })).write([bad]);
    expect(result).toMatchObject({ status: 'not-written', code: 'timeout' });
    expect(result).not.toHaveProperty('rejectedPointIndex');
    expect(fetch).not.toHaveBeenCalled();
  } finally { clock.mockRestore(); }
});
