import { describe, expect, it, vi } from 'vitest';
import { createInfluxDbSink } from '../src/influxdb/sink.js';
import { InfluxValidationError } from '../src/influxdb/contracts.js';
import { ok, options, point } from './influx-sink.helpers.js';

describe('InfluxDB sink delivery', () => {
  it('constructs without I/O and sends ordered bounded batches with fixed request settings', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const getToken = vi.fn(() => 'synthetic-token');
    const sink = createInfluxDbSink(options(fetch, { getToken, limits: { maxBatchPoints: 2 } }));
    expect(fetch).not.toHaveBeenCalled();
    expect(getToken).not.toHaveBeenCalled();
    const receipt = await sink.write([point(1), point(2), point(3)]);
    expect(receipt).toEqual({ status: 'acknowledged', totalPoints: 3,
      acknowledgedPoints: 3, uncertainPoints: 0, notAttemptedPoints: 0,
      batches: [{ index: 0, pointCount: 2, status: 'acknowledged' },
        { index: 1, pointCount: 1, status: 'acknowledged' }] });
    expect(getToken).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledTimes(2);
    const [url, init] = fetch.mock.calls[0]!;
    expect(String(url)).toBe('http://localhost:8086/api/v2/write?org=test+org&bucket=test+bucket&precision=ns');
    expect(init).toMatchObject({ method: 'POST', redirect: 'error',
      headers: { Authorization: 'Token synthetic-token', 'Content-Type': 'text/plain; charset=utf-8' } });
    expect(String(init?.body).split('\n')).toHaveLength(2);
    expect(String(fetch.mock.calls[1]![1]?.body)).toContain('value=3 ');
  });

  it('splits by UTF-8 bytes without splitting any point', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const receipt = await createInfluxDbSink(options(fetch, { limits: { maxBatchBytes: 64 } }))
      .write([point(1), point(2), point(3)]);
    expect(receipt.status).toBe('acknowledged');
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(fetch.mock.calls.every(([, init]) => Buffer.byteLength(String(init?.body)) <= 64)).toBe(true);
  });

  it('validates all points before resolving credentials or dispatching', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const getToken = vi.fn(() => 'synthetic-token');
    const result = await createInfluxDbSink(options(fetch, { getToken }))
      .write([point(), { ...point(), timestampNs: 'bad' }]);
    expect(result).toMatchObject({ status: 'not-written', code: 'invalid-data',
      totalPoints: 2, acknowledgedPoints: 0, uncertainPoints: 0, notAttemptedPoints: 2, batches: [] });
    expect(getToken).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it('does not call credentials or fetch for an empty write', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const getToken = vi.fn(() => 'synthetic-token');
    expect(await createInfluxDbSink(options(fetch, { getToken })).write([]))
      .toEqual({ status: 'empty', totalPoints: 0, acknowledgedPoints: 0,
        uncertainPoints: 0, notAttemptedPoints: 0, batches: [] });
    expect(getToken).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([200, 201, 400, 429, 500])('only acknowledges 204; %i is uncertain and stops later batches', async status => {
    const fetch = vi.fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(ok()).mockResolvedValue(new Response('private-body', { status }));
    const result = await createInfluxDbSink(options(fetch, { limits: { maxBatchPoints: 1 } }))
      .write([point(1), point(2), point(3)]);
    expect(result).toMatchObject({ status: 'partial', code: 'http-error', totalPoints: 3,
      acknowledgedPoints: 1, uncertainPoints: 1, notAttemptedPoints: 1,
      batches: [{ index: 0, status: 'acknowledged' },
        { index: 1, status: 'unknown', code: 'http-error', httpStatus: status }] });
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(JSON.stringify(result)).not.toContain('private-body');
  });

  it('refuses invalid trusted configuration at construction', () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    for (const extra of [{ url: 'http://user:secret@localhost' }, { url: 'file:///tmp/test' },
      { url: 'https://example.test/?token=secret' }, { timeoutMs: 0 }, { timeoutMs: 30_001 },
      { org: '' }, { bucket: '\nsecret' }]) {
      expect(() => createInfluxDbSink(options(fetch, extra))).toThrow(InfluxValidationError);
    }
    expect(fetch).not.toHaveBeenCalled();
  });
});
