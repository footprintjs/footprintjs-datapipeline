import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { describe, expect, it, vi } from 'vitest';
import { createInfluxDbSink } from '../src/influxdb/sink.js';
import { bounded, never, ok, options, point } from './influx-sink.helpers.js';

async function localServer(handler: (req: IncomingMessage, res: ServerResponse) => void) {
  const server = createServer(handler);
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Test server did not start.');
  return { url: `http://127.0.0.1:${address.port}`, close: () => new Promise<void>((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
    server.closeAllConnections();
  }) };
}

describe('InfluxDB sink safety boundaries', () => {
  it('never forwards Authorization to a redirect destination', async () => {
    const forwarded: string[] = [];
    const destination = await localServer((req, res) => {
      forwarded.push(req.headers.authorization ?? 'missing');
      res.writeHead(204).end();
    });
    const original = await localServer((_req, res) => res.writeHead(307, { Location: destination.url }).end());
    try {
      const result = await createInfluxDbSink(options(globalThis.fetch, { url: original.url })).write([point()]);
      expect(result).toMatchObject({ status: 'unknown', code: 'transport-error', uncertainPoints: 1 });
      expect(forwarded).toEqual([]);
    } finally {
      await original.close();
      await destination.close();
    }
  });

  it('does not read or await response bodies even when cancellation hangs', async () => {
    const text = vi.fn(async () => 'private-response');
    const cancel = vi.fn(() => never<void>());
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ({ status: 400, text,
      body: { cancel } }) as unknown as Response);
    const result = await bounded(createInfluxDbSink(options(fetch)).write([point()]));
    expect(result).toMatchObject({ status: 'unknown', code: 'http-error', uncertainPoints: 1 });
    expect(text).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(result)).not.toContain('private-response');
  });

  it('snapshots all encoded bodies before any asynchronous credentials callback', async () => {
    const mutable = point(2);
    const points = [point(1), mutable];
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const getToken = () => {
      Object.assign(mutable, { timestampNs: 'not-nanoseconds' });
      points.push(point(99));
      return 'synthetic-token';
    };
    const result = await createInfluxDbSink(options(fetch, { getToken })).write(points);
    expect(result).toMatchObject({ status: 'acknowledged', totalPoints: 2, acknowledgedPoints: 2 });
    expect(String(fetch.mock.calls[0]![1]?.body)).not.toContain('not-nanoseconds');
    expect(String(fetch.mock.calls[0]![1]?.body)).not.toContain('value=99 ');
  });

  it('does not dispatch credentials that arrive after timeout', async () => {
    let finish!: (token: string) => void;
    const getToken = () => new Promise<string>(resolve => { finish = resolve; });
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const result = await bounded(createInfluxDbSink(options(fetch, { getToken, timeoutMs: 10 })).write([point()]));
    finish('synthetic-token');
    await Promise.resolve();
    expect(result).toMatchObject({ status: 'not-written', code: 'timeout', notAttemptedPoints: 1 });
    expect(fetch).not.toHaveBeenCalled();
  });

  it('cancels pending credentials before any request is dispatched', async () => {
    const controller = new AbortController();
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const getToken = () => { controller.abort(); return never<string>(); };
    const result = await bounded(createInfluxDbSink(options(fetch, { getToken }))
      .write([point()], { signal: controller.signal }));
    expect(result).toMatchObject({ status: 'not-written', code: 'cancelled', notAttemptedPoints: 1 });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each(['', 'synthetic token', 'synthetic\ntoken'])('refuses an invalid credential without HTTP', async token => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const result = await createInfluxDbSink(options(fetch, { getToken: () => token })).write([point()]);
    expect(result).toMatchObject({ status: 'not-written', code: 'credentials-unavailable' });
    expect(fetch).not.toHaveBeenCalled();
  });
});
