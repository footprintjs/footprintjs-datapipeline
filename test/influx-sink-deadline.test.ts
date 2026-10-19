import { describe, expect, it, vi } from 'vitest';
import { createInfluxDbSink } from '../src/influxdb/sink.js';
import { bounded, never, ok, options, point } from './influx-sink.helpers.js';

describe('InfluxDB sink deadlines and cancellation', () => {
  it('never dispatches an already-cancelled write', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const controller = new AbortController();
    controller.abort(new Error('private-cancel-reason'));
    const result = await createInfluxDbSink(options(fetch)).write([point()], { signal: controller.signal });
    expect(result).toMatchObject({ status: 'not-written', code: 'cancelled', notAttemptedPoints: 1 });
    expect(fetch).not.toHaveBeenCalled();
    expect(JSON.stringify(result)).not.toContain('private-cancel-reason');
  });

  it('includes hanging credentials in the whole-operation deadline', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const sink = createInfluxDbSink(options(fetch, { getToken: () => never(), timeoutMs: 10 }));
    const result = await bounded(sink.write([point()]));
    expect(result).toMatchObject({ status: 'not-written', code: 'timeout', notAttemptedPoints: 1 });
    expect(fetch).not.toHaveBeenCalled();
  });

  it('bounds a fetch that ignores cancellation and refuses its late acknowledgement', async () => {
    let finish!: (response: Response) => void;
    const fetch = vi.fn<typeof globalThis.fetch>(() => new Promise(resolve => { finish = resolve; }));
    const result = await bounded(createInfluxDbSink(options(fetch, { timeoutMs: 10 })).write([point()]));
    expect(result).toMatchObject({ status: 'unknown', code: 'timeout', uncertainPoints: 1 });
    const cancel = vi.fn(async () => {});
    finish({ status: 204, body: { cancel } } as unknown as Response);
    await Promise.resolve();
    expect(result.status).toBe('unknown');
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('cancels during fetch with uncertain dispatched points and no further requests', async () => {
    const controller = new AbortController();
    const fetch = vi.fn<typeof globalThis.fetch>(async () => {
      controller.abort();
      return ok();
    });
    const result = await bounded(createInfluxDbSink(options(fetch, { limits: { maxBatchPoints: 1 } }))
      .write([point(1), point(2)], { signal: controller.signal }));
    expect(result).toMatchObject({ status: 'unknown', code: 'cancelled',
      acknowledgedPoints: 0, uncertainPoints: 1, notAttemptedPoints: 1 });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('retains prior acknowledgements when cancellation occurs before the next batch', async () => {
    const controller = new AbortController();
    const body = { cancel() { controller.abort(); return Promise.resolve(); } };
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ({ status: 204, body }) as Response);
    const result = await createInfluxDbSink(options(fetch, { limits: { maxBatchPoints: 1 } }))
      .write([point(1), point(2)], { signal: controller.signal });
    expect(result).toMatchObject({ status: 'partial', code: 'cancelled',
      acknowledgedPoints: 1, uncertainPoints: 0, notAttemptedPoints: 1 });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('keeps acknowledged, uncertain and undispatched counts distinct on a later timeout', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValueOnce(ok()).mockImplementation(() => never());
    const result = await bounded(createInfluxDbSink(options(fetch, {
      timeoutMs: 10, limits: { maxBatchPoints: 1 },
    })).write([point(1), point(2), point(3)]));
    expect(result).toMatchObject({ status: 'partial', code: 'timeout', totalPoints: 3,
      acknowledgedPoints: 1, uncertainPoints: 1, notAttemptedPoints: 1,
      batches: [{ index: 0, status: 'acknowledged' }, { index: 1, status: 'unknown', code: 'timeout' }] });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('does not leak credential and transport exceptions', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => { throw new Error('private-token-and-url'); });
    const credentials = await createInfluxDbSink(options(fetch, {
      getToken() { throw new Error('private-token-and-url'); },
    })).write([point()]);
    expect(credentials).toMatchObject({ status: 'not-written', code: 'credentials-unavailable' });
    const transport = await createInfluxDbSink(options(fetch)).write([point()]);
    expect(transport).toMatchObject({ status: 'unknown', code: 'transport-error', uncertainPoints: 1 });
    expect(JSON.stringify([credentials, transport])).not.toContain('private-token-and-url');
  });
});
