import { describe, expect, it, vi } from 'vitest';
import { WriteStopped } from '../src/influxdb/budget.js';
import { InfluxValidationError } from '../src/influxdb/contracts.js';
import { createInfluxDbSink } from '../src/influxdb/sink.js';
import { ok, options, point } from './influx-sink.helpers.js';

describe('InfluxDB sink hostile boundary values', () => {
  it('refuses a revoked points proxy with a fixed receipt', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const revocable = Proxy.revocable([point()], {});
    revocable.revoke();
    const result = await createInfluxDbSink(options(fetch)).write(revocable.proxy);
    expect(result).toMatchObject({ status: 'not-written', code: 'invalid-data', totalPoints: 0 });
    expect(fetch).not.toHaveBeenCalled();
  });

  it('does not read an array length through its get trap', async () => {
    const length = vi.fn(() => { throw new Error('private-length'); });
    const points = new Proxy([point()], { get(target, key, receiver) {
      return key === 'length' ? length() : Reflect.get(target, key, receiver);
    } });
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    expect(await createInfluxDbSink(options(fetch)).write(points)).toMatchObject({
      status: 'acknowledged', totalPoints: 1, acknowledgedPoints: 1,
    });
    expect(length).not.toHaveBeenCalled();
  });

  it('uses the encoded snapshot for successful receipt counts', async () => {
    let lengthReads = 0;
    const points = new Proxy([point()], { getOwnPropertyDescriptor(target, key) {
      const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
      if (key === 'length' && descriptor && ++lengthReads === 1) return { ...descriptor, value: 99 };
      return descriptor;
    } });
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    expect(await createInfluxDbSink(options(fetch)).write(points)).toMatchObject({
      status: 'acknowledged', totalPoints: 1, acknowledgedPoints: 1, notAttemptedPoints: 0,
    });
  });

  it('refuses option accessors and non-signals without leaking or starting HTTP', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const getter = vi.fn(() => { throw new Error('private-signal'); });
    const withAccessor = Object.defineProperty({}, 'signal', { get: getter });
    const fakeSignal = { get addEventListener() { return getter(); } };
    const sink = createInfluxDbSink(options(fetch));
    for (const request of [withAccessor, { signal: fakeSignal }]) {
      const result = await sink.write([point()], request as { signal: AbortSignal });
      expect(result).toMatchObject({ status: 'not-written', code: 'invalid-options' });
      expect(JSON.stringify(result)).not.toContain('private-signal');
    }
    expect(getter).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it('does not inspect app-thrown cancellation codes or their getters', async () => {
    const code = vi.fn(() => { throw new Error('private-code'); });
    const foreign = Object.defineProperty(new WriteStopped('cancelled'), 'code', { get: code });
    const fetch = vi.fn<typeof globalThis.fetch>(async () => { throw foreign; });
    const credentialResult = await createInfluxDbSink(options(fetch, { getToken() { throw foreign; } })).write([point()]);
    const transportResult = await createInfluxDbSink(options(fetch)).write([point()]);
    expect(credentialResult).toMatchObject({ status: 'not-written', code: 'credentials-unavailable' });
    expect(transportResult).toMatchObject({ status: 'unknown', code: 'transport-error' });
    expect(code).not.toHaveBeenCalled();
    expect(JSON.stringify([credentialResult, transportResult])).not.toContain('private-code');
  });

  it.each(['private-status', undefined, NaN, 0, 999])('never copies an invalid HTTP status into a receipt', async status => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ({ status }) as unknown as Response);
    const result = await createInfluxDbSink(options(fetch)).write([point()]);
    expect(result).toMatchObject({ status: 'unknown', code: 'transport-error', uncertainPoints: 1 });
    expect(result.batches[0]).not.toHaveProperty('httpStatus');
    expect(JSON.stringify(result)).not.toContain('private-status');
  });

  it('checks the deadline after reading response status before acknowledging', async () => {
    let elapsed = 0;
    const clock = vi.spyOn(performance, 'now').mockImplementation(() => elapsed);
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ({
      get status() { elapsed = 11; return 204; },
    }) as Response);
    try {
      const result = await createInfluxDbSink(options(fetch, { timeoutMs: 10 })).write([point()]);
      expect(result).toMatchObject({ status: 'unknown', code: 'timeout', acknowledgedPoints: 0, uncertainPoints: 1 });
    } finally { clock.mockRestore(); }
  });

  it('normalizes constructor accessor failures to the fixed options error', () => {
    const fetch = vi.fn<typeof globalThis.fetch>(async () => ok());
    const config = Object.defineProperty(options(fetch), 'getToken', { get() { throw new Error('private-config'); } });
    try {
      createInfluxDbSink(config);
      throw new Error('Invalid options were accepted.');
    } catch (error) {
      expect(error).toBeInstanceOf(InfluxValidationError);
      expect((error as Error).message).toBe('InfluxDB input or options were refused.');
    }
  });
});
