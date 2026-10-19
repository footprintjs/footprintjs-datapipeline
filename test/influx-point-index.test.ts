import { describe, expect, it } from 'vitest';
import { InfluxValidationError, type InfluxLimits, type InfluxPoint } from '../src/influxdb/contracts.js';
import { encodeLineProtocol, readRejectedPointIndex } from '../src/influxdb/encoding.js';

const point: InfluxPoint = { measurement: 'm', tags: [],
  fields: [['v', { type: 'float', value: 1 }]], timestampNs: '0' };
const bad: InfluxPoint = { ...point, fields: [['v', { type: 'float', value: Number.NaN }]] };

function refused(points: unknown, limits?: InfluxLimits): InfluxValidationError {
  try { encodeLineProtocol(points as readonly InfluxPoint[], limits); }
  catch (error) {
    expect(error).toBeInstanceOf(InfluxValidationError);
    return error as InfluxValidationError;
  }
  throw new Error('Expected preflight refusal.');
}

describe('Rejected Influx point position', () => {
  it.each([0, 1])('reports zero-based local point index %s', index => {
    const error = refused(index === 0 ? [bad, point] : [point, bad, point]);
    expect(error).toMatchObject({ code: 'invalid-data', rejectedPointIndex: index });
    expect(readRejectedPointIndex(error)).toBe(index);
    expect(Object.getOwnPropertyDescriptor(error, 'rejectedPointIndex'))
      .toMatchObject({ value: index, writable: false, configurable: false });
  });

  it('reports the first refused point without silently dropping it or inspecting later points', () => {
    let laterInspected = false;
    const later = new Proxy(point, { ownKeys() { laterInspected = true; return Reflect.ownKeys(point); } });
    expect(refused([point, bad, later]).rejectedPointIndex).toBe(1);
    expect(laterInspected).toBe(false);
  });

  it('reports point-local shape and per-point pair limits', () => {
    const tooMany = { ...point, tags: Array.from({ length: 257 }, (_, i) => [`k${i}`, 'v']) };
    expect(refused([point, null]).rejectedPointIndex).toBe(1);
    expect(refused([point, tooMany])).toMatchObject({ code: 'limit-exceeded', rejectedPointIndex: 1 });
  });

  it('reports an individual line that cannot fit a batch', () => {
    const large = { ...point, fields: [['v', { type: 'string', value: 'x'.repeat(40) }]] };
    expect(refused([point, large], { maxBatchBytes: 32 }))
      .toMatchObject({ code: 'limit-exceeded', rejectedPointIndex: 1 });
  });

  it('omits point position for aggregate byte and point limits, even on the first line', () => {
    const bytes = Buffer.byteLength(encodeLineProtocol([point]));
    for (const [input, limits] of [
      [[point, point], { maxPoints: 1 }], [[point, point], { maxBytes: bytes * 2 }],
      [[point], { maxBytes: bytes - 1 }],
    ] as const) {
      const error = refused(input, limits);
      expect(error.code).toBe('limit-exceeded');
      expect(Object.hasOwn(error, 'rejectedPointIndex')).toBe(false);
      expect(readRejectedPointIndex(error)).toBeUndefined();
    }
  });

  it('omits position for invalid outer arrays and options', () => {
    for (const [input, limits] of [
      [null, undefined], [new Array(1), undefined], [[point], { maxPoints: 0 }],
    ] as const) {
      const error = refused(input, limits);
      expect(Object.hasOwn(error, 'rejectedPointIndex')).toBe(false);
      expect(readRejectedPointIndex(error)).toBeUndefined();
    }
  });

  it('replaces a forged local index with the real position and fixed wording', () => {
    const forged = new InfluxValidationError('invalid-data');
    Object.defineProperty(forged, 'rejectedPointIndex', { value: 999 });
    forged.message = 'private-response';
    const proxy = new Proxy(point, { ownKeys() { throw forged; } });
    const error = refused([point, proxy]);
    expect(error.rejectedPointIndex).toBe(1);
    expect(readRejectedPointIndex(error)).toBe(1);
    expect(error.message).toBe('InfluxDB input or options were refused.');
    expect(JSON.stringify(error)).not.toContain('private-response');
  });

  it('does not carry a forged index out of an outer array failure', () => {
    const forged = Object.assign(new InfluxValidationError('invalid-data'), { rejectedPointIndex: 0 });
    const proxy = new Proxy([point], { ownKeys() { throw forged; } });
    const error = refused(proxy);
    expect(Object.hasOwn(error, 'rejectedPointIndex')).toBe(false);
    expect(readRejectedPointIndex(error)).toBeUndefined();
  });

  it('never reads a foreign index property or accepts a copied diagnostic', () => {
    let inspected = false;
    const forged = new Proxy({}, { get() { inspected = true; throw new Error('private-response'); } });
    expect(readRejectedPointIndex(forged)).toBeUndefined();
    const real = refused([bad]);
    expect(readRejectedPointIndex({ ...real })).toBeUndefined();
    expect(inspected).toBe(false);
  });
});
