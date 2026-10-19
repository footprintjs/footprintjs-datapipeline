import { describe, expect, it } from 'vitest';
import { encodeLineProtocol } from '../src/influxdb/encoding.js';
import { normalizeInfluxLimits } from '../src/influxdb/limits.js';
import { INFLUX_DEFAULT_LIMITS, type InfluxPoint } from '../src/influxdb/contracts.js';

const point: InfluxPoint = { measurement: 'm', tags: [],
  fields: [['x', { type: 'string', value: '💾' }]], timestampNs: '0' };

describe('Influx bounds', () => {
  it('uses hard defaults, clamps dependent defaults and freezes options', () => {
    expect(normalizeInfluxLimits()).toEqual(INFLUX_DEFAULT_LIMITS);
    expect(normalizeInfluxLimits({ maxPoints: 1, maxBytes: 10 })).toEqual({
      maxPoints: 1, maxBytes: 10, maxBatchPoints: 1, maxBatchBytes: 10,
    });
    expect(Object.isFrozen(normalizeInfluxLimits())).toBe(true);
  });

  it.each([
    { maxPoints: 10_001 }, { maxBytes: 8 * 1024 * 1024 + 1 },
    { maxBatchPoints: 5_001 }, { maxBatchBytes: 1024 * 1024 + 1 },
    { maxPoints: 0 }, { maxBytes: -1 }, { maxBatchPoints: 1.5 },
    { maxBatchBytes: Infinity }, { maxPoints: 1, maxBatchPoints: 2 },
    { maxBytes: 10, maxBatchBytes: 11 }, { unknown: 1 },
  ])('refuses invalid or raised limits: %j', limits => {
    expect(() => normalizeInfluxLimits(limits)).toThrow();
  });

  it('bounds UTF-8 bytes including separators and refuses an individually oversized line', () => {
    const encoded = encodeLineProtocol([point, point]);
    const bytes = Buffer.byteLength(encoded);
    expect(encodeLineProtocol([point, point], { maxBytes: bytes })).toBe(encoded);
    expect(() => encodeLineProtocol([point, point], { maxBytes: bytes - 1 })).toThrow();
    expect(() => encodeLineProtocol([point], { maxBatchBytes: Buffer.byteLength(encodeLineProtocol([point])) - 1 }))
      .toThrow();
  });

  it('refuses overflow rather than truncating points or pair lists', () => {
    expect(() => encodeLineProtocol([point, point], { maxPoints: 1 })).toThrow();
    expect(() => encodeLineProtocol([{ ...point,
      tags: Array.from({ length: 257 }, (_, i) => [`k${i}`, 'v'] as const) }])).toThrow();
    expect(() => encodeLineProtocol([{ ...point,
      fields: Array.from({ length: 257 }, (_, i) => [`k${i}`, { type: 'float', value: i }] as const) }])).toThrow();
  });

  it('bounds each string by UTF-8 bytes before escaping', () => {
    const allowed = { ...point, fields: [['k', { type: 'string', value: '='.repeat(65_536) }]] } as InfluxPoint;
    expect(encodeLineProtocol([allowed])).toContain('='.repeat(65_536));
    const large = '💾'.repeat(16_385);
    for (const overrides of [{ measurement: large }, { tags: [['k', large]] },
      { fields: [['k', { type: 'string', value: large }]] }]) {
      expect(() => encodeLineProtocol([{ ...point, ...overrides } as InfluxPoint])).toThrow();
    }
  });
});
