import { describe, expect, it } from 'vitest';
import { encodeLineProtocol } from '../src/influxdb/encoding.js';
import { InfluxValidationError, type InfluxPoint } from '../src/influxdb/contracts.js';

const point: InfluxPoint = { measurement: 'metric', tags: [],
  fields: [['value', { type: 'float', value: 1 }]], timestampNs: '0' };

describe('InfluxDB 2 lossless identifier profile', () => {
  it.each([',', ' ', '='])('refuses literal backslash before %j in measurement and field names', suffix => {
    const atom = `prefix\\${suffix}name`;
    for (const value of [{ ...point, measurement: atom }, { ...point,
      fields: [[atom, { type: 'float', value: 1 }]] as InfluxPoint['fields'] }]) {
      expect(() => encodeLineProtocol([value])).toThrow(InfluxValidationError);
    }
  });

  it.each([',', ' ', '='])('retains literal backslash before %j in tag names and values', suffix => {
    const atom = `prefix\\${suffix}name`;
    const escaped = `prefix\\\\${suffix}name`;
    expect(encodeLineProtocol([{ ...point, tags: [[atom, atom]] }]))
      .toBe(`metric,${escaped}=${escaped} value=1 0`);
  });

  it.each(['measurement', 'field'])('refuses literal backslash before quote in %s name', role => {
    const atom = 'prefix\\"name';
    const value = role === 'measurement' ? { ...point, measurement: atom } : { ...point,
      fields: [[atom, { type: 'float', value: 1 }]] as InfluxPoint['fields'] };
    expect(() => encodeLineProtocol([value])).toThrow(InfluxValidationError);
  });

  it('retains literal backslash before quote in tag names and values', () => {
    const atom = 'prefix\\"name';
    expect(encodeLineProtocol([{ ...point, tags: [[atom, atom]] }]))
      .toBe(`metric,${atom}=${atom} value=1 0`);
  });
});
