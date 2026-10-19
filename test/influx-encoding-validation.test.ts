import { describe, expect, it } from 'vitest';
import { encodeLineProtocol } from '../src/influxdb/encoding.js';
import { InfluxValidationError, type InfluxPoint } from '../src/influxdb/contracts.js';

const valid: InfluxPoint = { measurement: 'metric', tags: [],
  fields: [['value', { type: 'float', value: 1 }]], timestampNs: '0' };
const encode = (value: unknown) => encodeLineProtocol(value as readonly InfluxPoint[]);

describe('Influx encoding boundary', () => {
  it.each([null, {}, [null], [{ ...valid, fields: null }], [{ ...valid, tags: [['x']] }],
    [{ ...valid, tags: [['x', 1]] }], [{ ...valid, extra: 'ignored?' }],
    [{ ...valid, fields: [['x', { type: 'float', value: true }]] }],
    [{ ...valid, fields: [['x', { type: 'boolean', value: 1 }]] }],
    [{ ...valid, fields: [['x', { type: 'string', value: null }]] }],
    [{ ...valid, fields: [['x', { type: 'integer', value: 1 }]] }],
    [{ ...valid, fields: [['x', { type: 'unsigned', value: '1' }]] }],
  ])('refuses malformed data without silently converting it', value => {
    expect(() => encode(value)).toThrow(InfluxValidationError);
  });

  it.each([Number.NaN, Infinity, -Infinity])('refuses non-finite floats: %s', value => {
    expect(() => encode([{ ...valid, fields: [['x', { type: 'float', value }]] }]))
      .toThrow(InfluxValidationError);
  });

  it.each(['01', '-0', '+1', ' 1', '1.0', '1e3', '', '9223372036854775808', '-9223372036854775809'])
    ('refuses noncanonical or out-of-range integer: %j', value => {
      expect(() => encode([{ ...valid, fields: [['x', { type: 'integer', value }]] }]))
        .toThrow(InfluxValidationError);
    });

  it.each(['01', '-0', '+1', ' 1', '1.0', '1e3', '', '9223372036854775807', '-9223372036854775807'])
    ('refuses noncanonical or out-of-range timestamps: %j', timestampNs => {
      expect(() => encode([{ ...valid, timestampNs }])).toThrow(InfluxValidationError);
    });

  it.each(['\n', '\r', '\0', '\ud800', '\udfff'])('rejects unsafe text in every position: %j', bad => {
    for (const overrides of [{ measurement: `m${bad}` }, { tags: [[`k${bad}`, 'v']] },
      { tags: [['k', `v${bad}`]] }, { fields: [[`k${bad}`, { type: 'float', value: 1 }]] },
      { fields: [['k', { type: 'string', value: `v${bad}` }]] }]) {
      expect(() => encode([{ ...valid, ...overrides }])).toThrow(InfluxValidationError);
    }
  });

  it('refuses accessors and sparse arrays without invoking caller code', () => {
    let invoked = false;
    const getter = { ...valid };
    Object.defineProperty(getter, 'measurement', { get() { invoked = true; return 'other'; } });
    expect(() => encode([getter])).toThrow(InfluxValidationError);
    const array = new Array(1);
    Object.defineProperty(array, '0', { get() { invoked = true; return valid; } });
    expect(() => encode(array)).toThrow(InfluxValidationError);
    expect(() => encode(new Array(1))).toThrow(InfluxValidationError);
    expect(invoked).toBe(false);
  });

  it('returns only a fixed code and fixed text even for a hostile proxy', () => {
    const hostile = new Proxy({}, { ownKeys() { throw new Error('private-response'); } });
    try { encode([hostile]); throw new Error('did not refuse'); }
    catch (error) {
      expect(error).toBeInstanceOf(InfluxValidationError);
      expect(error).toMatchObject({ code: 'invalid-data', message: 'InfluxDB input or options were refused.' });
      expect(String(error)).not.toContain('private-response');
    }
  });

  it('reads array length from a bounded data descriptor, never a proxy getter', () => {
    let invoked = false;
    const array = new Proxy([valid], { get(target, key, receiver) {
      if (key === 'length') { invoked = true; throw new Error('private-length'); }
      return Reflect.get(target, key, receiver);
    } });
    expect(encode(array)).toBe('metric value=1 0');
    expect(invoked).toBe(false);
  });

  it('replaces even a forged validation error with fixed public wording', () => {
    const forged = new InfluxValidationError('invalid-data');
    forged.message = 'private-response';
    const hostile = new Proxy({}, { ownKeys() { throw forged; } });
    expect(() => encode([hostile])).toThrow('InfluxDB input or options were refused.');
  });
});
