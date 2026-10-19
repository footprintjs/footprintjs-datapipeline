import { describe, expect, it } from 'vitest';
import { encodeLineProtocol, encodePointLines } from '../src/influxdb/encoding.js';
import { InfluxValidationError, type InfluxPoint } from '../src/influxdb/contracts.js';

function point(overrides: Partial<InfluxPoint> = {}): InfluxPoint {
  return { measurement: 'metric', tags: [],
    fields: [['value', { type: 'float', value: 5000 }]], timestampNs: '1792436400000000000',
    ...overrides };
}

describe('Influx line encoding', () => {
  it('retains caller order and explicit field types without changing numeric meaning', () => {
    expect(encodeLineProtocol([point({ tags: [['z', 'second'], ['a', 'first']], fields: [
      ['decimal', { type: 'float', value: 5000 }],
      ['count', { type: 'integer', value: '5000' }],
      ['enabled', { type: 'boolean', value: false }],
      ['identity', { type: 'string', value: '00042' }],
    ] })])).toBe('metric,z=second,a=first decimal=5000,count=5000i,enabled=false,identity="00042" 1792436400000000000');
  });

  it('escapes measurement, tag and field names and quoted values independently', () => {
    expect(encodeLineProtocol([point({ measurement: 'a ,=b', tags: [['a ,=', 'b ,=']],
      fields: [['f ,=', { type: 'string', value: 'a"b\\c' }]], timestampNs: '0' })]))
      .toBe('a\\ \\,=b,a\\ \\,\\==b\\ \\,\\= f\\ \\,\\=="a\\"b\\\\c" 0');
  });

  it('keeps literal internal backslashes in names without Python normalization', () => {
    expect(encodeLineProtocol([point({ measurement: 'disk\\path',
      tags: [['key\\path', 'a\\\\b\\,c']], fields: [['f\\key', { type: 'float', value: 0 }]],
      timestampNs: '-1' })]))
      .toBe('disk\\path,key\\path=a\\\\b\\\\,c f\\key=0 -1');
  });

  it('supports empty strings, finite scientific notation, unicode and negative zero', () => {
    expect(encodeLineProtocol([point({ tags: [['label', '💾']], fields: [
      ['empty', { type: 'string', value: '' }], ['small', { type: 'float', value: 1e-7 }],
      ['zero', { type: 'float', value: -0 }],
    ], timestampNs: '0' })])).toBe('metric,label=💾 empty="",small=1e-7,zero=-0 0');
  });

  it('retains full integer and timestamp ranges without Number conversion', () => {
    expect(encodeLineProtocol([point({ timestampNs: '-9223372036854775806', fields: [
      ['min', { type: 'integer', value: '-9223372036854775808' }],
      ['max', { type: 'integer', value: '9223372036854775807' }],
    ] }), point({ timestampNs: '9223372036854775806' })]))
      .toBe('metric min=-9223372036854775808i,max=9223372036854775807i -9223372036854775806\nmetric value=5000 9223372036854775806');
  });

  it('returns detached immutable line snapshots and no trailing newline', () => {
    const input = point();
    const lines = encodePointLines([input]);
    (input.fields as [string, { type: 'float'; value: number }][])[0]![1].value = 99;
    expect(lines).toEqual(['metric value=5000 1792436400000000000']);
    expect(Object.isFrozen(lines)).toBe(true);
    expect(encodeLineProtocol([])).toBe('');
  });

  it.each([
    { measurement: '#comment' }, { measurement: '_reserved' }, { measurement: '' },
    { measurement: 'tail\\' }, { tags: [['x', 'tail\\']] }, { tags: [['tail\\', 'x']] },
    { fields: [['tail\\', { type: 'float', value: 1 }]] },
    { tags: [['x', '']] }, { tags: [['_reserved', 'x']] }, { fields: [] },
    { fields: [['_reserved', { type: 'float', value: 1 }]] },
    { tags: [['x', 'a'], ['x', 'b']] },
    { fields: [['x', { type: 'float', value: 1 }], ['x', { type: 'float', value: 2 }]] },
  ])('rejects invalid names, empty values and duplicate keys: %j', overrides => {
    expect(() => encodeLineProtocol([point(overrides as Partial<InfluxPoint>)])).toThrow(InfluxValidationError);
  });
});
