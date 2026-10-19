import { InfluxValidationError } from './contracts.js';

export function requireData(condition: unknown): asserts condition {
  if (!condition) throw new InfluxValidationError('invalid-data');
}

/** Inspect data properties without executing caller-provided getters. */
export function dataRecord(value: unknown, keys: readonly string[]): Record<string, unknown> {
  requireData(value !== null && typeof value === 'object' && !Array.isArray(value));
  const prototype = Object.getPrototypeOf(value);
  requireData(prototype === Object.prototype || prototype === null);
  const names = Reflect.ownKeys(value);
  requireData(names.length === keys.length && names.every(key => typeof key === 'string' && keys.includes(key)));
  const result: Record<string, unknown> = Object.create(null);
  for (const key of keys) {
    const property = Object.getOwnPropertyDescriptor(value, key);
    requireData(property && property.enumerable && 'value' in property);
    result[key] = property.value;
  }
  return result;
}

export function dataArray(value: unknown, maximum: number): readonly unknown[] {
  requireData(Array.isArray(value));
  const length: unknown = Object.getOwnPropertyDescriptor(value, 'length')?.value;
  requireData(typeof length === 'number' && Number.isSafeInteger(length) && length >= 0);
  if (length > maximum) throw new InfluxValidationError('limit-exceeded');
  requireData(Object.getPrototypeOf(value) === Array.prototype);
  requireData(Reflect.ownKeys(value).length === length + 1);
  const result: unknown[] = [];
  for (let index = 0; index < length; index++) {
    const property = Object.getOwnPropertyDescriptor(value, String(index));
    requireData(property && property.enumerable && 'value' in property);
    result.push(property.value);
  }
  return result;
}

const MAX_STRING_BYTES = 64 * 1024;
const UNPAIRED_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

export function text(value: unknown, allowEmpty = false): string {
  requireData(typeof value === 'string');
  if (value.length > MAX_STRING_BYTES || Buffer.byteLength(value) > MAX_STRING_BYTES) {
    throw new InfluxValidationError('limit-exceeded');
  }
  requireData((allowEmpty || value.length > 0) && !/[\n\r\0]/.test(value) && !UNPAIRED_SURROGATE.test(value));
  return value;
}

export function token(value: unknown, kind: 'measurement' | 'tag-key' | 'field-key' | 'tag'): string {
  const result = text(value);
  // A terminal backslash consumes the following delimiter in InfluxDB 2's parser.
  requireData(!result.endsWith('\\') && !result.includes('\t'));
  // These forms fail or change identity in InfluxDB 2's measurement/field parsers.
  if (kind === 'measurement' || kind === 'field-key') requireData(!/\\[ ,="]/.test(result));
  if (kind !== 'tag') requireData(!result.startsWith('_'));
  if (kind === 'measurement') requireData(!result.startsWith('#'));
  if (kind === 'tag-key' || kind === 'field-key') requireData(result !== 'time');
  return result;
}

export function boundedDecimal(value: unknown, minimum: bigint, maximum: bigint): string {
  requireData(typeof value === 'string' && value.length <= 20 && /^(?:0|-?[1-9]\d*)$/.test(value));
  const number = BigInt(value);
  requireData(number >= minimum && number <= maximum);
  return value;
}
