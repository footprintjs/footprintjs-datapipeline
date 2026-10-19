import { boundedDecimal, dataArray, dataRecord, requireData, token } from './boundary.js';
import { InfluxValidationError, type InfluxLimits, type InfluxPoint, type InfluxValidationCode } from './contracts.js';
import { encodeField } from './field.js';
import { normalizeInfluxLimits } from './limits.js';

const MAX_PAIRS = 256;
const MAX_SERIES_BYTES = 65_535;
const MIN_TIMESTAMP = -9223372036854775806n;
const MAX_TIMESTAMP = 9223372036854775806n;
const escapeMeasurement = (value: string) => value.replace(/[ ,]/g, character => `\\${character}`);
const escapeKey = (value: string) => value.replace(/[ ,=]/g, character => `\\${character}`);

function safeFailure(error: unknown): InfluxValidationError {
  let code: InfluxValidationCode = 'invalid-data';
  try {
    const property = error instanceof InfluxValidationError ? Object.getOwnPropertyDescriptor(error, 'code') : undefined;
    if (property && 'value' in property && ['invalid-data', 'limit-exceeded', 'invalid-options'].includes(property.value)) {
      code = property.value as InfluxValidationCode;
    }
  } catch { /* Caller-controlled error inspection cannot expose its own failure. */ }
  return new InfluxValidationError(code);
}

function encodePairs(value: unknown, fields: boolean, prefixBytes = 0): readonly string[] {
  const pairs = dataArray(value, MAX_PAIRS);
  requireData(!fields || pairs.length > 0);
  const keys = new Set<string>();
  return pairs.map(pair => {
    const tuple = dataArray(pair, 2);
    requireData(tuple.length === 2);
    const key = token(tuple[0], fields ? 'field-key' : 'tag-key');
    requireData(!keys.has(key));
    keys.add(key);
    const encodedKey = escapeKey(key);
    if (fields && prefixBytes + 4 + Buffer.byteLength(encodedKey) > MAX_SERIES_BYTES) {
      throw new InfluxValidationError('limit-exceeded');
    }
    const encoded = fields ? encodeField(tuple[1]) : escapeKey(token(tuple[1], 'tag'));
    return `${encodedKey}=${encoded}`;
  });
}

function encodePoint(value: unknown): string {
  const point = dataRecord(value, ['measurement', 'tags', 'fields', 'timestampNs']);
  const measurement = escapeMeasurement(token(point.measurement, 'measurement'));
  const tags = encodePairs(point.tags, false);
  const prefix = [measurement, ...tags].join(',');
  // InfluxDB 2 limits the encoded series key, including each field's name.
  const fields = encodePairs(point.fields, true, Buffer.byteLength(prefix));
  const timestamp = boundedDecimal(point.timestampNs, MIN_TIMESTAMP, MAX_TIMESTAMP);
  return `${prefix} ${fields.join(',')} ${timestamp}`;
}

/** Validate and detach every point before a sink can resolve credentials or send. */
export function encodePointLines(points: readonly InfluxPoint[], limits?: InfluxLimits): readonly string[] {
  const normalized = normalizeInfluxLimits(limits);
  try {
    const input = dataArray(points, normalized.maxPoints);
    const lines: string[] = [];
    let bytes = 0;
    for (const point of input) {
      const line = encodePoint(point);
      const lineBytes = Buffer.byteLength(line);
      bytes += lineBytes + (lines.length > 0 ? 1 : 0);
      if (lineBytes > normalized.maxBatchBytes || bytes > normalized.maxBytes) {
        throw new InfluxValidationError('limit-exceeded');
      }
      lines.push(line);
    }
    return Object.freeze(lines);
  } catch (error) {
    throw safeFailure(error);
  }
}

export function encodeLineProtocol(points: readonly InfluxPoint[], limits?: InfluxLimits): string {
  return encodePointLines(points, limits).join('\n');
}
