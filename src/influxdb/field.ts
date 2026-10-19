import { boundedDecimal, dataRecord, requireData, text } from './boundary.js';

const MIN_INTEGER = -9223372036854775808n;
const MAX_INTEGER = 9223372036854775807n;

export function encodeField(value: unknown): string {
  const field = dataRecord(value, ['type', 'value']);
  switch (field.type) {
    case 'float':
      requireData(typeof field.value === 'number' && Number.isFinite(field.value));
      return Object.is(field.value, -0) ? '-0' : String(field.value);
    case 'integer':
      return `${boundedDecimal(field.value, MIN_INTEGER, MAX_INTEGER)}i`;
    case 'boolean':
      requireData(typeof field.value === 'boolean');
      return String(field.value);
    case 'string':
      return `"${text(field.value, true).replace(/["\\]/g, character => `\\${character}`)}"`;
    default:
      requireData(false);
  }
}
