import { ensure } from '../../validation.js';
import type { InfluxReadRequest } from './contracts.js';

/** Read only shallow data properties; reject nested values before JSON traversal. */
function dataRecord(value: unknown, maximum: number): Record<string, unknown> {
  ensure(value !== null && typeof value === 'object' && !Array.isArray(value), 'Read object required');
  const prototype = Object.getPrototypeOf(value);
  ensure(prototype === Object.prototype || prototype === null, 'Plain read object required');
  ensure(Reflect.ownKeys(value).length <= maximum, 'Read object exceeds bounds');
  const result: Record<string, unknown> = Object.create(null);
  for (const key of Reflect.ownKeys(value)) {
    ensure(typeof key === 'string' && key.length <= 64, 'Invalid read key');
    const property = Object.getOwnPropertyDescriptor(value, key)!;
    ensure('value' in property && property.enumerable, 'Read data properties required');
    result[key] = property.value;
  }
  return result;
}

export function boundedRequest(input: unknown): InfluxReadRequest {
  const request = dataRecord(input, 3);
  ensure(Object.keys(request).sort().join(',') === 'parameters,templateRef,window', 'Unsupported read request');
  const ref = dataRecord(request.templateRef, 2);
  ensure(Object.keys(ref).sort().join(',') === 'id,version', 'Exact template reference required');
  ensure(Object.values(ref).every(value => typeof value === 'string' && value.length > 0 && value.length <= 256), 'Template reference exceeds bounds');
  const window = dataRecord(request.window, 3);
  ensure(Object.keys(window).sort().join(',') === 'basis,end,start', 'Source window required');
  ensure(Object.values(window).every(value => typeof value === 'string' && value.length <= 48), 'Window exceeds bounds');
  const parameters = dataRecord(request.parameters, 32);
  ensure(Object.values(parameters).every(value => typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isSafeInteger(value)) ||
    (typeof value === 'string' && value.length <= 4096)), 'Primitive query settings required');
  return { templateRef: ref, window, parameters } as unknown as InfluxReadRequest;
}
