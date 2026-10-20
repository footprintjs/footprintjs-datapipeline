import { createHash } from 'node:crypto';
import { connectionName, influxEndpoint } from '../connection.js';
import { assertRef, assertTimestamp, cloneJson, digest, ensure } from '../../validation.js';
import type { InfluxDbReaderOptions, InfluxQueryTemplate, InfluxReadRequest } from './contracts.js';
import { boundedRequest } from './request.js';

/** Escape interpolation as well as quotes/backslashes: JSON escaping alone is insufficient in Flux. */
function literal(value: unknown, kind: string): string {
  if (kind === 'integer') { ensure(typeof value === 'number' && Number.isSafeInteger(value), 'Integer setting required'); return String(value); }
  if (kind === 'boolean') { ensure(typeof value === 'boolean', 'Boolean setting required'); return String(value); }
  ensure(kind === 'string' && typeof value === 'string' && value.length <= 4096 && !/[\u0000-\u001f\u007f]/u.test(value), 'String setting required');
  return JSON.stringify(value).replaceAll('${', '\\${');
}
export function registeredTemplates(input: readonly InfluxQueryTemplate[]) {
  const templates = cloneJson(input);
  ensure(Array.isArray(templates) && templates.length > 0 && templates.length <= 32, 'Templates required');
  const registry = new Map<string, InfluxQueryTemplate>();
  for (const template of templates) {
    assertRef(template.ref);
    ensure(Object.keys(template.ref).sort().join(',') === 'id,version' && template.ref.id.length <= 256 && template.ref.version.length <= 256, 'Template reference exceeds bounds');
    ensure(Object.keys(template).sort().join(',') === 'parameters,ref,text' && typeof template.text === 'string' && template.text.length > 0 && template.text.length <= 32768, 'Invalid template');
    ensure(template.parameters && typeof template.parameters === 'object' && !Array.isArray(template.parameters), 'Invalid template settings');
    ensure(Object.entries(template.parameters).length <= 32 && Object.entries(template.parameters).every(([key, kind]) => /^[A-Za-z][A-Za-z0-9_]{0,63}$/u.test(key) && !['bucket', 'start', 'end', 'limit'].includes(key) && typeof kind === 'string' && ['string', 'integer', 'boolean'].includes(kind)), 'Invalid parameter declaration');
    const bindings = [...template.text.matchAll(/\{\{([A-Za-z][A-Za-z0-9_]*)\}\}/gu)].map(match => match[1]!);
    ensure(['bucket', 'start', 'end', 'limit', ...Object.keys(template.parameters)].every(key => bindings.includes(key)) && bindings.every(key => ['bucket', 'start', 'end', 'limit'].includes(key) || Object.hasOwn(template.parameters, key)), 'Invalid template bindings');
    const key = digest(template.ref); ensure(!registry.has(key), 'Duplicate template'); registry.set(key, template);
  }
  return registry;
}
export function prepareRead(input: unknown, registry: ReturnType<typeof registeredTemplates>, settings: { bucket: string; maxWindowMs: number; maxRows: number }) {
  const request = boundedRequest(input);
  ensure(request && Object.keys(request).sort().join(',') === 'parameters,templateRef,window', 'Unsupported read request');
  assertRef(request.templateRef);
  const template = registry.get(digest(request.templateRef)); ensure(template, 'Unregistered query template');
  const window = request.window;
  ensure(window && Object.keys(window).sort().join(',') === 'basis,end,start' && window.basis === 'source', 'Explicit source window required');
  assertTimestamp(window.start); assertTimestamp(window.end);
  ensure([window.start, window.end].every(at => !/\.\d{4}/u.test(at)), 'Query bounds support millisecond precision');
  const elapsed = Date.parse(window.end) - Date.parse(window.start);
  ensure(elapsed > 0 && elapsed <= settings.maxWindowMs, 'Read window exceeds bounds');
  ensure(request.parameters && typeof request.parameters === 'object' && !Array.isArray(request.parameters) && Object.keys(request.parameters).sort().join(',') === Object.keys(template.parameters).sort().join(','), 'Unexpected query settings');
  const values = new Map<string, string>([['bucket', literal(settings.bucket, 'string')], ['start', literal(window.start, 'string')], ['end', literal(window.end, 'string')], ['limit', String(settings.maxRows + 1)]]);
  for (const [key, kind] of Object.entries(template.parameters)) values.set(key, literal(request.parameters[key], kind));
  const queryText = template.text.replace(/\{\{([A-Za-z][A-Za-z0-9_]*)\}\}/gu, (_match, key: string) => values.get(key)!);
  return { templateRef: request.templateRef, parameters: request.parameters, window, bucket: settings.bucket, queryText, querySha256: createHash('sha256').update(queryText).digest('hex') };
}
export function readSettings(options: InfluxDbReaderOptions) {
  const copy = { maxRows: 20000, maxBytes: 1024 * 1024, maxWindowMs: 86400000, timeoutMs: 30000, ...options };
  ensure(typeof copy.getToken === 'function' && typeof (copy.fetch ?? globalThis.fetch) === 'function', 'Connection required');
  for (const [value, max] of [[copy.maxRows, 20000], [copy.maxBytes, 8 * 1024 * 1024], [copy.maxWindowMs, 86400000], [copy.timeoutMs, 30000]]) ensure(Number.isSafeInteger(value) && value! > 0 && value! <= max!, 'Invalid read limit');
  connectionName(copy.org); connectionName(copy.bucket);
  const url = influxEndpoint(copy.url, 'query', { org: copy.org });
  return { ...copy, url, fetch: copy.fetch ?? globalThis.fetch, clock: copy.clock ?? (() => new Date().toISOString()), registry: registeredTemplates(copy.templates) };
}
