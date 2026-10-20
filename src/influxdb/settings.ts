import { InfluxValidationError, type InfluxDbSinkOptions } from './contracts.js';
import { connectionName, influxEndpoint } from './connection.js';
import { normalizeInfluxLimits } from './limits.js';

export function readSinkSettings(options: InfluxDbSinkOptions) {
  if (!options || typeof options !== 'object' || typeof options.getToken !== 'function') {
    throw new InfluxValidationError('invalid-options');
  }
  const timeoutMs = options.timeoutMs ?? 30_000;
  const fetch = options.fetch ?? globalThis.fetch;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000 || typeof fetch !== 'function') {
    throw new InfluxValidationError('invalid-options');
  }
  return Object.freeze({
    url: influxEndpoint(options.url, 'write', { org: connectionName(options.org), bucket: connectionName(options.bucket), precision: 'ns' }),
    getToken: options.getToken, fetch, timeoutMs, limits: normalizeInfluxLimits(options.limits),
  });
}
