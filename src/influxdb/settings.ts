import { InfluxValidationError, type InfluxDbSinkOptions } from './contracts.js';
import { normalizeInfluxLimits } from './limits.js';

function checkedName(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 1024 || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new InfluxValidationError('invalid-options');
  }
  return value;
}

function writeUrl(raw: string, org: string, bucket: string): string {
  try {
    const url = new URL(raw);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
      throw new Error();
    }
    url.pathname = `${url.pathname.replace(/\/+$/u, '')}/api/v2/write`;
    url.search = new URLSearchParams({ org, bucket, precision: 'ns' }).toString();
    return url.href;
  } catch {
    throw new InfluxValidationError('invalid-options');
  }
}

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
    url: writeUrl(options.url, checkedName(options.org), checkedName(options.bucket)),
    getToken: options.getToken, fetch, timeoutMs, limits: normalizeInfluxLimits(options.limits),
  });
}
