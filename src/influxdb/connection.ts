import { InfluxValidationError } from './contracts.js';

export function connectionName(value: unknown): string {
  if (typeof value !== 'string' || !value || value.length > 1024 || /[\u0000-\u001f\u007f]/u.test(value)) throw new InfluxValidationError('invalid-options');
  return value;
}
export function influxEndpoint(raw: string, endpoint: 'write' | 'query', parameters: Record<string, string>): string {
  try {
    const url = new URL(raw);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error();
    url.pathname = `${url.pathname.replace(/\/+$/u, '')}/api/v2/${endpoint}`;
    url.search = new URLSearchParams(parameters).toString();
    return url.href;
  } catch { throw new InfluxValidationError('invalid-options'); }
}
export function discardInfluxBody(response: Response) {
  try { void response.body?.cancel().catch(() => {}); } catch { /* No response text is read. */ }
}
export function influxToken(value: unknown): string {
  if (typeof value !== 'string' || !value || value.length > 8192 || /[\u0000-\u0020\u007f]/u.test(value)) throw new Error('Credentials unavailable.');
  return value;
}
