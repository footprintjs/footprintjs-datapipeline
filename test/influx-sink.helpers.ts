import type { InfluxDbSinkOptions, InfluxPoint } from '../src/influxdb/contracts.js';

export const point = (value = 1): InfluxPoint => ({
  measurement: 'sample', tags: [['site', 'test']],
  fields: [['value', { type: 'float', value }]], timestampNs: '1792436400000000000',
});

export function options(fetch: typeof globalThis.fetch, extra: Partial<InfluxDbSinkOptions> = {}): InfluxDbSinkOptions {
  return { url: 'http://localhost:8086', org: 'test org', bucket: 'test bucket',
    getToken: () => 'synthetic-token', fetch, ...extra };
}

export const ok = (): Response => new Response(null, { status: 204 });
export const never = <T>(): Promise<T> => new Promise(() => {});

export async function bounded<T>(work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('Write did not respect its budget.')), 500);
    })]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
