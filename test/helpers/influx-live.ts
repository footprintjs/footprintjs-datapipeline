import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createInfluxDbSink } from '../../src/influxdb/index.js';

export const liveEnabled = !!process.env.INFLUX_TEST_URL;
if (process.env.INFLUX_TEST_REQUIRED === '1' && !liveEnabled) throw new Error('Disposable InfluxDB is required.');

export interface LiveRow { readonly type: string; readonly data: Readonly<Record<string, string>> }

/** Only loopback test instances and a new per-test bucket are permitted. */
export async function createLiveFixture() {
  const url = process.env.INFLUX_TEST_URL!;
  const token = process.env.INFLUX_TEST_TOKEN;
  const org = process.env.INFLUX_TEST_ORG;
  if (!/^http:\/\/127\.0\.0\.1:\d+$/u.test(url) || !token || !org) {
    throw new Error('Explicit loopback test configuration is required.');
  }
  async function request(path: string, init: RequestInit = {}) {
    const response = await fetch(url + path, { ...init, redirect: 'error', signal: AbortSignal.timeout(5000),
      headers: { Authorization: `Token ${token}`, ...init.headers } });
    if (!response.ok) { await response.body?.cancel(); throw new Error('Disposable InfluxDB setup/query failed.'); }
    return response;
  }
  const organizations = await (await request(`/api/v2/orgs?org=${encodeURIComponent(org)}`)).json() as {
    orgs: { id: string }[];
  };
  const orgID = organizations.orgs[0]?.id;
  if (!orgID) throw new Error('Disposable organization is absent.');
  const bucket = `footprint-writer-test-${randomUUID()}`;
  const created = await (await request('/api/v2/buckets', { method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ orgID, name: bucket, retentionRules: [] }),
  })).json() as { id: string };
  return {
    sink: (limits = {}) => createInfluxDbSink({ url, org, bucket, getToken: () => token, limits }),
    async rawWrite(body: string) {
      const response = await request(`/api/v2/write?org=${encodeURIComponent(org)}&bucket=${bucket}&precision=ns`, {
        method: 'POST', headers: { 'Content-Type': 'text/plain' }, body,
      });
      await response.body?.cancel();
    },
    async rows(): Promise<LiveRow[]> {
      const query = `from(bucket: "${bucket}") |> range(start: 2020-01-01T00:00:00Z, stop: 2040-01-01T00:00:00Z)
        |> map(fn: (r) => ({r with stamp: string(v: int(v: r._time))}))`;
      const csv = await (await request(`/api/v2/query?org=${encodeURIComponent(org)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/csv' },
        body: JSON.stringify({ query, type: 'flux', dialect: { header: true, delimiter: ',',
          annotations: ['datatype', 'group', 'default'] } }),
      })).text();
      const helper = fileURLToPath(new URL('./influx-csv.py', import.meta.url));
      return JSON.parse(execFileSync('python3', ['-I', helper], { input: csv, encoding: 'utf8',
        env: {}, timeout: 5000, maxBuffer: 1024 * 1024 })) as LiveRow[];
    },
    async close() { await (await request(`/api/v2/buckets/${created.id}`, { method: 'DELETE' })).body?.cancel(); },
  };
}
