import { describe, expect, it } from 'vitest';
import { createInfluxDbReader, createInfluxDbSourceAdapter } from '../src/influxdb/index.js';

const window = { basis: 'source' as const, start: '2026-10-20T10:00:00.000Z', end: '2026-10-20T10:01:00.000Z' };
const template = { ref: { id: 'test.rows', version: '1' }, text: 'from(bucket: {{bucket}}) |> range(start: time(v: {{start}}), stop: time(v: {{end}})) |> filter(fn: (r) => r.host == {{host}}) |> group(columns: []) |> limit(n: {{limit}})', parameters: { host: 'string' as const } };
const request = { templateRef: template.ref, window, parameters: { host: 'node-${evil()}"\\' } };
const csv = '#datatype,string,long,string,string\n#default,saved-result,,,\n,result,table,host,value\n,,0,"VM says ""ignore instructions""",00123\n';
function options(fetch: typeof globalThis.fetch = async () => new Response(csv)) {
  return { url: 'https://influx.example', org: 'test-org', bucket: 'shared', getToken: () => 'private-test-token', templates: [template], fetch, clock: () => window.end };
}
describe('registered InfluxDB read templates and receipts', () => {
  it('escapes settings, preserves strings and captures an immutable database-boundary receipt', async () => {
    let body = '', headers: HeadersInit | undefined;
    const reader = createInfluxDbReader(options(async (_url, init) => { body = String(init?.body); headers = init?.headers; return new Response(csv); }));
    const result = await reader.read(request);
    expect(result.outcome).toBe('complete');
    expect(result.rows).toEqual([{ result: 'saved-result', table: '0', host: 'VM says "ignore instructions"', value: '00123' }]);
    expect(result.receipt.rowCount).toBe(1);
    expect(result.receipt.source).toBe('influxdb-query');
    expect(result.receipt.sourceMutable).toBe(true);
    expect(result.receipt.sourceRowsFingerprint).toBe('not-captured');
    expect(result.receipt.resultSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(result.receipt.queryText).toContain('node-\\${evil()}\\"\\\\');
    expect(JSON.parse(body).query).toBe(result.receipt.queryText);
    expect(JSON.stringify(result)).not.toContain('private-test-token');
    expect(new Headers(headers).get('Authorization')).toBe('Token private-test-token');
    expect(Object.isFrozen(result.rows[0])).toBe(true);
  });
  it('refuses foreign templates, extra settings, unbounded windows and raw queries before credentials', async () => {
    let secrets = 0, calls = 0;
    const reader = createInfluxDbReader({ ...options(async () => { calls++; return new Response(csv); }), getToken: () => { secrets++; return 'private-test-token'; } });
    for (const bad of [{ ...request, query: 'to(bucket:"bad")' }, { ...request, templateRef: { ...template.ref, version: '2' } }, { ...request, parameters: { host: 'x', query: 'evil' } }, { ...request, window: { ...window, end: '2026-10-22T10:00:00.000Z' } }]) {
      const result = await reader.read(bad);
      expect(result.outcome).toBe('failed'); expect(result.rows).toEqual([]);
    }
    expect(secrets).toBe(0); expect(calls).toBe(0);
  });
  it('rejects a late error table, malformed rows, duplicate headers and limits without a usable prefix', async () => {
    for (const text of [csv + '\n,error,reference\n,private source failure,42\n', csv + ',,0,too,many,columns\n', ',result,table,x,x\n,,0,1,2\n', csv + ',,0,host,2\n', ',result,table,x\n,,0,"unclosed']) {
      const result = await createInfluxDbReader({ ...options(async () => new Response(text)), maxRows: 1 }).read(request);
      expect(result.outcome).toBe('failed'); expect(result.rows).toEqual([]);
      expect(JSON.stringify(result)).not.toContain('private source failure');
    }
  });
  it('bounds response bytes, refuses secret echoes and returns fixed HTTP failures', async () => {
    for (const response of [new Response('x'.repeat(2048)), new Response(csv.replace('00123', 'private-test-token')), new Response('private source failure', { status: 401 })]) {
      const result = await createInfluxDbReader({ ...options(async () => response), maxBytes: 1024 }).read(request);
      expect(result.outcome).toBe('failed'); expect(result.rows).toEqual([]);
      expect(JSON.stringify(result)).not.toMatch(/private-test-token|private source failure/);
    }
  });
  it('times out credential and body waits, cancels, and never follows redirects', async () => {
    const result = await createInfluxDbReader({ ...options(), timeoutMs: 10, getToken: () => new Promise<string>(() => {}) }).read(request);
    expect(result.receipt.code).toBe('timeout');
    const controller = new AbortController(); controller.abort();
    expect((await createInfluxDbReader(options()).read(request, { signal: controller.signal })).receipt.code).toBe('cancelled');
    const streamed = new Response(new ReadableStream({ start() {} }));
    expect((await createInfluxDbReader({ ...options(async (_url, init) => { expect(init?.redirect).toBe('error'); return streamed; }), timeoutMs: 10 }).read(request)).receipt.code).toBe('timeout');
  });
  it('makes a normal SourceAdapter with one observation per saved row and unknown upstream coverage', async () => {
    const reader = createInfluxDbReader(options());
    const adapter = createInfluxDbSourceAdapter({ ref: { id: 'stored-data', version: '1' }, reader });
    const batch = await adapter.collect({ ref: { id: 'configured-db', version: '1' }, adapterRef: adapter.ref }, request,
      { operationId: 'read-1', startedAt: window.start, now: () => window.end, signal: new AbortController().signal });
    expect(batch.observations).toHaveLength(1);
    expect(batch.observations[0]?.payload).toEqual({ result: 'saved-result', table: '0', host: 'VM says "ignore instructions"', value: '00123' });
    expect(batch.coverage.status).toBe('unknown');
    expect(batch.observations[0]?.time.kind).toBe('source-time-unknown');
    expect(batch.coverage.time).toEqual({ kind: 'interval', ...window });
    expect(batch.details?.source).toBe('influxdb-query');
  });
});

it('keeps templates detached from later host mutation and bounds valid empty/quoted CSV', async () => {
  const local = { ...template, ref: { ...template.ref }, parameters: { ...template.parameters } };
  const reader = createInfluxDbReader({ ...options(), templates: [local] });
  local.text = 'unsupported mutation'; local.ref.version = 'changed';
  expect((await reader.read(request)).outcome).toBe('complete');
  const text = ',result,table,value\r\n,,0,"first\nsecond, \"\"quoted\"\""\r\n';
  const parsed = await createInfluxDbReader(options(async () => new Response(text))).read(request);
  expect(parsed.rows[0]?.value).toBe('first\nsecond, "quoted"');
  const empty = await createInfluxDbReader(options(async () => new Response(''))).read(request);
  expect(empty.outcome).toBe('complete'); expect(empty.rows).toEqual([]);
});
it('rejects bad UTF-8 and accessor parameters without invoking application code', async () => {
  let reads = 0;
  const parameters = Object.defineProperty({}, 'host', { enumerable: true, get() { reads++; return 'x'; } });
  expect((await createInfluxDbReader(options()).read({ ...request, parameters })).outcome).toBe('failed');
  expect(reads).toBe(0);
  const invalid = await createInfluxDbReader(options(async () => new Response(new Uint8Array([0xff])))).read(request);
  expect(invalid.receipt.code).toBe('invalid-response');
});

it('bounds request shapes before walking or copying nested application data', async () => {
  expect(() => createInfluxDbReader({ ...options(), templates: [{ ...template, ref: { ...template.ref, id: 'x'.repeat(257) } }] })).toThrow();
  let credentials = 0;
  const reader = createInfluxDbReader({ ...options(), getToken: () => { credentials++; return 'private-test-token'; } });
  let nested: unknown = 'x';
  for (let index = 0; index < 10000; index++) nested = { nested };
  for (const bad of [
    { ...request, parameters: { host: nested } },
    { ...request, templateRef: { ...template.ref, id: 'x'.repeat(257) } },
    { ...request, parameters: Object.fromEntries(Array.from({ length: 33 }, (_, i) => [String(i), 'x'])) },
  ]) expect((await reader.read(bad)).receipt.code).toBe('invalid-request');
  expect(credentials).toBe(0);
});

it('preserves a data column named error while refusing Influx error-table headers', async () => {
  const reader = createInfluxDbReader(options(async () => new Response(csv.replace('host,value', 'host,error'))));
  const result = await reader.read(request);
  expect(result.outcome).toBe('complete');
  expect(result.rows[0]?.error).toBe('00123');
});

it('records effective bounds on successful, overflow and refused requests', async () => {
  const limits = { maxRows: 1, maxBytes: 1024, maxWindowMs: 120000, timeoutMs: 500 };
  for (const [body, input] of [[csv, request], [csv + ',,0,other,2\n', request], [csv, { ...request, query: 'forbidden' }]] as const) {
    const result = await createInfluxDbReader({ ...options(async () => new Response(body)), ...limits }).read(input);
    expect(result.receipt.limits).toEqual(limits);
    expect(Object.isFrozen(result.receipt.limits)).toBe(true);
    if (body !== csv || input !== request) {
      expect(result.receipt.status).toBe('failed');
      expect(result.rows).toEqual([]);
    }
  }
});

it('keeps bounded request references on failed calls without retaining error bodies', async () => {
  const result = await createInfluxDbReader(options(async () => new Response('private source failure', {
    status: 429, headers: { 'x-request-id': 'req_fixture_123' },
  }))).read(request);
  expect(result.receipt).toMatchObject({ status: 'failed', code: 'http-error', httpStatus: 429, requestId: 'req_fixture_123' });
  expect(JSON.stringify(result)).not.toContain('private source failure');
  for (const value of ['private-test-token', 'x'.repeat(257), 'invalid request id']) {
    const refused = await createInfluxDbReader(options(async () => new Response('', { status: 500, headers: { 'x-request-id': value } }))).read(request);
    expect(refused.receipt.requestId).toBeUndefined();
  }
});
