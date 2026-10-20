import { discardInfluxBody as discard, influxToken } from '../connection.js';
import { deepFreeze } from 'foottrace/write';
import { assertTimestamp, digest } from '../../validation.js';
import { createWriteBudget } from '../budget.js';
import { writeSignal } from '../writeOptions.js';
import { readQueryBody } from './body.js';
import { parseInfluxCsv } from './csv.js';
import { ReadFault, type InfluxDbReader, type InfluxDbReaderOptions, type InfluxReadCode, type InfluxReadReceipt, type InfluxReadResult } from './contracts.js';
import { prepareRead, readSettings } from './templates.js';

export function createInfluxDbReader(options: InfluxDbReaderOptions): InfluxDbReader {
  const settings = readSettings(options);
  return Object.freeze({ async read(input: unknown, options: { readonly signal?: AbortSignal } = {}): Promise<InfluxReadResult> {
    let phase: 'request' | 'credentials' | 'query' = 'request';
    let prepared: ReturnType<typeof prepareRead> | undefined, requestedAt: string | null = null, dispatched = false, httpStatus: number | undefined, requestId: string | undefined;
    let budget: ReturnType<typeof createWriteBudget> | undefined;
    const clock = (): string | null => { try { const at = settings.clock(); assertTimestamp(at); return at; } catch { return null; } };
    const receipt = (status: 'complete' | 'failed', rowCount: number, extra: Partial<InfluxReadReceipt> = {}): InfluxReadReceipt => ({
      source: 'influxdb-query', status, requestedAt, finishedAt: clock(), dispatched, sourceMutable: true,
      sourceRowsFingerprint: 'not-captured', limits: { maxRows: settings.maxRows, maxBytes: settings.maxBytes, maxWindowMs: settings.maxWindowMs, timeoutMs: settings.timeoutMs }, rowCount, ...(prepared ? { ...prepared, org: settings.org } : {}), ...(httpStatus !== undefined ? { httpStatus } : {}), ...(requestId ? { requestId } : {}), ...extra });
    try {
      requestedAt = clock(); if (!requestedAt) throw new ReadFault('invalid-request');
      budget = createWriteBudget(settings.timeoutMs, writeSignal(options));
      prepared = prepareRead(input, settings.registry, settings);
      phase = 'credentials';
      const token = influxToken(await budget.wait(settings.getToken));
      if (JSON.stringify(prepared).includes(token)) { prepared = undefined; throw new ReadFault('credentials-unavailable'); }
      phase = 'query'; const active = budget;
      const response = await budget.wait(() => { active.check(); dispatched = true;
        return settings.fetch(settings.url, { method: 'POST', redirect: 'error', signal: active.signal,
          headers: { Authorization: `Token ${token}`, 'Content-Type': 'application/json', Accept: 'application/csv' },
          body: JSON.stringify({ query: prepared!.queryText, type: 'flux', dialect: { header: true, delimiter: ',', annotations: ['datatype', 'group', 'default'] } }),
        }).then(value => { if (active.signal.aborted) discard(value); return value; }); });
      httpStatus = response.status;
      const reference = response.headers.get('x-request-id') ?? response.headers.get('request-id');
      if (reference && /^[A-Za-z0-9_-]{1,256}$/.test(reference) && !reference.includes(token)) requestId = reference;
      if (response.status !== 200) { discard(response); throw new ReadFault('http-error'); }
      const body = await readQueryBody(response, settings.maxBytes, budget);
      if (body.text.includes(token)) throw new ReadFault('secret-echo');
      const rows = parseInfluxCsv(body.text, settings.maxRows);
      return deepFreeze({ outcome: 'complete', rows, receipt: receipt('complete', rows.length, { responseSha256: body.sha256, resultSha256: digest(rows) }) });
    } catch (error) {
      const code: InfluxReadCode = budget?.reason() ?? (phase === 'request' ? 'invalid-request' : phase === 'credentials' ? 'credentials-unavailable' : error instanceof ReadFault ? error.code : 'transport-error');
      return deepFreeze({ outcome: 'failed', rows: [], receipt: receipt('failed', 0, { code }) });
    } finally { budget?.close(); }
  } });
}
