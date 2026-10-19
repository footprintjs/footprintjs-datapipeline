import { InfluxValidationError, type InfluxBatchReceipt, type InfluxDbSink,
  type InfluxDbSinkOptions, type InfluxPoint, type InfluxWriteCode } from './contracts.js';
import { createWriteBudget, type StopCode } from './budget.js';
import { encodePointLines } from './encoding.js';
import { partitionLines } from './partition.js';
import { writeReceipt } from './receipts.js';
import { readSinkSettings } from './settings.js';
import { inspectPointCount, writeSignal } from './writeOptions.js';

function discardBody(response: Response) {
  try { void response.body?.cancel().catch(() => {}); } catch { /* No response text is read. */ }
}

function tokenValue(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 8192 || /[\u0000-\u0020\u007f]/u.test(value)) {
    throw new Error('Credentials unavailable.');
  }
  return value;
}

type WritePhase = 'options' | 'validation' | 'credentials' | 'transport';

function failureCode(error: unknown, phase: WritePhase, stopped?: StopCode): InfluxWriteCode {
  if (stopped) return stopped;
  if (phase === 'options') return 'invalid-options';
  if (phase === 'validation') {
    try {
      const descriptor = error && typeof error === 'object' ? Object.getOwnPropertyDescriptor(error, 'code') : undefined;
      if (descriptor && 'value' in descriptor && descriptor.value === 'limit-exceeded') return 'limit-exceeded';
    } catch { /* Never inspect a foreign exception further. */ }
    return 'invalid-data';
  }
  return phase === 'credentials' ? 'credentials-unavailable' : 'transport-error';
}

/** Sequential, bounded delivery; a failed dispatched batch is never retried. */
export function createInfluxDbSink(options: InfluxDbSinkOptions): InfluxDbSink {
  let settings: ReturnType<typeof readSinkSettings>;
  try { settings = readSinkSettings(options); } catch { throw new InfluxValidationError('invalid-options'); }
  return Object.freeze({
    async write(points: readonly InfluxPoint[], options: { readonly signal?: AbortSignal } = {}) {
      let total = inspectPointCount(points);
      const receipts: InfluxBatchReceipt[] = [];
      let budget: ReturnType<typeof createWriteBudget> | undefined;
      let phase: WritePhase = 'options';
      let dispatched = false;
      let index = 0;
      let count = 0;
      try {
        budget = createWriteBudget(settings.timeoutMs, writeSignal(options));
        const activeBudget = budget;
        phase = 'validation';
        const lines = encodePointLines(points, settings.limits);
        total = lines.length;
        const batches = partitionLines(lines, settings.limits);
        if (!batches.length) return writeReceipt(total, receipts);
        budget.check();
        phase = 'credentials';
        const token = tokenValue(await budget.wait(settings.getToken));
        phase = 'transport';
        for (const batch of batches) {
          budget.check();
          count = batch.pointCount;
          const response = await budget.wait(() => {
            activeBudget.check();
            dispatched = true;
            return settings.fetch(settings.url, { method: 'POST', redirect: 'error',
              headers: { Authorization: `Token ${token}`, 'Content-Type': 'text/plain; charset=utf-8' },
              body: batch.body, signal: activeBudget.signal }).then(response => {
                if (activeBudget.signal.aborted) discardBody(response);
                return response;
              });
          });
          try {
            const status: unknown = response.status;
            if (typeof status !== 'number' || !Number.isInteger(status) || status < 100 || status > 599) throw new Error();
            budget.check();
            if (status !== 204) {
              receipts.push({ index, pointCount: count, status: 'unknown', code: 'http-error', httpStatus: status });
              return writeReceipt(total, receipts, 'http-error');
            }
            receipts.push({ index, pointCount: count, status: 'acknowledged' });
            dispatched = false;
          } finally {
            discardBody(response);
          }
          index++;
        }
        return writeReceipt(total, receipts);
      } catch (error) {
        const code = failureCode(error, phase, budget?.reason());
        if (dispatched) receipts.push({ index, pointCount: count, status: 'unknown', code });
        return writeReceipt(total, receipts, code);
      } finally {
        budget?.close();
      }
    },
  });
}
