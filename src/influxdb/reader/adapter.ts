import type { JsonObject, VersionedRef } from '../../contracts.js';
import type { SourceAdapter } from '../../ports.js';
import type { InfluxDbReader } from './contracts.js';

/** Normal collect flow; one saved observation per returned query row, one receipt per read. */
export function createInfluxDbSourceAdapter(options: { readonly ref: VersionedRef; readonly reader: InfluxDbReader }): SourceAdapter {
  return { ref: options.ref, async collect(source, request, context) {
    const result = await options.reader.read(request, { signal: context.signal });
    const time = result.receipt.window ? { kind: 'interval' as const, ...result.receipt.window } : { kind: 'source-time-unknown' as const };
    return { key: context.operationId, outcome: result.outcome,
      observations: result.rows.map((row, index) => ({ key: `query-row-${index}`, subject: { sourceRef: { ...source.ref }, rowIndex: index },
        schemaRef: { id: 'influxdb.query-row', version: '1' }, time, ...(result.receipt.finishedAt ? { collectedAt: result.receipt.finishedAt } : {}), payload: row })),
      coverage: { status: 'unknown', time, reason: 'Query completion does not establish upstream collection coverage or original device evidence.' },
      details: JSON.parse(JSON.stringify(result.receipt)) as JsonObject,
      ...(result.outcome === 'failed' ? { errors: [{ code: result.receipt.code ?? 'query-failed', message: 'The stored-data query could not complete.' }] } : {}) };
  } };
}
