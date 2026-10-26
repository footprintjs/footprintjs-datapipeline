# Bounded InfluxDB reads with evidence

The optional `/influxdb` reader imports existing stored data through normal
`SourceAdapter`, `collect` and repository contracts. It adds no scheduler,
permissions framework, database, graph or model query generator. Vendor and
customer schemas and topology meanings belong to application adapters.

## Register reviewed templates

```ts
import { createInfluxDbReader, createInfluxDbSourceAdapter } from 'footprintjs-datapipeline/influxdb';

const query = {
  ref: { id: 'app.saved-sensor-rows', version: '1' },
  parameters: { host: 'string' as const },
  text: `from(bucket: {{bucket}})
    |> range(start: time(v: {{start}}), stop: time(v: {{end}}))
    |> filter(fn: (r) => r._measurement == "sensor")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> filter(fn: (r) => r.host == {{host}})
    |> group(columns: []) |> limit(n: {{limit}})`,
};
const reader = createInfluxDbReader({
  url: configuredUrl, org: configuredOrg, bucket: configuredBucket,
  getToken: resolveReadOnlyToken, templates: [query],
});
const adapter = createInfluxDbSourceAdapter({
  ref: { id: 'app.stored-sensors', version: '1' }, reader,
});
// Register adapter/source with createPipeline, then use pipeline.collect.
const request = {
  templateRef: query.ref,
  window: { basis: 'source', start: '2026-10-20T10:00:00.000Z', end: '2026-10-20T10:01:00.000Z' },
  parameters: { host: 'configured-host' },
};
```

Templates are trusted, versioned application code. This transport does not parse
Flux to certify that a template has no side effects. Register reviewed read-only
templates and provision a server-enforced read-only token. The client cannot
infer the token's grants. Neither query text nor endpoints come from the model.
A model-facing tool should expose bounded settings only and retain the app's
permission checks. This reader is not itself an agent tool or an authorizer.

Templates use `{{name}}` bindings, outside quotes. Reserved bindings are bucket,
start, end and limit (configured maximum rows plus one, so overflow is detectable).
All must occur. Other bindings declare string, integer or boolean types. Values
are escaped as Flux literals, including `${...}` interpolation. Undeclared
settings, unknown template revisions, raw-query properties and invalid windows
are refused before credential resolution or HTTP. Revisions have no latest fallback.

Use the declared schema to select measurement/field columns, reshape into rows,
and then filter fields such as an address. Filtering a field as though it were
a tag can silently select nothing. Do not move field filters ahead of the
reshape to obtain a faster but different query. A template must apply its result
limit globally after grouping as needed; the library cannot certify Flux's plan.

## Bounds and transport

Defaults: 20,000 returned rows, 1 MiB response, 24-hour window and a 30-second
whole-read budget. Limits may be lowered; response size may be raised to at most
8 MiB. Windows are explicit nonempty half-open `[start,end)` instants, with at
most millisecond precision. The budget includes credentials, headers and streamed
body reads. Cancellation bounds waiting even when a host callback ignores it.
Redirects are refused. TLS uses platform certificate verification. No retry occurs.

CSV is consumed record by record from the bounded body. Quoted commas, newlines,
doubled quotes, repeated table headers and default annotations are supported.
Cells stay strings, including identifiers with leading zeros and int64 values.
The application explicitly interprets metric types. A late CSV error table can
arrive after HTTP 200; it refuses the entire read. Malformed UTF-8, rows, excessive
columns/cells, row/byte overflows and credential echoes also refuse the result.
No usable partial prefix or source exception text is returned.

## Receipt and saved evidence

Each read returns immutable rows and one receipt. The receipt names the template
revision, exact expanded query and settings, configured bucket/org, requested
window, dispatch status, request/finish clocks, row count and fixed error code.
Every receipt, including failed or refused reads, records the effective
`limits: { maxRows, maxBytes, maxWindowMs, timeoutMs }`. A limit failure returns
no rows and never a complete prefix. HTTP failures also retain the status and
a bounded request ID when supplied; private error bodies are withheld.
Success includes the SHA-256 of response bytes and of canonical decoded rows.
The source adapter saves each returned row once as an ordinary observation; the
receipt belongs in its existing batch evidence details. It does not repeat all
rows inside the receipt. Generic rows declare source time unknown. The requested
window scopes coverage and the receipt; it never becomes a row's validity interval.
Application normalization explicitly assigns timestamp or aggregate-window meanings.

`source: 'influxdb-query'` is the evidence boundary. A completed query does not
prove upstream polling was complete, that missing rows mean zero, or that a
saved row is the original device response. Upstream coverage stays `unknown`.
Numbers Python changed or discarded cannot be reconstructed by this reader.

`sourceMutable: true` and `sourceRowsFingerprint: 'not-captured'` are explicit.
A saved aggregate result is immutable; the underlying database is not. The
reader promises no atomic source snapshot, repeatable read or continuous topology.
Late arrivals, corrections, deletion and retention can change subsequent reads.
A returned aggregate's hash is not a hash of all its contributing source rows.

For drill-down, run a registered bounded row template, save that new result, and
page the saved result with the app-authorized evidence viewer. Recheck permission
on every page. Never page live database results across turns or overwrite an old
finding. If a new aggregate differs, report that difference. If it matches, the
contributing rows may still have changed: report change status as unknown unless
an original row-set fingerprint or stronger source version was actually captured.
An empty later read alone cannot distinguish expiry from deletion or collection
absence. Do not invent a more specific reason without evidence.

## Delegate calculation only after equivalence tests

Transport support does not authorize any calculation. Each app maintains a
versioned list of permitted query calculations, tied to its template and reference
method. Test against a real disposable InfluxDB using identical saved inputs and
explicit windows. Include boundary timestamps, empty windows, partial last windows,
field-vs-tag filtering, float tolerance and late/corrected data. Unsupported
calculations remain disabled or use the existing reference transform.

`test/influx-reader-live.test.ts` exercises count, sum, min, max and mean against
this library's existing numeric-sample reference, with a true read-only token.
It uses the existing loopback-only disposable-bucket helper; CI requires that
server. Local absence is a counted skip, never proof of database equivalence.
This generic fixture does not validate vendor-specific calculations or establish
operation-weighted latency, percentiles, root cause or VM ownership.

No existing file, collector, write policy, retention or application flow changes
when this optional API is unused. Existing `/influxdb` writes retain their behavior.
