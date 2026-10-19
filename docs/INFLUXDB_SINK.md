# Optional InfluxDB 2 sink

`footprintjs-datapipeline/influxdb` exports `encodeLineProtocol`,
`createInfluxDbSink`, their types, `INFLUX_DEFAULT_LIMITS` and
`InfluxValidationError`. This is an opt-in output adapter, independent of
`Repository.commit`. Existing collection, derivation and scheduling behavior
does not change. There is no new dependency or automatic background work.

## Typed points and time

A point has a measurement, ordered tag pairs, ordered field pairs and a required
`timestampNs`. Each field declares its type: finite `float` number, signed
`integer` decimal string, `boolean`, or `string`. Strings and signed integers
are never inferred from JavaScript numbers. Unsigned integer fields are outside
this initial profile. Duplicate keys, empty tag values and empty field lists
are refused rather than merged, omitted or converted. Empty string fields and
false/zero values remain distinct from missing fields.

Integer fields use the signed int64 range. Timestamp strings use InfluxDB's
narrower range, `-9223372036854775806` through `9223372036854775806`.
Internally both are checked with `BigInt`; the JSON-safe public values stay
decimal strings. The encoder never reads a clock, changes a timestamp, invents
missing values, sorts points or deduplicates them. The host must choose which
moment is stamped and whether it is observation, poll-start or response time.

Float spelling may differ across languages: `5000` and `5000.0` are both Influx
floats, while `5000i` is an integer. Compare stored typed values rather than raw
text when checking two implementations. Do not parse timestamps or int64 fields
through `Number`. Equal fixture clocks and separate live poll times are different
comparison cases.

The Python interoperability test uses five explicit decimal clock readings in
the declared seconds range below `2^31`. Node supplies whole nanoseconds; Python
uses `int(float(seconds) * 1e9)`. Their fixture-only allowance is **256 ns**,
covering the floating-point clock/product rounding in that range. It rejects a
one-millisecond shift to a different event. This allowance never rewrites a
stored timestamp, merges nearby points or excuses a live collection delay.
The real-database test separately verifies that supplied timestamps survive
exactly. Vendor timestamp policy belongs in the adapter.

## Lossless encoding profile

The encoder applies standard delimiter escaping and double-quotes string fields.
It preserves ordered input and ordinary Unicode, quotes and internal backslashes.
It does not perform a vendor collector's lossy normalization. Inputs must be
finite plain data: no accessors, holes, hidden fields or unexpected keys.
Malformed UTF-16, CR/LF/NUL, reserved names and unsupported spellings are refused
with fixed diagnostic text. Identifier tabs and terminal backslashes are refused.

InfluxDB 2.7.12 round trips exposed additional non-lossless forms: a literal
backslash immediately before comma, space, equals or double quote in measurement
or field names. Those are refused before I/O. The corresponding tag names and
values are supported and tested. String field backslashes/quotes are escaped
without changing their values. This bounded profile does not claim to serialize
every legacy wire token that a collector may have emitted. Applications must
record refused data as a gap and decide compatibility changes explicitly.

Each string is limited to 64 KiB in UTF-8, each point to 256 tags and 256 fields,
and the encoded series key to InfluxDB's 65,535-byte ceiling. A call admits at
most 10,000 points and 8 MiB, including newline separators. Requests contain at
most 5,000 points and 1 MiB. Host limits can lower these ceilings; invalid limits
are refused. No oversized point is split and no prefix is silently published.
The whole call is encoded before credentials or HTTP, so a bad later point
cannot allow an earlier valid point to escape first.

### Point-local refusals

An encoder `InfluxValidationError`, or the sink's `not-written` receipt, may
include `rejectedPointIndex`: the zero-based position of the first refused point
in **this call's** input. It contains no name, field value or exception text.
The encoder assigns that position itself; caller-thrown errors cannot supply it.
Whole-call byte/count budgets, malformed outer arrays, options and delivery
failures have no point index. A point exceeding its individual request-size
ceiling can have an index; this means it cannot fit the configured request,
not that its source observation is false.

Preflight still rejects the entire call with zero dispatches. The sink does not
drop points or resend the remainder. A host may apply an explicit admission
policy, record that point's source reference and fixed reason as a coverage gap,
then submit a new bounded list. Keep a new ordered manifest for that list:
indexes never refer to an earlier call. Do not discard a healthy point to satisfy
an aggregate budget, or claim complete coverage after omitting required data.
Vendor adapters must first reproduce their existing collector's cleanup exactly;
this diagnostic is not a replacement for the Python compatibility contract.

## Explicit delivery and receipts

The host supplies `url`, `org`, `bucket`, `getToken` and optional limits,
`timeoutMs`, and a fetch implementation. Resolve the raw token in the callback,
outside recorded point data. Destination configuration is trusted host input,
never model-selected. HTTP/HTTPS base paths are supported; userinfo, query and
fragment parts are refused. The sink posts sequential bounded requests to
`/api/v2/write`, always with `precision=ns`, and refuses redirects.

Only HTTP 204 acknowledges a batch. Other statuses, a lost connection or a
deadline can leave that batch partially stored. Server bodies are cancelled
without reading them. Receipts contain fixed codes and validated numeric status
codes, never URL, response text, tokens or arbitrary exception diagnostics.

| Status | Meaning |
|---|---|
| `empty` | Validated empty input; no credentials or HTTP used |
| `acknowledged` | Every submitted point received a 204 acknowledgement |
| `partial` | Earlier batches acknowledged, with later uncertain or unattempted points |
| `unknown` | At least one batch dispatched without acknowledgement; no earlier batch acknowledged |
| `not-written` | No batch dispatched, for example validation, credentials or cancellation failure |

Every receipt satisfies `totalPoints = acknowledgedPoints + uncertainPoints +
notAttemptedPoints`. Batch receipts describe dispatched requests only. Rejected
input counts are diagnostic and can be zero when malformed data cannot safely
be counted. Acknowledged counts are submissions, not distinct stored points:
InfluxDB can merge/overwrite repeated point identities. Order remains significant.

The whole-call budget defaults to and cannot exceed 30 seconds. It includes
credential resolution and HTTP waits. Cancellation stops waiting even if a host
callback ignores its signal; late completion cannot send the next batch or turn
an uncertain request into acknowledgement. Synchronous host code cannot be
preempted; the budget is checked again before accepting its result. No automatic
retry, durable queue, exactly-once delivery or reconciliation is supplied.

### Replaying an unknown write

InfluxDB identifies a point by measurement, tag set and timestamp, and merges its
fields on repeated writes. Replaying the same ordered payload to the same
destination preserves the stored result when no competing write has changed
those same fields and no earlier request can still apply updates out of order.
Retain the admitted typed points and their exact timestamps:
never recollect, read a new clock, recompute values or change the destination on
retry. Preserve point order, including any overlapping field updates. A delayed
replay can overwrite a later correction, so the host must coordinate ownership
and ordering, including any previous request that may still be running; an
`unknown` receipt is not blanket permission to retry.

The sink never retries automatically. A host-authorized replay is a new attempt
with a separate receipt linked to the same immutable manifest. A later 204 does
not rewrite the earlier attempt's uncertainty or prove exactly-once delivery.
Disposable-database tests simulate a lost acknowledgement, replay the unchanged
payload and compare the stored values/types/timestamps. Another test proves
that replaying old values can overwrite a later correction at the same identity.

## Footprint and evidence ownership

Call `sink.write()` in a named Footprint step, with one recorded point per entity
and tracked reads of the source evidence. Store the receipt, then use a named
`decide()` rule to distinguish acknowledgement from uncertainty. Returning a
receipt lets the native execution record retain failure facts; a completed
JavaScript step alone does not mean Influx accepted its points.

The source repository and Influx are independent outputs. An Influx receipt
does not publish evidence, and a repository commit does not prove Influx delivery.
Persist source observations and exact row references under the existing evidence
rules; the sink neither stores raw responses nor grants authority to make claims.
The host owns crash recovery, receipt retention and any later retry policy.
Influx is not a replacement `Repository`: it cannot meet that port's atomic
data-plus-evidence publication contract.

The proposed host integration stores durable run facts in the existing
`Repository` and native flow records in the existing `TraceStore`; the sink does
not persist them itself. The [host receipt plan](INFLUXDB_HOST_RECEIPTS.md) names
the immutable source, point-manifest, intent and result records and their crash
rules. That integration remains application work.

## Verification and next integration

Tests cover typed encoding, preflight limits, escaping, malformed data, frozen
snapshots, cancellation, redirects, late callbacks and partial batch receipts.
`influx-flow.test.ts` uses ordinary native steps and a decision: a delivery traces
to its own point/source, excludes a later unrelated read, keeps source data when
delivery is uncertain and never records credentials.

CI runs Node 22 and 24 against a disposable InfluxDB 2.7.12 service. Tests create
unique buckets, verify decoded values/types/timestamps and delete those buckets.
They prove equivalent float spellings, ordered duplicate-point overwrite and
that a failed mixed batch really can store a valid row, plus the conditional
replay behavior above. Python 3 is required for
the clock probe and independent standard-library CSV reader. A missing Python
interpreter fails the tests. Without explicit loopback test configuration, eight
database tests are reported as skipped; CI requires them and fails without it.
No database is required to use the encoder; the sink connects only when called.

This library PR is the prerequisite for the first application writer. After a
reviewed release, the Neo adapter can pin that published version and prove the
complete saved client-response contract plus source-row links. PowerScale names,
top-N, sentinels, node pools, legacy zero-ID behavior and poll timestamp selection
remain there. No vendor collector, production write, service schedule, mapping
enablement or switch-over is part of this library change.

References: [InfluxDB 2 line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/)
and [write API](https://docs.influxdata.com/influxdb/v2/api/#tag/Write).
