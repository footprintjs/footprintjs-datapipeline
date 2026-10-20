# footprintjs-datapipeline

A small TypeScript library for collecting observations, deriving new observations, and reading the evidence behind them. Collection and derivation execute as FootPrint flows. Adapters own acquisition, transforms own computation, and a repository owns durable publication.

Node.js 22.13 or later is required. Build and run this checkout with the commands below.

## Run the example

```sh
npm install
npm run check
npm run example
```

[examples/basic.ts](examples/basic.ts) defines a local adapter that collects two numeric readings and a transform that calculates their mean. It collects, retries the same request, derives the mean, then reads both batches, their observations, and their structured evidence. Data and full execution traces use separate filesystem stores in a temporary directory that the example removes when it finishes. It needs no external service or credentials.

The derived payload is `{ "mean": 12, "count": 2, "unit": "example-unit" }`. Both operations report a committed batch and a stored trace.

```text
src/
  contracts.ts             record and request types
  ports.ts                 adapter, transform and storage interfaces
  createPipeline.ts        public collect / derive / read operations
  flows/collect.ts         FootPrint collection flow
  flows/derive.ts          FootPrint derivation flow
  prepared/contracts.ts    schedules, prepared summaries and their storage port
  prepared/scheduler.ts    bounded, restartable collect-and-derive scheduling
  storage/filesystem.ts    optional local repository and trace store
  storage/sqliteSummaries.ts optional transactional summary time index
examples/basic.ts          runnable collection and derivation
test/                     contract, flow and filesystem checks
```

## The three operations

Register exact versions of your sources, adapters and transforms. A source names its adapter and may carry public configuration. Resolve credentials inside your adapter without placing them in persisted configuration, requests, payloads or error messages.

With the `source`, `adapter` and `mean` definitions from the example:

```ts
import { createPipeline } from 'footprintjs-datapipeline';
import {
  createFilesystemRepository,
  createFilesystemTraceStore,
} from 'footprintjs-datapipeline/filesystem';

const pipeline = createPipeline({
  sources: [source],
  adapters: [adapter],
  transforms: [mean],
  repository: createFilesystemRepository({ directory: '.pipeline-data/records' }),
  traceStore: createFilesystemTraceStore({ directory: '.pipeline-data/traces' }),
});

const collected = await pipeline.collect({
  sourceRef: source.ref,
  idempotencyKey: 'collect-window-1',
  request: { series: 'sample-series' },
});

if (collected.commit.status !== 'committed' || !collected.commit.batchRef) {
  throw new Error('Inspect the commit receipt before continuing.');
}

const derived = await pipeline.derive({
  methodRef: mean.ref,
  idempotencyKey: 'derive-window-1',
  inputBatchRefs: [collected.commit.batchRef],
  window: {
    basis: 'source',
    start: '2026-10-01T12:00:00Z',
    end: '2026-10-01T12:02:00Z',
  },
});

if (derived.commit.evidenceRef) {
  console.log(await pipeline.read(derived.commit.evidenceRef));
}
```

`collect` and `derive` return `{ commit, trace }`. `read` accepts a batch, observation or evidence reference and returns the published record, or `undefined` when it is absent. `reconcileCollect(request)` looks up the exact collection request and source configuration without acquiring data: it returns its saved receipt, `undefined` when absent, or a failed receipt for a conflict or lookup failure. It does not write a new execution trace.

## Write a plugin

Implement `SourceAdapter.collect(source, request, context)` or `Transform.apply(inputs, request, context)`. Each returns one finite `CandidateBatch` containing a logical key, observations, coverage and an outcome. The core assigns durable references and builds structured evidence. Plugin code receives an `AbortSignal` and a stable operation identity through `context`.

An observation has a subject, a versioned schema, an explicit time basis, and either a JSON payload or an external artifact reference. A transform can name the exact input observations supporting each output with `inputRefs`. When it omits those references, the record explicitly identifies batch-level support instead.

Keep service-specific clients and adapters in separate packages. The core has no infrastructure vendor SDK or assumed metric schema. Implement the `Repository` and optional `TraceStore` ports to integrate other storage systems.

For example, a separate NeoPowerScale integration can expose this boundary around its existing collector:

```ts
import type {
  CandidateBatch, JsonValue, OperationContext, Source,
} from 'footprintjs-datapipeline';

interface NeoPowerScaleCollector {
  collect(
    source: Source,
    request: JsonValue,
    context: OperationContext,
  ): Promise<CandidateBatch>;
}
```

That collector can supply the `collect` method of a versioned `SourceAdapter`. The integration owns authentication, vendor API calls, mapping to observation schemas, and honest coverage declarations. This interface documents the boundary; this package supplies no NeoPowerScale client or working vendor adapter.

## What the records mean

- **Idempotency:** reuse a key for retries of the same operation. A different request or configuration under the same operation identity is a conflict. This supports reconciliation of stored results; it does not guarantee that external adapter side effects execute exactly once.
- **Publication and completeness:** `commit.status` describes the repository acknowledgement. A successfully stored batch can still have `outcome: 'partial'` or `coverage.status: 'unknown'`. Inspect all of them. An `unknown` commit outcome requires reconciliation; it does not establish that nothing was stored.
- **Coverage:** adapters and transforms declare what population and time interval their result covers. Empty data is not automatically complete, and missing information is not zero. The library does not infer the full population of an external system.
- **Time:** source time and collection time are distinct. A source with no known timestamp can report `source-time-unknown`. The core fills an omitted observation `collectedAt` from its capture clock, independently of source time. Derivation windows are half open `[start, end)` and explicitly name their clock basis.
- **Lineage:** evidence records the exact source/adapter or transform revisions, request and input batch references. These links describe data provenance; they do not establish a physical cause in an external system.
- **Execution traces:** full traces are stored after the data outcome is known, independently of data publication. Trace storage can fail while a data commit succeeds. Check `trace.status`; omitting a trace store reports `disabled`.

Before calling a transform, the core filters input observations to the requested window. For a `source` window, it selects source points in `[start, end)` and source intervals fully contained in the window; it does not split or prorate an overlapping interval. Intervals straddling a boundary, unknown source times and times on a different basis are excluded and conservatively limit the output's coverage. For a `collection` window, it selects by the observation's `collectedAt`, retaining the separate source-time fields. Derivation evidence records the selected observation references and `excludedUnknownTimes`, the count of observations excluded because their source time cannot be assigned wholly to the window.

Use bounded collection requests and finite plugin results. The default limits are 10,000 observations and 8 MiB of encoded JSON per candidate batch, configurable through `createPipeline({ limits: { maxObservations, maxBytes }, ... })`. The core rejects an oversized candidate rather than automatically chunking it. Cancellation is cooperative: plugins must honor `context.signal`; a timeout cannot undo a remote operation they already performed.

Both operations accept a second argument `{ signal, timeoutMs }`. A timeout before publication prevents later pipeline stages from publishing. Once a repository commit has begun, cancellation can leave an `unknown` storage outcome; retry the same key to reconcile it. A timeout while storing the execution trace preserves an already acknowledged data result and reports the trace as unavailable. Concurrent callers for the same request on one pipeline instance share the first caller's execution and cancellation options.

## Filesystem backend

The optional `/filesystem` subpath provides a reference repository and trace store. The repository publishes an operation's records and structured evidence together using a staged directory, content hashes, directory rename and synchronization. It reads only published records and checks stored hashes.

This backend targets one host and a modest number of operations on a local POSIX filesystem with atomic directory rename and working `fsync`. Record lookup scans published manifests. It is not a distributed storage service or a tamper-evident archive. External artifact references are rejected because this backend does not verify their bytes; use inline payloads or supply an artifact-aware repository.

The filesystem trace store also offers `read(traceRef)` for inspecting a stored trace. Full traces may contain plugin inputs and outputs, so choose their storage location and retention policy accordingly.

Pass `compression: 'gzip'` to `createFilesystemTraceStore` to compress new trace
files. The default remains plain JSON. Both formats keep the same trace reference:
its SHA-256 covers the original canonical UTF-8 JSON, not the compressed bytes.
The reader reopens either format, bounds decompression and verifies that original
fingerprint before returning the record. Decisions, values and provenance are
unchanged; this is storage encoding, not sampling or a compact snapshot view.

Gzip writes and reads default to a 16 MiB uncompressed content ceiling, including
the trace envelope. `maxTraceBytes` may lower that ceiling. Compressed input is
also bounded (the content ceiling plus 64 KiB of encoding overhead); malformed,
changed, invalid UTF-8, oversized or fingerprint-mismatched gzip is refused as
`storage_corrupt`. An oversized gzip write is refused as `trace_limit` before
publication. These gzip bounds do not change existing plain-file size behavior.

New compressed files use `<id>.json.gz`; old plain files remain `<id>.json`.
Existing traces are never rewritten. Upgrade every reader before opting writers
into gzip: older package versions cannot discover the new file suffix. Changing
the setting does not migrate existing files or reclaim old storage. The same
atomic publication and local filesystem assumptions apply. Compression neither
encrypts evidence nor changes access, retention or data-commit guarantees.

## Scheduled collection and prepared summaries

The host owns a versioned `CollectionSchedule`: a namespace, source, transform,
summary projector, stable start time, interval and bounded collection request.
Keep its start time and revision stable across restarts. Changing any configuration
under the same namespace and schedule revision is a conflict; use a new revision
for a changed plan. Namespaces are acquisition identities, not conversation IDs.
The host must authorize readers of a shared namespace.

```ts
import { createCollectionScheduler, type SummaryProjector } from 'footprintjs-datapipeline';
import { createSqliteSummaryStore } from 'footprintjs-datapipeline/sqlite';

const projector: SummaryProjector = {
  ref: { id: 'sample-mean-summary', version: '1' },
  async project(observation, context) {
    const batch = context.collectionBatch;
    const input = await context.read(batch.observationRefs[0]!);
    if (input?.kind !== 'observation' || !input.collectedAt) throw new Error('Missing capture receipt');
    return {
      key: observation.key,
      subject: observation.subject,
      collectedAt: input.collectedAt,
      payload: observation.payload as { [key: string]: import('footprintjs-datapipeline').JsonValue },
    };
  },
};
const schedule = {
  ref: { id: 'sample-series-poll', version: '1' },
  namespace: 'installation-a',
  sourceRef: source.ref,
  methodRef: mean.ref,
  summaryRef: projector.ref,
  startAt: '2026-10-01T12:00:00.000Z',
  intervalMs: 60_000,
  request: { series: 'sample-series' },
};
const summaries = createSqliteSummaryStore({ filename: '.pipeline-data/summaries.sqlite' });
const scheduler = createCollectionScheduler({ pipeline, store: summaries,
  schedules: [schedule], projectors: [projector] });

// Drive one bounded tick, or await scheduler.start({ signal }) in the host.
await scheduler.runDue();
const result = await summaries.query({ schedule,
  start: '2026-10-01T12:00:00.000Z', end: '2026-10-01T14:00:00.000Z' });
await summaries.close?.();
```

The scheduler calls the existing `collect` and `derive` FootPrint flows for each
current due poll. The projector runs during ingestion and may read its committed
inputs to preserve sufficient statistics and bounded provenance. It must use the
actual capture receipt time for `collectedAt`. Domain adapters own units, metric
meaning, aggregation rules and evidence retrieval. For example, a later mean can
merge counts and sums; an exact median cannot be reconstructed from means.

Construction starts no timer. `runDue({ now?, signal? })` supports a trusted host
clock and controlled execution; `start({ signal, pollIntervalMs? })` drives ticks
until cancellation. Ticks serialize within one scheduler. Past slots are recorded
as `missed`; only the newest due slot within `maxLatenessMs` (default one interval)
can acquire current data. Restart never fills a past slot with a new snapshot.
Empty, failed and missed polls stay distinct. Failed publication does not advance
the durable cursor. Collection operation keys are stable per namespace, schedule
revision and slot, so retries reconcile existing commits.

SQLite publishes poll status and all its summaries in one transaction. Its time
index selects summaries by actual `collectedAt` in `[start, end)`, with no raw
repository reads, adapter calls or transform runs. Poll coverage counts scheduled
slots separately: `additionalReceiptPolls` identifies included receipts whose
scheduled slot fell outside the query. A received sample is not proof of continuous
coverage or a source operation interval. Queries return a bounded, explicit
`truncated` result; callers must not label a truncated merge as a complete window.
Ranges are limited to 31 days and 20,000 expected slots, with at most 50,000
summary records per query (10,000 by default).

The optional `/sqlite` backend uses Node's built-in SQLite, a local database with
WAL and immutable content digests. It is suitable for one acquisition owner per
schedule on one host. It does not elect a distributed scheduler leader or prevent
two processes from simultaneously contacting a source. Supply another
`SummaryStore` implementation and host coordination when that is required.
Stored summary digests protect record consistency; validating original source
bytes remains a separate, explicit evidence operation.

## Development

```sh
npm run typecheck
npm test
npm run build
```

The package exports contracts, flows and scheduling at `footprintjs-datapipeline`,
the raw repository at `/filesystem`, and the independently imported summary index
at `/sqlite`.

## Optional numeric summaries

Transforms can use `summarizeNumericSamples(metric, samples)` while captured
observations are available, then persist that JSON state in their derived payload.
`mergeNumericStatistics(states)` combines disjoint states without reading raw
values. The state retains count, mean, M2, sample variance, explicit missing/invalid
counts, strict `>` counts for configured thresholds, and first/last/min/max anchors
with exact observation references. Sample variance is `null` for fewer than two
valid samples; it is zero for a known constant series.

A metric declares its source revision, metric revision, unit, sample grain,
weighting, clock, dimensions, population and retention policy. All identities and
thresholds must match to merge. Duplicate or overlapping input references are
rejected: count/mean/M2 cannot safely deduplicate them. Support is bounded at 4,096
observations, with at most 32 configured thresholds; narrow windows or partition
work when those bounds are reached. Overflow and unrepresentable variation are
rejected. IEEE754 arithmetic is finite precision; a material-loss guard can refuse
large offsets or cancellation, and does not promise arbitrary precision or a
final-value error bound. Missing values never become zero. An average of sample averages remains
equally weighted by samples; it is not an operation-weighted average or a median.

`compareNumericWindows({ current, baseline, policy })` compares these stored
sample means. Each window provides its half-open range, numeric state and declared
expected/observed slots. The policy sets minimum valid samples, minimum slot and
valid-sample coverage, and strict absolute/optional relative change thresholds.
The helper refuses confident comparisons across incompatible metrics, unequal
window durations or expected slots, overlapping inputs, inadequate coverage or
missing samples. A zero baseline has no finite relative change. Returned windows,
input references, policy, effects and reasons support replay; an observed change
does not establish a physical cause or statistical significance.

Adapters own the meaning of coverage and the sampling/retention policy. The
helpers cannot establish that a changing top-N population represents an entire
system. Older summaries without these states remain readable but cannot acquire
variance or exact threshold counts retroactively. Use a new transform/projector
revision and collection schedule when adopting the new state. Collection,
derivation, storage and schedule contracts are unchanged.

## Optional InfluxDB 2 output

The `/influxdb` subpath provides a bounded line-protocol encoder and an explicit
HTTP sink. Fields declare float, signed integer, boolean or string types;
integer fields and nanosecond timestamps use decimal strings. The host owns
measurement meaning, timestamp choice, destination and credential resolution.

```ts
import { createInfluxDbSink, type InfluxDbSinkOptions } from 'footprintjs-datapipeline/influxdb';

async function publish(options: InfluxDbSinkOptions) {
  const sink = createInfluxDbSink(options);
  return sink.write([{
    measurement: 'sample_metric', tags: [['site', 'example']],
    fields: [['value', { type: 'float', value: 5000 }],
      ['samples', { type: 'integer', value: '4' }]],
    timestampNs: '1792411200125000000',
  }]);
}
```

Construction starts nothing. Call the sink from an ordinary named Footprint
step and retain its delivery receipt in the run. All points are validated and
encoded before credentials or HTTP; requests preserve order and stop on the
first uncertain batch. Receipts count acknowledged, uncertain and unattempted
submissions separately. An HTTP error can mean partial storage, never proof of
zero writes. No automatic retry or cross-store transaction is implied.

Point-local preflight failures include a value-free `rejectedPointIndex`; global
limits do not blame a point. The host decides whether to record a coverage gap
and submit a new list. Replaying an unknown write requires an unchanged payload,
timestamp, destination and order, with protection against overwriting later
corrections. The sink never drops points or retries automatically.

The sink is independent of the repository and adds no scheduler, source client,
vendor mapping or change to existing collect/derive flows. See the
[sink contract and verification](docs/INFLUXDB_SINK.md) for limits, the supported
lossless encoding profile, timeout behavior and disposable-database tests.

## Native collection-window evidence (opt in)

`runWindowFlow({ operationId, chart, input?, traceStore?, signal?, timeoutMs?,
maxRecordBytes? })` runs an ordinary Footprint chart with full read capture,
`writeProvenance: 'reads-prefix'`, and recorded native decision/control evidence.
It returns `{ record, trace }`: the engine's own snapshot and the existing
independent trace-storage outcome. It introduces no graph schema, walker or rule
language. Use the public `foottrace` readers and `foottrace/cause` to inspect it.
Trace persistence failing does not turn a completed computation into a data
commit; this runner has no repository publication authority.

The runner defaults to a 30-second budget shared by asynchronous execution and
trace acknowledgement, with an 8 MiB retained-record ceiling and 1 MiB input
ceiling. The trace envelope's `recordedAt` is the runner-start clock; source
validity and learned times belong in explicit chart values. Input/output limits
do not preempt trusted synchronous chart code or encoding. Custom
`prepareWindow`/`project` callbacks must honor the signal and bound their own
work; the scheduler does not independently time out these trusted callbacks.

`WindowExecutionError` retains the existing fixed `code` and message, with a
frozen `context`: `phase`, Footprint's shared `AttemptCategory` in `category`,
and `runtimeStageId` only when the native terminal failure or checkpoint names the execution.
Phases distinguish validation, initialization, execution, serialization, paused
and aborted work. A runner deadline has category `timeout`; caller cancellation
and failures without a native classification retain `unknown`. Window-wide
aborts omit a stage identifier rather than blaming an earlier branch failure. Stage categories
and ownership come from Footprint's public `getFailureContext()`, without
inspecting retry records or reconstructing errors from observer events.
Recovered retries, observed inner calls and contained branch errors cannot
supply another stage's cause. A contained error does not fail an otherwise
completed window.
Raw exceptions, messages, stacks, validation issues and arbitrary error codes
are not copied into this context. Initialization or serialization failures do
not blame the last successful stage. The host still owns its chart's logging
and must use non-secret stage identifiers.

The returned native record is detached and deeply frozen. For `runWindowFlow`,
the trace store receives this same immutable value, parsed once; a store must
not mutate it. A mutation attempt that throws makes trace persistence
`unavailable` while the completed record remains intact. This does not change
the existing collect/derive trace-store contract.

From 0.6.0, the window uses Footprint's built-in control recorder, whose snapshot
bundle is named `control-dependencies`. Records saved by 0.4.x and 0.5.x use
`window-control-dependencies`. Applications that read saved control bundles must
recognize both profiles before upgrading, and reject an ambiguous selection
rather than combine duplicate bundles. Existing stored records are not rewritten.

A `SummaryProjector` may implement `prepareWindow(context)` to run that chart
once after each committed collect/derive pair, before any summary projection.
The context contains the immutable committed collection and derivation batches
and observations, slot, schedule, read port and cancellation signal. Its result
is a JSON object of at most 64 KiB, supplied as immutable `project()`
`context.window`. Persist only compact trace references and selectors in each
summary. Bump the projector/schedule revision when adopting this hook; the
library never synthesizes native evidence for older history. Hook failure
publishes a failed poll without summaries; a host can explicitly retain a trace
storage `unavailable` outcome in metadata while keeping supported statistics.

Author independent top-level state keys per resource and metric, with focused
steps that read exactly the keys used by each conclusion. Footprint tracks root
keys: putting all resources under one object cannot provide resource-level
attribution. Write compact receipt references, actual validity and learned times,
metric states and shared `RequirementResult` records; leave source bodies in the
authorized evidence repository. Use native `decide()` for rules. An application
must explicitly project its recorded attention/requirements into the public
record reader's attention map: a state write is not automatically an anomaly.

These records explain evidence derivation, not physical causation. A snapshot
of a topology link does not prove it was valid throughout a window. Missing
mappings block only the claims that require them. This release does not join
independent windows into a historical topology or window tree; unresolved
cross-window links remain unknown. Original collect/derive and unconfigured
projectors keep their existing behavior.
