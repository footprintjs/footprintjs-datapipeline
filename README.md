# footprintjs-datapipeline

A small TypeScript library for collecting observations, deriving new observations, and reading the evidence behind them. Collection and derivation execute as FootPrint flows. Adapters own acquisition, transforms own computation, and a repository owns durable publication.

Version 0.1.0 requires Node.js 22 or later. Build and run this checkout with the commands below.

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
  storage/filesystem.ts    optional local repository and trace store
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

`collect` and `derive` return `{ commit, trace }`. `read` accepts a batch, observation or evidence reference and returns the published record, or `undefined` when it is absent.

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

## Development

```sh
npm run typecheck
npm test
npm run build
```

The package exports the core API at `footprintjs-datapipeline` and the filesystem backend at `footprintjs-datapipeline/filesystem`.
