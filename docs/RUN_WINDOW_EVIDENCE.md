# Saved run inputs and window evidence (opt-in)

This first slice adds one generic, explicit transform. It starts no collection,
clock, service, database query, deletion, retry or archive. Existing pipelines
are unchanged. PowerScale units, selection and acquisition stay in the adapter;
this library contains no PowerScale, vCenter or Neo dependency.

## What the host saves per poll

Use an existing `SourceAdapter.collect()` and repository. A run is one committed
batch containing one receipt observation with `RUN_RECEIPT_SCHEMA` and zero or
more per-series contribution observations with `RUN_CONTRIBUTION_SCHEMA`.
These are ordinary observations, not a new store or graph format.

Receipt payload (public metadata only):

```ts
{
  slotAt: '2026-10-20T12:00:00.000Z',
  status: 'complete', // complete | partial | failed | empty
  rawDisposition: 'not-retained', // retained | not-retained | expired
  retentionPolicyRef: { id: 'installation-policy', version: '1' },
}
```

A contribution has the exact declared metric dimensions as its `subject`, actual
capture time as `collectedAt`, and this payload:

```ts
{
  receiptKey: 'receipt', // logical receipt key in this same committed batch
  metric,               // existing NumericMetricDefinition; exact compatible identity
  status: 'valid',      // valid | missing | invalid
  value: 20,            // finite, including zero; absent for missing/invalid
}
```

Both receipt and contribution use their actual capture `collectedAt`, inside
the scheduled slot. Preserve unknown source operation time as
`time: { kind: 'source-time-unknown' }`; poll times are not operation timestamps.
A failed/empty run cannot carry a numeric contribution. A partial run may carry
valid numbers, but cannot satisfy its completeness requirement. A thrown source
error that leaves no committed receipt appears as `not-collected`; this feature
cannot infer its failure reason. Adapters must save explicit failure receipts
when that distinction is available.

Core `collect()` assigns references and binds every observation to the source
and batch. Core `derive()` loads committed inputs and applies its existing
half-open collection-window filter. Do not construct source receipts in response
to an investigation or use a current snapshot to fill an old slot.

## Enable and derive

```ts
const windowTransform = createRunWindowEvidenceTransform({
  enabled: true,       // omitted or false returns undefined: do not register
  metric,             // one declared series; reuse NumericMetricDefinition
  anchorAt: '2026-10-20T00:00:00.000Z',
  intervalMs: 5_000,
  traceStore,          // existing TraceStore; optional, independently acknowledged
});
// Register this transform in createPipeline({ transforms: [windowTransform!] }).
const result = await pipeline.derive({
  methodRef: windowTransform!.ref,
  idempotencyKey: 'series-a-window-1200-cutoff-1205',
  inputBatchRefs: committedRunBatchRefs,
  window: {
    basis: 'collection',
    start: '2026-10-20T12:00:00.000Z',
    end: '2026-10-20T12:05:00.000Z',
  },
  request: { knowledgeCutoff: '2026-10-20T12:05:00.000Z' },
});
```

The exact metric, anchor and cadence determine the transform's versioned identity.
Schedule-aligned windows contain at most 64 slots, at most 64 input batches and
8,192 scanned observations. Partition series/windows in the host. The existing
8 MiB native record limit still applies; oversized work is refused, not cut.
Saved source observations survive refused derivation. Existing pipeline limits
also apply. No ceiling is increased by this feature.

## What the result supports

- Numeric statistics reuse `summarizeNumericSamples`, with exact saved
  contribution references. Means are equal-weight means of the declared sample
  grain, never silently operation-weighted means or request percentiles.
- Receipt coverage lists expected, received, missing, failed, partial and empty
  polls. A missing poll contributes no numeric value and receives its own
  unresolved `RequirementResult`. Even 60/60 receipts do not establish continuous
  source-operation coverage: `continuous` stays `unknown`.
- A receipt without this series' contribution is not a zero. It remains a gap
  for this series. Missing/invalid contribution values use the numeric helper's
  existing explicit counts.
- Completeness requirements affect `${windowRef}:completeness` only; IDs include
  the series, window and cutoff identity. The payload exposes `windowRef`. Applications
  still must apply their existing claim gate when asserting completeness; this
  transform grants no authority and changes no agent output guard.
- Ordinary Footprint steps record the schedule and cutoff, import one receipt and contribution per slot, record
  a named `decide()` completeness rule, and derive numbers separately from gap
  counts. Per-write read provenance is enabled by `runWindowFlow`. Its stored
  native trace can be opened by existing foottrace cause tools. References point
  to saved inputs; they do not create automatic cross-run graph edges or prove
  physical cause. No new walker is introduced.
- The native trace acknowledgement is in derivation evidence
  `details.nativeTrace`. Trace storage failure does not erase a committed
  summary. Without a trace store, structured input support remains available,
  but later native cause replay is unavailable.

The knowledge cutoff excludes batches durably finished after it. Reassessing
identical supplied inputs at the same cutoff produces the same observations;
late data requires a new operation key/cutoff and creates a new assessment.
Earlier summaries are never rewritten. This is a knowledge bound over the
supplied batches, not a history discovery index or a guarantee that an operator
supplied every known run. Authorization, trustworthy clocks and namespace
selection belong to the host.

## Storage and deployment boundary

`rawDisposition` is the producer's declared retention fact, not a verified
archive read. A fingerprint cannot recreate an expired/not-retained response.
This feature retains compact inputs through existing collect/storage APIs and
creates a native trace only for an explicitly requested window. It does **not**
select representative/exception runs, compress automatically, set retention
periods, delete anything, implement disk quotas or change today's per-poll
writer traces. Those remain separate reviewed changes and operator decisions.

No Python collector, Neo original flow, mapping flag, device connection,
production service, dependency version or paid model call changes here. This
library PR must be released and explicitly adopted by the PowerScale adapter
before Neo can use it. Default cause-reader traversal limits may require a
narrower window or explicit host limits; a saved window fitting the record cap
is not a promise of a complete explanation at every query limit.

## Runnable offline proof

`node --import tsx examples/run-window.ts --output /absolute/new-empty-folder`
saves 59 synthetic run receipts for 60 scheduled slots, including one failure
and one partial capture. It reopens the saved native trace with foottrace cause
tools, verifies statistic → contribution → receipt links, and writes `report.json`.
There are 58 valid numeric inputs and three explicit gaps. No device, database
server, model call or Python process runs. The fixture omits raw archives deliberately.

## Timing and capacity measurement

See [the timing review](RUN_WINDOW_TIMING.md) for separate fixture-ingestion,
repository-read and native-flow measurements at 64 slots / 8,192 observations,
and the bounded bulk-read optimization.

## Full-trace retention decision

The approved host policy keeps every recorded run trace and every window trace
with lossless gzip, without sampling or automatic deletion. The library default
remains unchanged. See [the storage plan](FULL_TRACE_STORAGE_PLAN.md) for
configuration, reader rollout, capacity assumptions and raw-archive boundaries.
The offline example now saves and reopens all 59 run traces, plus its two window
execution traces; summaries do not replace them.
