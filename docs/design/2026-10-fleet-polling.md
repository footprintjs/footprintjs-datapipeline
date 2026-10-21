# Fleet polling and independent window summaries

Status: proposal for review; types below are illustrative, not exported APIs.
This document changes no runtime, package export, schedule, retention or service.
It can be reviewed alongside Neo #48 without changing its code or measurement.

## 1. Problem and existing pieces

A slow source or window derivation must not queue every other source's poll.
Restart must recover saved inputs without asking a device for historical data.
This is a library coordination problem; product adapters retain metric meaning.

| Already available in datapipeline 0.9.0 | Reuse |
| --- | --- |
| `Pipeline.collect`, `reconcileCollect`, `derive`, `read` | Acquisition, lookup-only recovery and computation |
| `Repository`, optional `readMany` | Bounded committed-record loading |
| `CollectionSchedule.startAt`, versioned schedule identity | Source anchors and immutable configuration |
| `createRunWindowEvidenceTransform` | Saved contributions, statistics, named completeness decisions |
| `RUN_RECEIPT_SCHEMA`, `RUN_CONTRIBUTION_SCHEMA` | Per-run facts; missing values never become zero |
| Packed repository, compressed trace and retained repository | Existing storage, verification and expiry semantics |
| `SummaryStore` | Existing prepared-summary queries, under its current contract |

`src/prepared/scheduler.ts` currently serializes schedules and performs collect,
derive and projection together. Keep that behavior unchanged. Extending its loop
with parallel tasks would not by itself separate those workloads.
`SummaryStore` cannot be an acquisition-only ledger: `collected` requires a
committed derivation and at least one summary. Do not mislabel a poll as `empty`.

Recommendation: a proposed opt-in coordinator over existing pipeline ports,
with a small durable coordination index and an independent summary runner.
Extract shared slot validation only where needed; do not copy the old scheduler.
No new device client, graph format, cause walker or metric format is proposed.

## 2. Immutable offset plan

Options: let hosts supply every offset, or offer a deterministic evenly spaced
plan. Recommend explicit host configuration with an optional pure plan builder.
For interval `I`, ordered source count `N`, source `i` receives
`offsetMs = floor(i * I / N)` and anchor `baseAnchor + offsetMs`.
Require positive safe integer milliseconds and `1 <= N <= I`; offsets are unique.
Also require an explicit source-count bound validated before copying the plan;
a long interval must not permit an arbitrarily large source list.
Recompute each due instant from anchor and slot index, never by adding rounded
intervals. Fourteen sources at 5,000 ms have offsets 0 through 4,642 ms.
Source order, membership, request/policy revisions and offsets enter the plan
fingerprint. Changes require a new revision/namespace; restart cannot reorder it.

```ts
interface FleetSourcePlan {
  readonly sourceRef: VersionedRef;
  readonly scheduleRef: VersionedRef;
  readonly offsetMs: number;
  readonly requestFingerprint: string; // public configuration, no credentials
}
interface FleetPlan {
  readonly ref: VersionedRef;
  readonly namespace: string; // host-authorized acquisition namespace
  readonly baseAnchorAt: string;
  readonly intervalMs: number;
  readonly sources: readonly FleetSourcePlan[];
}
```

Construction validates and snapshots the plan. Omitted/false `enabled` returns
no coordinator and performs no I/O. No interval, source count or lateness default
is chosen here. The same plan produces identical due instants after restart.

## 3. Bounded dispatch and lookup-only recovery

At most one invocation is active per source. A due slot with an active prior
invocation records an overlap gap; it never queues another invocation. A late
slot records a late-start gap. Global active work and recovery per tick are
explicitly bounded; capacity refusal is a recorded dispatch gap, not a zero.
Persist an intent before invoking the poll port; if that save fails, do not call
the source. Publication uncertainty stays
uncertain: no automatic retry that could contact a device. One host owns a plan;
this proposal supplies no distributed lease or multi-host leader election.

```ts
interface SlotIdentity {
  readonly planRef: VersionedRef;
  readonly sourceRef: VersionedRef;
  readonly slotAt: string;
}
interface SavedSlotPort {
  collect(slot: SlotIdentity, signal: AbortSignal): Promise<PipelineResult>;
  reconcile(slot: SlotIdentity, signal: AbortSignal):
    Promise<PipelineResult | undefined>;
}
interface FleetLifecycle {
  runDue(options: { now: string; signal: AbortSignal }): Promise<void>;
  start(options: { signal: AbortSignal }): Promise<void>;
  stop(): Promise<void>; // stop dispatch, cancel cooperatively, drain work
}
```

`reconcile` has the existing `Pipeline.reconcileCollect` semantics: exact saved
request/configuration, no adapter invocation, no new trace. A product port may
compose multiple collects, but must expose lookup of its completed compact
result. An acquisition saved without its compact result is not a completed poll.
Do not guess private operation IDs, reopen internal SQL, or call `collect` to
discover references. A later explicit repair from saved raw input is separate
from recovery and cannot acquire a replacement historical response.

After restart, bounded recovery processes persisted intents and elapsed slots.
Committed results are reconciled; missing, expired, conflicted or unconfirmed
results stay distinct gaps. No historical slot invokes the device. Backlog and
recovery deferral are reported; recovery cannot run an unbounded catch-up loop.
The host must ensure the previous owner stopped before starting another owner.

## 4. Durable cursor, outcomes and input manifests

Options: hold references only in memory, scan repository directories, or keep a
public bounded journal. Recommend the journal. The first loses work on restart;
the second defeats indexed lookup and couples apps to private storage layout.
Add a small generic port, not another repository or store of measurement values.

```ts
type DispatchGap = 'late-start' | 'overlap' | 'capacity'
  | 'not-collected' | 'expired' | 'lookup-conflict';
type SlotJournalEntry = {
  readonly slot: SlotIdentity;
  readonly observedAt: string;
} & (
  | { readonly state: 'intent'; readonly attemptRef: VersionedRef }
  | { readonly state: 'committed'; readonly batchRef: BatchRef;
      readonly evidenceRef: EvidenceRef } // acknowledged, membership checked
  | { readonly state: 'gap'; readonly reasonCode: DispatchGap }
  | { readonly state: 'unconfirmed'; readonly attemptRef: VersionedRef }
);
interface FleetJournal {
  register(plan: FleetPlan): Promise<void>; // changed fingerprint conflicts
  record(entry: SlotJournalEntry): Promise<void>;
  recover(plan: VersionedRef, limit: number):
    Promise<{ entries: readonly SlotJournalEntry[]; hasMore: boolean }>;
  seal(job: SummaryManifest): Promise<ObservationRef>;
  close(): Promise<void>;
}
```

The backend atomically records a terminal outcome and advances a contiguous
per-source cursor. An unrecorded result never advances it. Journal transitions,
manifest persistence and cursor recovery need crash tests before implementation
is accepted. Proposed backend: dedicated versioned tables through the existing
`/sqlite` entry point; preserve existing `SummaryStore` tables and semantics.
Applications use the port, never its SQL. Evidence manifests are ordinary
observations published through existing pipeline/repository APIs; the index
stores their references, cursor and dispatch metadata, not copies of raw rows.

A result recovered later is first known at recovery unless an earlier durable
journal entry proves otherwise. Never backdate that knowledge from source time.
The journal's cutoff selects its supplied knowledge, not all data existing in
the world. Sealing freezes the exact accepted references, gaps, source/metric
revisions and cutoff. Repeating a seal must match or conflict, never overwrite.
Journal loss/corruption is unavailable evidence, not an empty history. Its own
growth/retention policy is an explicit follow-up; no tombstones are deleted here.

## 5. Independent summary worker

Options: calculate inline, pass whole responses to a worker, or send sealed job
references. Recommend references. The worker receives an authorized compact
repository and trace store, not a device adapter, URL, token or raw namespace.
Its bounded job queue is durable through the journal, not an in-memory `Map`.

```ts
interface SummaryManifest {
  readonly jobRef: VersionedRef;
  readonly planRef: VersionedRef;
  readonly sourceRef: VersionedRef;
  readonly methodRef: VersionedRef;
  readonly window: { basis: 'collection'; start: string; end: string };
  readonly knowledgeCutoff: string;
  readonly inputBatchRefs: readonly BatchRef[];
  readonly slotOutcomes: readonly SlotJournalEntry[]; // bounded window only
}
interface SavedSummaryWorker {
  run(manifestRef: ObservationRef, signal: AbortSignal):
    Promise<PipelineResult | { status: 'no-saved-inputs'; manifestRef: ObservationRef }>;
}
```

The worker verifies manifest ownership, fingerprint, plan and batch membership
on every job. It calls existing `Pipeline.derive` with the sealed references and
`request: { knowledgeCutoff }`. Do not add a manifest field to that transform's
closed request or pass the manifest as a fake run batch. A normal Footprint job
flow records manifest selection and the returned derivation/trace references.
Later arrivals require a new manifest, cutoff and operation identity. Retry uses
the same manifest; if inputs expire, it reports expiry instead of substituting
newer data. Worker failure never cancels acquisition or erases prior summaries.
A dispatch-only gap has no acquired run receipt: the existing transform sees
`not-collected`. Its precise dispatch reason stays in the linked manifest and
coordination record, never in a fabricated device receipt.

Windows align to each source's anchor, including its offset. A five-minute
window has sixty 5-second slots; offset sources therefore finish at different
wall-clock instants. Do not force their inputs into an unaligned common range.
Retain current caps: 64 slots/batches, 8,192 scanned observations and 8 MiB native
records; partition work and refuse overflow explicitly. No limit is increased.
The current core requires at least one input batch for `derive`. A wholly absent
window returns the durable all-gap manifest and `no-saved-inputs`, without
inventing a receipt, statistic or native run-window result. A library extension
for native zero-input gap windows would need its own tests and review.

## 6. Evidence and application boundary

Existing window Footprint steps still explain statistics from saved contributions
and receipts. The coordination flow explains why a slot ran, was deferred or
became a gap. Domain failures retain their adapter codes; dispatcher codes must
not introduce competing `RequirementReason` names. Missing input blocks only
claims requiring it through existing shared `RequirementResult` and claim checks.
Reading a manifest grants no claim authority. Source text remains untrusted.
References across runs are explicit links, not automatic connected cause graphs;
neither a scheduling decision nor a latency correlation proves physical cause.
Keep coordination metadata compact; do not reintroduce per-point steps or copies
of raw responses and per-poll native traces. Measure any new coordination work.

Neo wiring is an example only: its existing lightweight PowerScale poller would
implement `SavedSlotPort`; #48 supplies optional retained host stores; its declared
watched-client metrics feed the existing window transform. Python, mapping,
original worker/agent flows and the full compatibility writers remain unchanged.

## 7. Proof, stages and decisions

First implement contract/plan validation and lookup-only recovery tests with
controlled clocks and fake ports. Next add journal crash/restart tests, then the
independent worker. Each is a separately reviewed opt-in slice; no service starts.
Required tests cover integer offsets, stable restart/order, caller mutation,
clock jumps, no overlap/queue, intent crash before/after commit, expired inputs,
unconfirmed publication, bounded backlog, changed plans and failed journal writes.
Worker tests prove no acquisition capability, fixed input/cutoff replay, all-gap
windows, partial truth, authorization and unchanged reopened native cause results.

Activation still waits for #48's storage measurement and reviewed host policies.
The separate local capacity PR must reproduce the old failure, then run the new
path at the handoff's one-hour/24-hour rates with cleanup and summaries active.
Report latency percentiles, explained slots, RSS slope/peak, event-loop lag, CPU,
summary delay and disk growth. Preserve safeguards; do not claim fleet readiness.

Open decisions: exact journal backend/transaction protocol; bounded backlog
policy and queue limits; host cutoff/grace policy; ownership coordination; journal
metadata lifecycle. Real retention, free-space floor, host resources, watched
series and deployed top-N remain owner parameters, not library defaults.
