# Fleet polling and independent window summaries

Status: revised proposal for review; types are illustrative, not exported APIs.
This changes no runtime, package export, schedule, retention or service.
It can be reviewed alongside Neo #48 without changing its code or measurement.

## 1. Existing pieces and the shared owner

A slow source or window derivation must not queue every other source's poll.
Restart must recover saved inputs without asking a device for historical data.

| Already available in datapipeline 0.9.0 | Reuse |
| --- | --- |
| `Pipeline.collect`, `reconcileCollect`, `derive`, `read` | Acquisition, lookup-only recovery and computation |
| `Repository`, optional `readMany` | Bounded committed-record loading |
| `CollectionSchedule.startAt`, versioned schedule identity | Source anchors and immutable configuration |
| `createRunWindowEvidenceTransform` | Saved contributions, statistics and named completeness decisions |
| `RUN_RECEIPT_SCHEMA`, `RUN_CONTRIBUTION_SCHEMA` | Per-run facts; missing values never become zero |
| Packed, compressed and retained stores | Existing publication, verification and expiry semantics |
| `SummaryStore` | Prepared-summary queries under its current contract |

`src/prepared/scheduler.ts` serializes schedules and performs collect, derive
and projection together. Keep it unchanged. `SummaryStore` cannot serve as an
acquisition-only ledger: `collected` requires a derivation and summaries.

First adopt the shared [attempt/intent ownership decision](2026-10-operation-ownership.md):
**the repository owns the facts**. Neo's writer journal already wraps that port.
Remove this draft's proposed fleet journal, durable cursor and SQLite outcome
tables. Deterministic, bounded slot lookup meets recovery needs without them.
The exclusive owner guard is coordination, never another attempt history.

The shared ownership decision now makes bounded store admission and real
cleanup/poll overlap proof prerequisites. A lifetime source-owner lock prevents
duplicate dispatch; it does not fix retention-store lock contention. #25 is a
proposed library fix, not an already released capability. Keep this a draft and
settle both contracts before building a coordinator.

## 2. Fixed source offsets and identities

Options: recompute even spacing from fleet size, hash source identity, or assign
stable offsets. Recommend explicit host-assigned offsets, with no automatic
rebalancing. Each integer offset is in `[0, intervalMs)` and unique in its fleet.
Validate a positive safe integer interval and explicit source-count bound before
copying the plan. Refuse collisions; never move existing sources to resolve one.
Adding C at 2,500 ms leaves A at 0 and B at 1,250 ms, regardless of list ordering.

Compute each due instant from the fixed source anchor and slot index, not by
adding rounded intervals. Membership changes revise fleet audit metadata, not
existing source anchors or operation identities. Changing one source's offset
or request/policy requires its own schedule revision and explicit cutover; other
sources retain their slots. Keep old source plans available for history lookup.

```ts
interface FleetSourcePlan {
  readonly acquisitionNamespace: string;
  readonly sourceOwnerKey: string; // stable physical-source ownership scope
  readonly sourceRef: VersionedRef;
  readonly scheduleRef: VersionedRef;
  readonly offsetMs: number;
  readonly requestFingerprint: string; // public configuration, no credentials
}
interface FleetPlan {
  readonly ref: VersionedRef; // membership audit, not a source's slot identity
  readonly authorityRef: VersionedRef;
  readonly baseAnchorAt: string;
  readonly intervalMs: number;
  readonly sources: readonly FleetSourcePlan[];
}
interface SlotIdentity {
  readonly acquisitionNamespace: string;
  readonly sourceRef: VersionedRef;
  readonly scheduleRef: VersionedRef;
  readonly slotAt: string;
}
```

Snapshot and validate the plan. Omitted/false `enabled` returns no coordinator
and performs no I/O. No interval, source count, lateness or lookback default is
chosen. Restart and adding/removing an unrelated source preserve existing IDs.

## 3. Dispatch without a queue; recovery without a journal

At most one invocation is active per source. An overlapping or late slot never
queues another device request. The global active-work limit also refuses excess
work immediately. Publish those refusals through an ordinary failed-poll path
with no device capability, following the existing late-start pattern.
A receipt describes the scheduled slot; its linked structured evidence records
the host decision and actual detection time, never a fictitious device read.

Use an adapter-supplied failure-policy binding. PowerScale imports #46's single
`FAILURE_CATEGORIES`; new overlap/capacity codes must be added there once.
There is no fleet reason enum, duplicated string list or message matching.
If saving a dispatch failure is unconfirmed, the slot remains unconfirmed or
not collected and the independent health sink is notified; do not retry a source.

```ts
// Proposed product port. FailureCategory comes from its one vocabulary owner.
interface SavedSlotPort<FailureCategory extends string> {
  collect(slot: SlotIdentity, signal: AbortSignal): Promise<PipelineResult>;
  reconcile(slot: SlotIdentity, signal: AbortSignal):
    Promise<PipelineResult | undefined>;
  recordFailure(slot: SlotIdentity, reason: FailureCategory,
    signal: AbortSignal): Promise<PipelineResult>;
}
interface FleetLifecycle {
  runDue(options: { now: string; signal: AbortSignal }): Promise<void>;
  start(options: { signal: AbortSignal }): Promise<void>;
  stop(): Promise<void>; // stop dispatch, cancel cooperatively, drain
}
```

The product port gives live polls and dispatch failures the same canonical slot
receipt identity. It reconciles before publication and never replaces an existing
receipt. Conflicting content is refused, not a second answer for that slot.
This unification needs tests; it is not claimed as an existing poller API.

`reconcile` uses the exact saved request/configuration and the current
`Pipeline.reconcileCollect` semantics: no adapter invocation or new trace.
A product composing raw and compact collects exposes lookup of its complete
compact result. Raw saved without compact is not a completed poll. Do not copy
private operation hashes, inspect internal SQL, scan directories or call
`collect` merely to discover refs. Explicit later repair can project saved raw
input; it must never acquire a historical replacement.

Recovery enumerates elapsed slots from the stable source plan and an explicitly
bounded requested window. It reconciles those slots and reports missing,
expired, conflicted and unconfirmed evidence distinctly. Bounded lookback/work
limits prevent an unbounded catch-up loop. Work outside the requested range is
deferred, never claimed complete. No durable cursor can advance ahead of facts.

Do not add a separate per-poll intent just to distinguish a crash before its
first saved fact. Whether the device was contacted is then unknown; the slot
stays not collected. A historical gap saved during recovery is learned now,
not backdated. Only an observed, saved dispatch decision can justify its reason.
Influx's stronger save-before-send intent remains an ordinary repository event.

## 4. Actual exclusive ownership and ordinary manifests

Acquire the shared decision's mandatory owner guard before `start` or manual
`runDue` can dispatch. Scope includes the canonical acquisition authority and
stable physical-source owner keys, independent of plan revision. Overlapping
plans must contend on common source keys. Recheck ownership before dispatch;
on lost/unknown ownership stop new work, cancel and drain before release.
A contender that fails to acquire ownership emits health only; it must not save
a failed poll under the legitimate owner's slot identity.

The retained repository's short transaction lock is insufficient. The first
local binding must hold a separate lifetime SQLite owner lock without blocking
retention writes or cleanup. Prove exclusion with competing real processes.
Multiple hosts require one shared exclusive authority over the same keys;
independent local lock files do not qualify. Unsupported multi-host startup is
refused. No TTL-based automatic takeover is proposed: a crashed owner's shared
grant is released only after fencing it, since devices cannot enforce tokens.
The shared-authority binding and two-host failure tests gate multi-host rollout.

A summary manifest is an ordinary immutable observation saved through the
existing host-event adapter/pipeline/repository port. It fixes selected batch
and receipt refs, saved gap refs, source/method revisions and cutoff, not copies
of raw rows or a mutable slot-status table. Validate explicit count/byte bounds.
Its deterministic request is lookupable with `reconcileCollect`; a missing job
can be assessed only from saved evidence. Do not recreate an expired manifest
under its previous ID or substitute fresh captures for expired inputs.

Trustworthy recorded publication/knowledge times control selection, never
source timestamps as a substitute. Unknown knowledge time stays a gap. A newly
saved host fact is learned now, even if about an old slot. Repeating an existing
manifest uses identical membership/cutoff or conflicts. Later information gets
a new assessment, never an update to an old manifest or summary. Ownership and
expiry metadata still grow under existing rules; no compaction is added here.

## 5. Independent summary worker

Send sealed references rather than responses. The worker receives an authorized
compact repository and trace store, with no device adapter, URL, token or raw
namespace. Bounded requested windows/cutoffs identify jobs; saved manifests and
idempotent derivations are the durable work. There is no second queue database.
Restart enumerates only an explicit bounded range and reconciles its manifests.

```ts
interface SummaryManifest {
  readonly jobRef: VersionedRef;
  readonly planRef: VersionedRef;
  readonly sourceRef: VersionedRef;
  readonly methodRef: VersionedRef;
  readonly window: { basis: 'collection'; start: string; end: string };
  readonly knowledgeCutoff: string;
  readonly inputBatchRefs: readonly BatchRef[];
  readonly slotEvidenceRefs: readonly ObservationRef[];
}
interface SavedSummaryWorker {
  run(manifestRef: ObservationRef, signal: AbortSignal):
    Promise<PipelineResult | { status: 'no-saved-inputs'; manifestRef: ObservationRef }>;
}
```

Verify ownership, fingerprint, plan and membership on every job. Call existing
`Pipeline.derive` with the sealed batches and `request: { knowledgeCutoff }`.
Do not add a manifest field to that transform's closed request or pass the
manifest as a fake poll batch. A normal Footprint job flow records selection
and returned derivation/trace refs. Summary retries use the same manifest and
stable derive identity; computation cannot call a device or sink. Expiry is
reported rather than replaced, and worker failure never cancels acquisition.

A saved dispatch failure has a failed-poll receipt and exact host reason in
linked evidence. The existing transform can count it. Without a saved receipt,
the slot remains not collected; no overlap/capacity reason is invented.

Windows align to each source's anchor, including its fixed offset; do not force
all sources into a common unaligned range. Preserve caps: 64 slots/batches,
8,192 scanned observations and 8 MiB native records. Partition or refuse overflow;
never raise a limit here. Core `derive` needs at least one input batch. If there
are saved failed receipts, a zero-valid-sample window can still be derived.
If there are no saved batches at all, return an ordinary all-gap manifest and
`no-saved-inputs`, without inventing a statistic or native run-window record.
A native zero-input extension would need its own review and tests.

## 6. Evidence, tests and staged implementation

Existing Footprint window steps explain numbers from saved contributions and
receipts. Host-event facts explain observed dispatch refusals; unrecorded
attempts remain unknown. Shared `RequirementResult` and claim checks block only
claims requiring missing evidence. Source strings stay untrusted. Reading a
manifest grants no authority, and cross-run links are not a new cause graph.
Do not reintroduce per-point steps, duplicated raw rows or per-poll native traces.

First prove the shared repository event/recovery contract, fixed source identity
and actual owner guard. Then implement bounded dispatch/recovery, followed by
the independent worker, each as a separate opt-in PR with tests written first.
Required tests include add/remove/reorder without moving unrelated sources;
collision/cutover validation; clock jumps; no overlap/queue; bounded recovery;
crashes before/after publication; expired/unconfirmed inputs; failed gap saves;
and unchanged outcomes after deleting any non-authoritative cache.

Owner tests must include competing real processes, overlapping plans/revisions,
crash release locally, unsupported multi-host binding, unknown/lost ownership
and release only after drain. Worker tests prove no acquisition capability,
fixed manifest/cutoff replay, all-gap windows, scoped claim blocking, access
checks and identical reopened native cause answers. Import shared failure codes
in tests rather than maintaining another list of expected spellings.

Neo's lightweight poller/#48 stores are examples only. Python, original worker,
agent flow, compatibility writers and mapping remain unchanged. Activation waits
for #48's measured storage and reviewed host policies, including bounded
bookkeeping, plus the separate cleanup-contention and sustained capacity phase.
That phase must reproduce the old failure, then measure realistic one-hour and
24-hour runs: explained slots, deadline percentiles, RSS trend/peak, CPU, event
loop lag, summary delay and disk growth. No fleet readiness is claimed here.

Remaining host choices are work/lookback bounds, cutoff/grace policy and the
reviewed ownership-authority binding. Cross-host activation waits for its proven
shared authority. Retention, free-space floor, host resources, watched series
and deployed top-N remain explicit owner settings, not library defaults.
