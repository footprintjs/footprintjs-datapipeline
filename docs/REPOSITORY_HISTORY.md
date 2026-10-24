# Finite repository history: seven-day laptop example

This optional new-namespace profile bounds exact ownership history without
letting forgotten IDs be published again. It reuses the retained repository,
its SQLite control ledger, verified-uptime expiry and native gzip operations.
It adds no scheduler, fleet journal, sealed store or database service. Explicit
maintenance batches yield outside the transaction; no maintenance starts itself.
Existing namespaces, IDs and the default repository are unchanged.

The library requires a finite host choice. The laptop example explicitly uses
`keepDays: 7`; there is no implicit history duration. This PR neither enables an
application nor approves a production retention policy.

## Explicit policy and scope

```ts
import {
  createRetainedFilesystemRepository, REPOSITORY_HISTORY_VERSION,
} from 'footprintjs-datapipeline/filesystem';

if (REPOSITORY_HISTORY_VERSION !== 2) throw Error('Unsupported history reader');
const repository = createRetainedFilesystemRepository({
  enabled: host.enableDevelopmentHistory,
  directory: host.newNamespaceDirectory,
  retentionMs: host.payloadRetentionMs, // required; cannot exceed keepDays
  expiryClock: 'verified-uptime',
  metadataLimits: host.metadataLimits, // bytes, operations and references
  historyRetirement: {
    profile: 'daily-periods-v1',
    keepDays: 7,
    namespaceId: host.persistedNamespaceInstance, // 32 lowercase hex characters
    period: host.explicitOperationPeriod,
    maxOperationBytes: host.maxPackedContentBytes,
  },
  onHealth: host.independentHealthSink,
});
```

Use `createPipeline` with this repository normally. The core's optional
repository identity port assigns operation and record IDs in the bound scope;
adapters do not build IDs, rewrite records or choose a period. The identity port
is absent in ordinary repositories, preserving their previous ID bytes.

`historyStatus()` returns the current period of **verified elapsed age**, the
retired prefix and cumulative checkpoint. Maintenance may keep a handle bound
to period zero: lookups and cleanup do not create new work. Before dispatching
new work, the host binds an immutable scope without reopening storage:

```ts
const { currentPeriod } = await repository.historyStatus();
const today = await repository.bindPeriod(currentPeriod);
const pipelineForNewSlots = createPipeline({ repository: today, /* registries */ });
```

`bindPeriod` accepts only the current verified period and shares the same open
connection/serialization. It never mutates an earlier pipeline's identity port.
A race with a period boundary is a refusal, not an implicit remint.

That new scope is **new work**, not an automatic retry. A retry must keep the
original scope/idempotency key or reconcile its original full operation ID.
Persist that binding with the application's existing scheduled/run facts; do
not create another attempt journal. Unknown crash outcomes remain gaps.
A closed period cannot accept a first publication, including one arriving after
acquisition crossed its boundary. Preflight is not a reservation: drain before
changing scopes, and record a boundary refusal through independent health/run
accounting. No request is moved to today's period to make a retry succeed.

The namespace instance is permanent, not a random value regenerated each start.
Payload retention, history duration, instance and packed-content limit are
immutable namespace policy. Changing them requires a reviewed migration.

## Kept, expired and retired are different

Periods are half-open 24-hour intervals of verified elapsed age, starting at zero.
A period retires only after its closing boundary plus the entire kept-history
interval. With seven days, a member keeps exact history for **at least seven and
up to eight verified days**, potentially longer for unknown restart downtime or
an incomplete physical cleanup. Uncertain publication alone does not delay
retirement after deletion is confirmed. Wall-clock jumps, source time, file time, time zones and daylight
saving do not accelerate this clock.

Within kept history, original ownership and receipts remain exact. Payload expiry
still reports `REPOSITORY_EXPIRED`, with separate publication/deletion evidence.
Below the retired prefix, reads, receipts, reconciliation and publication all
refuse with `REPOSITORY_HISTORY_UNAVAILABLE`: "Retained history is no longer kept."
This applies equally to a previously saved ID and an invented ID in that old
period. It never claims an individual record existed or was deleted after its
membership has been forgotten. Returning `undefined` there would be dishonest.

IDs encode namespace, period, record kind and operation owner. Each published
output must belong to its operation. An old reference cannot be relabelled as an
output of a current operation. Routing and hashes provide consistency, not source
authentication, access permission, factual truth or claim authority. Normal app
permission checks and untrusted-data handling remain required.

## One authority and bounded maintenance

The host's existing explicit `sweep({ limit })` lifecycle performs payload cleanup
and one history-retirement pass. `retireHistory({ limit })` allows that same
maintenance owner to run only metadata work. Neither starts itself. Poll/read
operations do not initiate retirement.

Maintenance verifies the next closed period in resumable chunks. A persisted
cursor and hash chain remain in the same authoritative ledger. Every member must
be expired and physically deletion-confirmed. Publication may be committed or
unconfirmed: an uncertain save with confirmed-gone files retires as **unconfirmed,
deleted**, never as committed. `retiredUnconfirmedOperations` counts it separately
within the cumulative `retiredOperations` total. Missing deletion proof blocks the
cursor and emits `REPOSITORY_HISTORY_BLOCKED`.

Each pass checks at most `limit` operations and `16 * limit` references, with
`limit` from 1 to 100. Reclamation has the same bounds; verification plus
reclamation share a pass's budget. A 10 ms cooperative work target also stops the
loop; individual SQLite/fsync/OS pauses are not a hard real-time guarantee. Query
plans use `(period, operationHash)` and `(operationHash, refHash)` indexes. There is
no global reference scan, whole-day hash transaction, or table count on admission.
Transactional counters change with the ownership rows and roll back with them.
Control-file checks run after acquiring the writer lock.

`retireHistory` returns verified/reclaimed operation and reference counts, plus
`pending` and `blocked`. `pending` means more bounded work remains, not a failed
retirement. Drain it in the separate maintenance worker, yielding between passes;
the API already waits 5 ms **after releasing** its transaction/serial owner so
other processes can enter their unchanged one-second admission retry window.
Never run a multi-pass drain inside a poll or use one large outer transaction.

A partial verification never advances the floor. Only the final verified member
allows the cumulative checkpoint, retired-prefix fence and first remaining-budget
reclamation to commit atomically. SIGKILL leaves either the previous exact floor
and durable cursor or the new fence. Garbage below that fence is reclaimed in
later bounded passes after restart. Foreign keys preserve reference ownership;
a missing counter, cursor or trigger on reopen is refused, not recreated.

Reclaimed SQLite pages are reused. One cumulative checkpoint and one cursor
replace an ever-growing receipt list. The checkpoint's hash and counters describe
the retired prefix, not a recoverable copy of the old per-record receipts.

Staging also has a bound: one deterministic temporary path per accepted intent.
Only a managed publisher holding the database writer transaction may clear that
intent's abandoned stage; readers never remove a live writer's files. Confirmed
expiry cleanup removes its remaining stage before confirming deletion. Links and
unknown contents are refused. Repeated killed retries cannot create a new orphan
path each time. The normal filesystem backend retains its existing staging scheme.

## Bounds and rollout

Required metadata caps bound operation/reference counts, main-file growth and
rollback-journal allowance. Each native operation is limited by
`maxOperationBytes`; native gzip overhead and filesystem allocation also consume
disk. Pending intents and their stages remain charged to the operation cap.
Choose the operation count and content ceiling together, and retain independent
free-space protection. These bounds do not make arbitrary traffic fit in a small
laptop disk or govern external traces, InfluxDB, unrelated files or other apps.

This revision uses `retention-v4.sqlite` and a version-4 legacy-reader fence.
It does not adopt the earlier unpublished v3 namespaces. The period-scoped ID
spelling is unchanged, while the new format fences older managed binaries. Old managed
constructors refuse it; no legacy directory is adopted, IDs rewritten or age
inferred. An active fence with a missing ledger, or a ledger with a missing fence,
is unavailable and is never silently recreated as empty history. Active authority
tables and their checkpoint/clock/policy rows are checked before initialization
statements; missing facts are not replaced with zeroes. Interrupted first
initialization can resume before any publisher was admitted. Refused pipeline
requests retain their known scoped operation ID, and identity admission remains
inside the caller cancellation/timeout boundary.

Upgrade every reader, publisher and cleanup worker before opting into a fresh
namespace. A plain/unmanaged filesystem client is not an authoritative reader:
it bypasses retention. The host must exclude those clients and old binaries from
this private managed directory. Also update exhaustive health-code handling. Never remove/reset a
namespace fence to recover space. A stale backup cannot prove the latest prefix:
restore requires a trusted latest checkpoint, or a new namespace instance. This
local store is not a tamper-proof archive or a distributed storage service.

## Reproduce the measured history cycles

```sh
npm run check
node --import tsx examples/measure-laptop-history.ts 50 32 32
node --import tsx examples/measure-laptop-history.ts 256 64 32
```

**Small native fixture, not fleet sizing.** Both use actual local files, eight
synthetic operations per day: 32 days for 50 rows and 64 days for 256 rows. Both
use seven-day payload/history retention, 80-operation caps and a 32 MiB metadata
allowance. Time is accelerated; no device, model, network or InfluxDB is used.
Allocated bytes include files, directories, SQLite and native operation files.
The following are historical measurements of the first revision (`cbe88ac`), on
Node 24.21.0, local APFS, 2026-10-23; the revised commands remain automated:

| Rows per operation | Allocated bytes after day 16 | Late range | Final control / operations bytes |
| --- | ---: | ---: | ---: |
| 50 | 2,084,864–2,088,960 | 4,096 | 1,171,456 / 917,504 |
| 256 | 10,113,024–10,162,176 | 49,152 | 5,345,280 / 4,816,896 |

Each completed 256 operations, kept 64 exact entries and retired 192 operations.
No health failures occurred. Maximum maintenance passes measured about 83 ms and
63 ms respectively. These are fixture timings, not a five-second fleet deadline
or memory-stability proof. They do not predict storage for 14 real clusters.

A separate 256-row trial with only 16 MiB of metadata allowance **failed** on day
8, poll 7: 1,272 of 1,277 permitted main-file pages were allocated, and the next
intent needed more space. The guard emitted `REPOSITORY_METADATA_LIMIT` and wrote
no partial intent. This was not counted as a completed run; the larger allowance
was explicit test configuration, not a changed default or bypassed guard.

Tests compare native saved records, references and cause answers after reopening,
keep missing polls separate from numeric support, and refuse old IDs after real
process kills before/after the retirement commit. They also kill repeated staged
retries, verify cap reuse/physical-byte plateaus, guard deletion/publication proof,
freeze policy, refuse old readers and exercise clock corrections.

The 256-row program is also rerun by `npm run check`; its test checks all 512
publications across 64 days, the exact kept/retired membership counts and actual late-cycle
allocated-byte range. Its existing test allowance covers the aggregate historical
fixture and does not increase any repository wait or collection deadline.

## Volume, maintenance rate and capacity

The MB figures above are **eight operations per day**, not 241,920 per day.
At that fleet rate, retained metadata is GB-scale and depends on each store's
reference count. One raw response operation has three references; a compact
receipt with one watched series has four, with more references for more series.
They must be measured separately, not as seven references in each store.

The repeatable metadata-only scale commands are:

```sh
node --import tsx examples/measure-history-volume.ts 241920 10 3
node --import tsx examples/measure-history-volume.ts 241920 1 4
```

The first spans ten full-volume periods; the second isolates one four-reference
period for lock/uncertainty verification, not an eight-period size plateau. They
bulk-seed actual SQLite metadata, run bounded verification/reclamation and
publish real native records from another process during the first retirement.
One in 1,000 seeded operations is unconfirmed; it must not block later days.
Bulk setup uses 10,000-row transactions and its own 8 MiB cache request; dirty
pages may exceed that request. Production connection settings are unchanged.
Files-gone states are seeded; this is **not** a native payload-deletion, device,
network or five-second fleet capacity test. The 12 GiB per-store metadata allowance
is explicit benchmark configuration, not a laptop/production recommendation or
allocated disk. A 2 GiB actual-free-space floor stops setup; temporary files are
removed afterward. Run profiles sequentially on a shared laptop.

The host must drain maintenance faster than the incoming work across payload
cleanup, verification **and** row reclamation. A single 100-operation pass every
35 seconds does not prove that. No scheduler or cadence is installed here. Hard
operation/reference/page caps bound allocation even if maintenance stops, but
admission then refuses with a metadata-limit alert; uninterrupted collection is
not promised. Production approval still requires a configured worker cadence,
headroom, full payload/summary overlap measurements and the actual store sizes.
The measured scale report and exact commands are in
[the review measurement](measurements/repository-history-review-2026-10-23.md).
