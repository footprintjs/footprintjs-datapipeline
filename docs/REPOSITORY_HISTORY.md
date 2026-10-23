# Finite repository history: seven-day laptop example

This optional new-namespace profile bounds exact ownership history without
letting forgotten IDs be published again. It reuses the retained repository,
its SQLite control ledger, verified-uptime expiry and native gzip operations.
It adds no scheduler, fleet journal, sealed store, database service or timer.
Existing namespaces, IDs and the default repository are unchanged.

The library requires a finite host choice. The laptop example explicitly uses
`keepDays: 7`; there is no implicit history duration. This PR neither enables an
application nor approves a production retention policy.

## Explicit policy and scope

```ts
import {
  createRetainedFilesystemRepository, REPOSITORY_HISTORY_VERSION,
} from 'footprintjs-datapipeline/filesystem';

if (REPOSITORY_HISTORY_VERSION !== 1) throw Error('Unsupported history reader');
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
new work, the host explicitly opens a handle for the current period.

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
unconfirmed work. Wall-clock jumps, source time, file time, time zones and daylight
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

Under the existing SQLite writer transaction, maintenance inspects the complete
next eligible period. Every member must be expired, publication-confirmed and
physically deletion-confirmed. Uncertain publication or incomplete cleanup blocks
retirement and emits `REPOSITORY_HISTORY_BLOCKED`. It never converts uncertainty
to success to meet a deadline. The hard caps still stop admission if an unresolved
period eventually fills the allowance.

The cumulative checkpoint, retired-prefix fence and first bounded row-reclamation
batch commit atomically. A crash leaves either the old exact inventory or the new
fence. Remaining garbage is selected from rows below that fence after restart.
At most the requested 1–100 operations and their memberships are reclaimed per
pass. Full-period verification is synchronous SQL work bounded by the namespace's
required operation/reference caps; large production caps need their own latency
measurement. There is no asynchronous catalogue publication or second maintenance
lease/journal to disagree with the ledger.

Reclaimed SQLite pages are reused. Admission counts actual rows in this format,
never maximum row IDs. A single cumulative checkpoint replaces an ever-growing
receipt list. Its hash and counters describe the retired prefix, not a recoverable
copy of the old per-record receipts.

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

This format uses `retention-v3.sqlite` and a version-3 legacy-reader fence. Old managed
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
node --import tsx examples/measure-laptop-history.ts 256 32 32
```

Both use actual local files, eight synthetic operations per simulated day for 32
days, seven-day payload/history retention, 80-operation caps and a 32 MiB metadata
allowance. Time is accelerated; no device, model, network or InfluxDB is used.
Allocated bytes include files, directories, SQLite and native operation files.
Measurements on Node 24.21.0, local APFS, 2026-10-23:

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

The 256-row program is also rerun by `npm run check`; its test checks all 256
publications, the exact kept/retired membership counts and actual late-cycle
allocated-byte range. Its longer test allowance covers the aggregate historical
fixture and does not increase any repository wait or collection deadline.
