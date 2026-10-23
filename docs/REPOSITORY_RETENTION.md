# Opt-in retained filesystem repository

## Optional verified-uptime expiry for new namespaces

Set `expiryClock: 'verified-uptime'` to use elapsed time for expiry instead of
wall time. The default remains the existing wall-clock format. No existing
namespace is adopted or converted. Reader-first rollout is required.

This profile saves one elapsed-age high-water mark in the existing repository
ledger. Each handle adds only its own observed monotonic duration to the age
at opening; transactions take the maximum of that value and the durable value.
Two handles do not sum the same time interval. Only committed checkpoints count
after restart. Unobserved downtime is **not credited**: retention may be longer
than the configured period, never shorter because the wall clock jumped.
This is not a promise to delete at a particular civil time after downtime.

`clock` still records the host's observed wall acceptance timestamp.
`acceptedAt` and `expiresAt` retain that nominal wall-time pair; they do not
decide deletion in this profile. Its receipts additionally name `expiryClock`,
`acceptedAgeMs` and `expiresAgeMs`. Source timestamps, file mtime, time zones
and DST do not decide age. A late wall correction changes no saved input/ref.

The elapsed source defaults to Node's `performance.now()`. `monotonicClock` is
a trusted host/test port, never model input; a wrapper over a wall clock is not
a valid implementation. Backward, nonfinite and overflowing elapsed readings
refuse access and emit `REPOSITORY_CLOCK_UNAVAILABLE`, with fixed text only.
No deletion or automatic clock reset follows such a refusal.

New namespaces use `retention-v2.sqlite` with a separate legacy-format fence at
`retention.sqlite`. Old constructors reject that fence without opening the real
elapsed-age ledger. An already opened legacy namespace cannot be converted.
Losing a fence is unconfirmed storage, not permission to initialize over old IDs.
The profile uses SQLite DELETE journals and EXTRA synchronization; the old WAL
profile is unchanged. This is no new attempt journal, history-retirement policy
or source-ownership lock. Restoring a stale backup cannot prove the latest age;
restore/admission fencing and retained-history limits need their own policy.

The clock/restart tests use actual repository reads, cleanup and the exact
0.9.0 constructor witness. They assert that saved values, references and native
publication fingerprints remain identical before expiry. Neither this profile
nor those synthetic tests establish fleet capacity or production activation.

Runtime references: [Node performance clock](https://nodejs.org/api/perf_hooks.html#performancenow),
[SQLite journal and synchronization rules](https://www.sqlite.org/pragma.html).

This is a managed **new namespace**, not adoption of existing data. The default
filesystem repository and existing packed records are unchanged. Nothing starts
a timer, sets a retention policy for an app, or runs cleanup automatically.

```ts
import { createRetainedFilesystemRepository } from 'footprintjs-datapipeline/filesystem';
const repository = createRetainedFilesystemRepository({
  enabled: configuration.retentionEnabled, // omitted/false: undefined, no I/O
  directory: configuration.newEvidenceNamespace,
  retentionMs: configuration.repositoryRetentionMs, // required; no default
  onHealth: event => independentHealthChannel.report(event),
});
// Host-owned, bounded maintenance, separate from collection:
if (repository) await repository.sweep({ limit: 100 });
// After all pending reads/writes/maintenance have finished:
repository?.close();
```

A private, canonical path on a separate filesystem from root is required unless
the host explicitly sets `requireSeparateFilesystem: false` for a test or approved
exception. Refuse an unmanaged nonempty namespace; never guess ages from file
modification times. A namespace binds one retention period; changing it requires
a separately reviewed migration, not an unnoticed configuration change.

## Evidence and retry semantics

Records are the same native packed operations introduced in 0.8.0. Content bytes,
references, immutable checks and original publication receipts are unchanged.
A version-1 SQLite ledger records hashed reference ownership, operation/request/write
fingerprints, accepted/expiry times and pending/stored/expired/deleted state. Publication confirmation is separate:
expiry never converts an unconfirmed attempt into a committed one. It
contains no captured responses or metric values. Hashes are consistency keys,
not source authentication or permission grants.

The pending intent commits before native publication. Interrupted publication
keeps that identity; reopening reconciles the native committed operation, with
no acquisition or HTTP delivery. A pending retry cannot silently change its
frozen candidate. Expired operation IDs and references cannot be resurrected,
even under a different operation ID. New callers must use new identities.
Records in this managed namespace have one publishing operation owner. Foreign
reference reuse is refused; input links to prior records remain ordinary links.

Reads and `findOperation` refuse expired records with `REPOSITORY_EXPIRED`.
`receipt(operationId)` still explains the expiry. Unknown references remain
undefined. Missing/corrupt data for a known stored reference is unconfirmed,
never an absence. Expiry is based on acceptance time, not device event time.
Once observed expired, a backward clock cannot make a record available again.
There is no pinning of ancestors: a live summary can outlive its inputs and must
report expired inputs honestly. The app still owns claim/access checks.

## Cleanup and concurrency

A durable expired tombstone precedes unlink. Each call selects at most 100
operations. Unlink and parent synchronization run **outside** the database
transaction, so slow disk cleanup does not hold the lock needed by polls. Once
those finish, a short transaction confirms `dataDeleted: true`. A failure or crash
between unlink and that acknowledgement keeps deletion unconfirmed; a later
explicit cleanup verifies absence and synchronizes before confirming it. An
expired identity can never publish again, including while its files are removed.
Each handle allows one cleanup pass, rejects close while it runs, and yields
between rows. This does not lengthen the admission wait or a poll's deadline.
It removes only the known packed file and its empty operation
folder, then syncs the parent. It does not recursively traverse folders or
follow symlinked operations. Failed deletion keeps `dataDeleted: false`, emits
a fixed health event, and can be retried. Tombstones and reference membership
remain after data removal, so the original references say expired after restart.
They are not automatically expired: metadata itself still grows.

SQLite full-synchronous transactions serialize metadata, native publication and
reads across cooperating processes. Competing readers, pollers and cleanup wait
asynchronously for up to one second to **start each transaction**, including
first-open schema admission. SQLite's synchronous busy handler stays disabled,
so the wait does not block the event loop that must release the lock. This is
an admission budget, not an end-to-end operation deadline. Final `COMMIT` has a
separate one-second asynchronous busy budget: DELETE-journal readers can prevent
that acknowledgement even after the writer was admitted. The elapsed checkpoint
is written once, and only the pending SQLite `COMMIT` is retried. On exhaustion,
the transaction is rolled back and the fixed `REPOSITORY_BUSY` alert is emitted.
This also applies when committing an expired mark or deletion confirmation.
Per instance at most
32 transactions are queued; queue wait and native I/O are separate costs.

If admission remains busy, or the local queue is full, access fails with
`REPOSITORY_BUSY` and the independent `onHealth` sink receives that fixed code.
No data or receipt is invented when an intent could not be saved. The host must
retain this alert outside the refused repository and account for the unsaved
slot as a gap. Other storage errors are not treated as lock contention. The
library retries **only database admission and a busy final COMMIT**: it never
repeats publication, a transaction body, cleanup side effects or an uncertain
HTTP send. If publication already ran before a final COMMIT was refused, its
files may be durable. The earlier intent remains unconfirmed; reconcile the
saved operation rather than treating BUSY as proof that nothing was stored.
Same-turn scalar reads are grouped into chunks of at most 256 (at most 8,192
queued references), so existing parallel loaders work without one inflate per
point. A refused bulk read is isolated per reference; an expired item cannot
make a fresh item expire. Completed values are never cached. Close only after
draining all work. No host failover, multi-host leader election, or workload
scheduler is introduced.

A durable ownership index selects the exact operation for a reference. Native
read/decoder/hash validation is reused rather than replaced; reads and immutable
checks no longer scan all historical operations. Public plain-store behavior is
unchanged. Packed decompression remains bounded per operation (16 MiB). Returned
bulk data and caller-held results can still be large; this is not an AI viewer.

## Actual inactive writer proof

`docs/measurements/measure-retained-repository.mts` runs the actual Neo client
writer against synthetic saved input, with a simulated acknowledged sink. A
100 ms controlled-clock TTL is solely an expiry test, not a recommendation.
The same 242 records across 12 operations match a plain repository; four native
cause answers match reopened traces. Restart performs no new capture/send.
Expiry removes all operation data while references remain explicitly expired.

| Fresh test namespace | Packed data | SQLite metadata after close | After data expiry |
| --- | ---: | ---: | ---: |
| 50 returned / 50 retained clients | 27,452 B | 49,152 B | 49,152 B metadata |
| 256 returned / 154 retained clients | 71,537 B | 69,632 B | 69,632 B metadata |

Metadata includes fixed namespace startup cost, so **do not multiply these
one-run SQLite sizes into a daily estimate**. A sustained test must measure
marginal growth including WAL, tombstones, trace ledger, raw data and gzip files.
These numbers are not a real cluster measurement or throughput proof.

This adds expiry, not a repository disk quota. The trace guard limits traces
only. Independent headroom monitoring, retention choices, capacity sizing and
sustained host tests remain required before deployment. Old readers cannot
explain ledger expiry and must never write into or replace managed readers in
this namespace. Python, mappings, original application flows and services stay
unchanged. Upgrade readers before enabling any managed storage.
