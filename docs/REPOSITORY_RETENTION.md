# Opt-in retained filesystem repository

## Optional limits for exact retained metadata

**Caps alone stop admission when exact metadata fills.** With only the limits
below, expired payload cleanup retains per-record ownership and history, so it
does not recover operation/reference capacity. The separate opt-in
[finite-history profile](REPOSITORY_HISTORY.md) can retire eligible periods and
reuse that capacity while preserving a permanent retired-prefix fence. Hosts
must explicitly configure its policy, run maintenance and validate their workload;
neither profile guarantees sustainable capacity or a wall-time deletion deadline.

New `verified-uptime` namespaces may also supply `metadataLimits` with required
`maxBytes`, `maxOperations` and `maxReferences`. There are no default limits.
The limits are persisted as immutable namespace policy. Every reader, publisher
and cleanup handle must supply the same policy; mismatched or older readers
refuse it. Existing namespaces are not migrated. The existing `/filesystem`
entry exports `RETAINED_REPOSITORY_SAFETY_VERSION = 1` so hosts pinned to older
releases can refuse a requested safety profile before opening storage.

The operation/reference limits are checked before a new intent is saved. An
intent owns its identity even when publication is unconfirmed. With limits alone, expired/deleted
payloads do **not** free those identities: accepted IDs and exact expiry history
are never forgotten or reused. At the limit, new work is refused with fixed
`REPOSITORY_METADATA_LIMIT` health output. Count exhaustion does not disable
existing reads, reconciliation or cleanup; native page/I/O exhaustion can still
refuse them. The host needs an independent alert channel.
Repeated refusals add no per-refusal row. Limits alone bound growth by stopping
admission; they do not provide perpetual availability or retire old periods.
For a finite horizon, use the separate [history-retirement policy](REPOSITORY_HISTORY.md)
and its coordinated reader, publisher and cleanup requirements.

`maxBytes` bounds the fixed SQLite control files, including allocated blocks and
rollback journals. It is **not** a quota for payloads, traces, filesystem directory
metadata, other processes or the whole disk. The minimum is 2 MiB and maximum
1 TiB. The supported local filesystem must have allocation blocks at most
64 KiB. A 1 MiB allowance covers the fixed format fence, sidecars, headers and
allocation rounding; the remaining budget reserves three copies of each allowed
4 KiB database page plus rollback frame overhead. SQLite's native page ceiling
enforces the main-file bound even for one large intent. The new namespace uses
DELETE journaling with cache spilling disabled; both settings are checked on
admission, so a reader cannot pin an ever-growing WAL. Transient control files
are checked after acquiring the writer lock, so another handle cannot remove its
journal during that check. A native `SQLITE_FULL` raised by a ledger write is
`REPOSITORY_METADATA_LIMIT`, including first-open fence/schema/policy/clock writes,
startup commit and budget activation, new intents, publication acknowledgement,
expiry/deletion markers, elapsed checkpoint or final commit. Startup uses the
configured limits even before the budget is active. This also applies
when SQLite has already rolled the transaction back. A publication-side failure
remains `REPOSITORY_UNCONFIRMED`, even if its error happens to carry the same
number. Neither outcome is permission to retry a publication or an uncertain send.

`repository.metadataCapacity({ operations, references })` reports exact retained
counts, remaining counts, charged control-file bytes and the database page
ceiling. It is a snapshot, **not a reservation**: later work or index-page growth
may still refuse admission. A host can stop polling early using this snapshot,
but must still handle an unconfirmed save and report the slot as a gap. Neither
this function nor a refusal fabricates a saved collection receipt.

The repeatable `repository-metadata-limits.test.ts` measurement accepts 20 native
operations (three references each), refuses 100 more, and checks settled and
dirty-transaction control bytes against a 2 MiB policy. Measured locally:
69,632 B settled and 98,304 B observed during dirty transactions. Byte usage stays unchanged
over the repeated refusals and exact counts survive reopen. A 5,000-reference
intent proves native `SQLITE_FULL` rollback without publication, its limit health
code, and SQLite's implicit transaction rollback; a later small
commit succeeds. Real cleanup overlaps seven saves under the bounded profile.
Native saved values, references, cause answers and separate missing-poll gaps
also match the unbounded profile. These are local synthetic safety tests, not a
fleet throughput result or a measured daily/annual storage projection.

Additional tests measure the **actual live rollback journal** and compare its
allocated bytes plus the main/fence files against the library's reported charge.
The maximum native page allowance is also checked against room for a complete
rollback image, a conservative header and the existing fixed files. A real
SQLite connection confirms cache spilling is disabled, and turning it on is
refused. Removing journal charging, over-allocating native pages, or removing
the spill setting must each fail a test. See [SQLite's journal layout](https://www.sqlite.org/fileformat2.html#the_rollback_journal).

## Optional verified-uptime expiry for new namespaces

Set `expiryClock: 'verified-uptime'` to use elapsed time for expiry instead of
wall time. The default remains the existing wall-clock format. No existing
namespace is adopted or converted. Reader-first rollout is required.

This profile saves one elapsed age and acceptance revision in the repository
ledger. A handle credits its observed monotonic duration only while the durable
revision still matches its last successful transaction (or initial snapshot).
Accepting a new operation changes that revision. If another handle accepts data,
the next transaction rebases at the saved age and its current monotonic sample,
discarding all ambiguous earlier time. An idle older handle therefore cannot
charge time from before another handle accepted a new record to that record.
Reads and maintenance take the maximum of the durable age and the handle's
continuous local clock without changing the revision. Overlapping handle
intervals are never summed.

Only committed checkpoints count after restart. Failed transactions and refused
final commits discard their staged checkpoint and acceptance revision. A live
handle retains its earlier committed clock anchor, subject to the next durable
revision check; a rollback cannot erase verified time or adopt a failed new
acceptance. Successful commits preserve local fractional time. Acceptance
rounds upward and expiry age rounds downward to avoid charging a preceding
fractional millisecond to newly accepted data.
Unobserved downtime is **not credited**. Clock origins need not agree between
processes, and neither wall timestamps nor a platform boot clock repair gaps.

These conservative rules can substantially extend retention. Frequent new
acceptances alternating between handles can discard nearly all idle time; with
no elapsed time inside their transactions, age can stop advancing until one
handle continues working without another handle accepting data. Reads and
maintenance alone do not interrupt that progress. This profile promises no
particular deletion time or minimum rate of age progress, and supplies no
storage-capacity bound. Retention never becomes shorter because the wall clock
jumped.

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

New namespaces use format **5**, retaining the `retention-v2.sqlite` filename,
with a separate legacy-reader fence at `retention.sqlite`. The fence first saves
an initializing identity, the ledger binds that same identity, then activation
commits after the ledger. Interrupted first setup can resume; once active, a
missing ledger or missing critical authority table/row is refused on reopen,
never replaced with empty ownership or age zero. Prior unpublished verified-age
formats and legacy namespaces are refused rather than migrated automatically.
Old constructors reject the new fence, but refusal is not necessarily read-only:
a 0.9 constructor can create empty legacy tables and change fence journaling
before its version check. Upgrade and stop all older managed readers, publishers
and cleanup workers before enabling a new namespace; do not mix versions there.
Losing either side is unavailable storage, not permission to initialize over old IDs. This protects incomplete
stores; it does not authenticate arbitrary external replacements or stale backups.
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

## Validation and capacity planning

Self-contained repository tests use synthetic library records to verify immutable
reopening, explicit expiry, interrupted publication recovery and deletion proof.
Contention tests hold real cleanup at file deletion while independent poll saves
finish; fault tests preserve unconfirmed outcomes rather than inventing receipts.
Run the focused checks from this checkout:

```sh
npm test -- test/repository-retention.test.ts test/repository-retention-faults.test.ts test/repository-contention.test.ts
```

A short controlled-clock TTL in a test is not a recommended retention policy.
Measure sustained marginal growth on the intended host, including packed data,
reference membership, tombstones, SQLite journals, trace metadata and raw archives.
One small namespace includes fixed startup costs and cannot establish a daily
capacity requirement or collection throughput.

This adds expiry, not a repository disk quota. The trace guard limits traces
only. Independent headroom monitoring, retention choices, capacity sizing and
sustained host tests remain required before deployment. Old readers cannot
explain ledger expiry and must never write into or replace managed readers in
this namespace. Existing collectors, mappings, application flows and services
stay unchanged. Upgrade readers before enabling any managed storage.
