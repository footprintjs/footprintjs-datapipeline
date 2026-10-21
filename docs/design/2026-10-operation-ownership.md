# One owner for saved attempts and intents

Status: shared design decision for the fleet-polling and multi-input-writer
drafts. No runtime, public API, backend or service is implemented here.

## Decision: the repository owns the facts

Use the existing `Repository` and `Pipeline.collect`, `reconcileCollect` and
`read` as the one persistence/reconciliation port. Host events are ordinary
versioned observations from a bounded host-event `SourceAdapter`, as already
described in `docs/INFLUXDB_HOST_RECEIPTS.md`.

Neo's `performanceWriter/WriterJournal` already wraps that port. Its snapshots
are temporary adapter inputs, and its durable events are repository records.
It is not another durable journal. Reuse that pattern through one small event
facade; do not introduce a second event store or attempt-state machine.

Remove the proposed `FleetJournal`, durable fleet cursor and fleet outcome
tables. Deterministic slot lookup over a bounded requested window meets recovery
requirements. Distinguishing a request that never started from one that started
and crashed before its first saved fact does not justify another store: both
are `not collected`, with whether device access happened explicitly unknown.

| Fact | Authoritative saved evidence |
| --- | --- |
| Poll completed or failed | Existing acquisition/compact poll records |
| Dispatch refused before device access | Ordinary failed-poll receipt and its structured host-event evidence |
| Which inputs an assessment used | Ordinary saved manifest observation |
| Influx write may start | Existing acknowledged publish-intent observation |
| Delivery was observed | Attempt-result observation, independently of receipt persistence |
| Payload expired | Existing retained-repository expiry receipt |

Raw and compact namespaces may remain physically separate. Each fact has one
owner and references the others; there is no transaction across repositories.
The retention ledger describes publication/expiry, not a second application
answer about device or Influx success. Summaries, caches and native traces may
refer to attempt facts but cannot override them.

## Identities and recovery

Acquisition namespace, stable source/schedule revision and slot time identify a
poll. Fleet membership/version is audit metadata, not part of an unchanged
source's slot identity. Writer run/stage/attempt identities retain their existing
versioned profile rules. A shared facade delegates identity construction to those
declared profiles and existing pipeline APIs; it never copies private core hashes.

Lookup uses the exact recorded request/configuration through `reconcileCollect`.
It never calls `collect` to discover a result, invokes a source, sends to Influx,
or scans private directories/tables. `undefined` means no published result was
found; a failed/conflicting lookup is not the same thing. Retention errors stay
expired or unconfirmed only when the storage contract supplies that evidence.

For an elapsed poll with no complete compact result, keep a gap. A saved raw
acquisition alone is not a complete compact poll. An explicit later repair may
project that same saved raw input; recovery never acquires a historical substitute.
If an ordinary failure fact is saved during recovery, its learned time is now,
not the historical slot time. The original assessment is never rewritten.

Before Influx dispatch, the existing capture, typed-point manifest and publish
intent must all be acknowledged. Intent without a saved terminal result means
delivery is unknown, with no automatic resend. This stronger write-side rule
does not require another fleet intent database or another fact before every poll.
Conflicting attempts, unconfirmed publication and expired supporting facts fail
safe; neither a cache nor an owner lock can convert them to success.

## Exclusive ownership is a separate guard

The current retained repository uses short SQLite `BEGIN IMMEDIATE` transactions.
They serialize storage access, not the interval from dispatch through device I/O.
The existing scheduler's in-process serialization is also insufficient. Neither
already prevents two processes or hosts from polling the same source.

The proposed coordinator must acquire an exclusive ownership guard before any
dispatch, including manual `runDue`. Scope includes the canonical acquisition
owner and stable physical-source ownership keys, independent of plan revisions.
Overlapping plans must conflict on the shared source keys. Acquire a bounded set
atomically, or in fixed order with complete rollback before device access.

```ts
// Proposed guard contract, not a journal or an exported API.
interface ExclusiveOwnership {
  run<T>(request: {
    readonly authorityRef: VersionedRef;
    readonly sourceOwnerKeys: readonly string[];
    readonly signal: AbortSignal;
  }, work: (guard: {
    assertHeld(): Promise<void>;
  }) => Promise<T>): Promise<T>;
}
```

An implementation must actually exclude other owners. Check ownership again
immediately before source dispatch. Stop new work on lost/unknown ownership;
cancel and drain in-flight work before releasing. Never release merely because
`stop()` was requested. Construction without a supported guard fails closed.

For the first single-host backend, use a lifetime SQLite owner lock on a
dedicated lock connection/file in the canonical authority directory. Do not
hold a retention-ledger write transaction for the whole run: that would block
poll commits and cleanup. The lock contains no attempt/outcome records. Two
real processes must be tested; a crash must release the OS-backed lock, while
a slow owner must not be evicted by a timer.

That local lock is not a cross-host lock. Multiple hosts require one shared
authority providing atomic, non-expiring exclusive grants over the same keys.
A grant is released after drain; after a host crash, takeover requires fencing
the old owner before an operator releases its grant. No TTL-based automatic
takeover is proposed: devices do not accept fencing tokens, so an old host
could otherwise keep sending requests after its lease expired. A local-only
backend must refuse a multi-host deployment; independent local lock files are
never presented as protection. The shared-authority binding and its two-host
failure tests are prerequisites to multi-host rollout, not capabilities already
provided by datapipeline 0.9.0.

Ownership metadata is coordination only. It cannot say a poll happened, replace
a missing receipt, approve a claim or establish physical cause.

## One failure vocabulary

The generic coordinator accepts an adapter-declared failure-policy binding,
not another enum of reason strings. PowerScale imports the single
`FAILURE_CATEGORIES` owner introduced by Neo #46, including #48's storage reasons.
Any new overlap/capacity categories must be added and validated there once;
neither design copies a list or classifies errors from message text.

Coverage states such as no saved result, expiry or unconfirmed publication keep
their existing repository/window meanings. They are not fabricated capture
categories or a second set of `RequirementReason` names. Fixed dispatch reasons
are retained in receipt-linked structured evidence. They describe a host
decision, not an attempted device request.

## Required shared proofs and first slice

Before either integration: prove ordinary host-event save/lookup with the same
repository port, stable identities and no second journal. Inject crashes before
publication, after publication before acknowledgement, before device access,
after capture before compact publication, and after Influx intent/dispatch.
The answer must come only from saved facts; uncertain delivery never retries.
Prove a deleted cache/cursor cannot change recovered facts because none is
authoritative, and no device/sink capability is available to lookup recovery.

The fleet slice additionally requires real competing-owner tests, overlap across
plan revisions, and failure to save a dispatch gap. The writer slice retains
save-before-send and exact manifest/support checks. Existing stored records and
Python/output behavior are never rewritten. Each implementation remains opt-in
and receives its own review before any deployment.
