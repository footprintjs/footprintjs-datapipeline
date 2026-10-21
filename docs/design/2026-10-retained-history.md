# Bookkeeping segments: offline prototype and library proposal

LOCAL SEED, 2026-10-21. **Design/measurement only.** Nothing in a collector,
library runtime, design PR, service, mapping flag or Python script was changed.
The live stores and all retained measurements were left untouched. The kept-history length remains undecided.

## Result

The same two **closed** 3,000-poll diagnostics were read using SQLite
`mode=ro&immutable=1`. Each contains 6,000 operation rows and 21,000 exact
reference memberships after payload expiry. The real public PowerScale poller
was replayed with in-memory repositories and its existing synthetic response
fixture. All operation hashes, request-fingerprint hashes, complete-write hashes,
accepted/expiry times, publication/deletion flags and memberships match both
closed ledgers. No device, database server, network or InfluxDB call occurred.

The following are **complete encoded byte counts**, per diagnostic. Both have
the same encoded lengths. The baseline is the two original SQLite files together.

| Format | Bytes | Allocated bytes | Relative to 6,696,960 B | Gzip bytes, first case |
| --- | ---: | ---: | ---: | ---: |
| Original SQLite bookkeeping | 6,696,960 | 6,696,960 | 1× | — |
| Generic exact full-ledger segment | 1,447,399 | 1,449,984 | 4.63× smaller | 1,385,502 |
| Poll-profile bitmap segment, full ledger | 1,372,841 | 1,376,256 | 4.88× smaller | 1,345,887 |
| Generic terminal public receipts only | 1,063,393 | 1,064,960 | 6.30× smaller | 993,714 |

The first two variants reproduce **every internal operation column** and every
membership. The third deliberately omits the terminal request/write hashes;
it preserves current public expiry-receipt fields and memberships only and
refuses pending or not-deleted operations. Its larger saving must not be
described as full internal-row equivalence.

The bitmap profile improves on the simpler generic full-ledger format by only
74,558 bytes, about **5.15%**. Most of the benefit comes from compact exact binary
rows and eliminating SQLite's duplicated text/index representation, not the bitmap.
These are sealed-file results, not a production SQLite-schema or throughput result.

## What the IDs actually mean

Verified against datapipeline 0.9.0 `src/createPipeline.ts`, the retained-repository
helpers and the adapter's actual public commit writes:

| Identity/check | Inputs it uses | Content-addressed? |
| --- | --- | --- |
| Core operation ID | Operation kind and exact idempotency key | No: logical identity |
| Batch/evidence reference | Operation ID, record kind, candidate batch key | No |
| Observation reference | Operation ID, record kind, logical observation key | No |
| Ledger operation key | SHA-256 of the core operation ID string | No additional content proof |
| Ledger reference key | Existing canonical digest of `{kind,id}` | Membership lookup key |
| Request fingerprint | Normalized request plus registered source/origin/config identity | Configuration/request consistency |
| Complete-write hash | The complete prepared write, including observations and evidence | Yes, over that prepared JSON |
| Raw-body SHA / saved trace ID | Raw body / saved trace bytes | Content checks |

A public-core test uses the same identity/request with different numeric values
in separate in-memory repositories. It gets the same operation ID, reference IDs
and request fingerprint, but different complete-write hashes. This does **not**
authorize overwriting a real committed operation; the normal repository guards
still apply.

For this deterministic poll profile, regeneration needs the exact source and raw
store revisions, adapter/core codec revision, run-ID template (`memory-${index}`),
slot/anchor/cadence, batch keys and each run's observation-key pattern. The actual
adapter qualifies its keys using the source and raw-store identities. Changing a
logical key changes its reference. Failure, empty and partial paths do not all
have the same observation keys or counts.

Schema references and metric values are not directly inputs to core reference
IDs. They do affect prepared content and can affect adapter key patterns. Request
fingerprints additionally depend on registered configuration/origin and, here,
the storage-policy reference. That reference includes canonical paths, retention
periods and the free-space floor. Reconstructing a complete-write hash requires
the full prepared content, including the response; expired data cannot be
recovered from a bitmap. This prototype retains that hash instead of guessing it.

The existing library digest and operation-hash helpers were **imported**, not
reimplemented in a production adapter. IDs and logical keys were captured through
the public poller's commits. A future supported identity codec must live in the
library and be versioned; a Neo copy of private hashing rules is not proposed.

The original filesystem receipt's wall-clock `committedAt` was in the deleted
native operation, not the surviving ledger. It cannot be replayed from these
expired stores. The ledger's actual `acceptedAt`, `expiresAt`, publication and
deletion facts are preserved exactly; no claim is made to recover lost timestamps.

## Why a window is more than eight bytes

One 60-bit bitmap (stored in eight bytes) can say which of sixty slots are
present. It cannot preserve their identities, state, owner, failure semantics,
actual receipt times or their immutable content checks.

The measured profile contains, across both namespaces:

| Complete component | Bytes |
| --- | ---: |
| 100 window blocks: state/outcome bitmaps, pattern/time exceptions, retained hashes | 392,800 |
| Window directory: timestamps, offsets, lengths, per-window checksums | 4,800 |
| Exact sorted operation-hash → slot index | 216,000 |
| Exact sorted reference-hash → owning slot index | 756,000 |
| Versioned profile/header/section directory and final checksum | 3,241 |
| **Total** | **1,372,841** |

Each window has nine masks: presence, stored, expired, confirmed, data-deleted,
and four run-receipt outcomes. A mask identifies a **recorded** fact, not physical
success. An uncertain acquisition can have an intent with no compact receipt;
an empty successful response is distinct from a missing poll. The source fixture
was all complete; separate public synthetic paths exercise those exceptions.

The exact opaque-ID lookup tables are included. Without them, today's API cannot
route a random operation/reference hash to its slot without scanning/rebuilding
the kept history. Regeneration is useful but does not remove this lookup cost.
Actual acceptance/expiry times that differ from the scheduled instant are encoded
as exceptions; a synthetic +37 ms case round-trips both exactly.

All byte counts include namespace/policy metadata, record layout, indexes,
publication/deletion flags, full lookup hashes, request/write hashes where promised,
checksums and exception counts. No external dictionary is required for decoding.
Gzip measurements include the entire checksummed container; inflation is capped.

## Proofs and limits

- Exact replay: 6,000 complete internal operation rows and 21,000 memberships per
  diagnostic for both full variants. The terminal variant separately checks all
  public receipt fields and memberships.
- 162,000 positive/altered lookup checks per diagnostic, 324,000 across both.
  Altering a known hash is not accepted as another known record. Foreign opaque
  hashes do not become recorded receipts or acquire claim authority.
- Additional public-poller examples preserve a missing slot, failed acquisition,
  partial capture, empty capture and unknown publication. Pending intent data is
  retained by full encodings; terminal-only compaction refuses it.
- Changed containers fail checksums. Bounded inflation rejects over-limit output,
  truncated gzip and trailing data. The decoder ceiling is 16 MiB and the metadata
  header ceiling is 512 KiB; this is not a hardened production decoder audit.
- Original closed-ledger hashes remain unchanged. No saved response, trace or
  historical source row was recreated. Native cause replay and live-store crash
  recovery have not been tested by this metadata-only prototype.

The measured files represent the complete inventory of one finite 3,000-poll
namespace pair. Future multi-segment routing/catalogue cost, hot SQLite and WAL,
concurrent publication, fsync latency, temporary migration duplication and
compaction CPU are **not measured**. No fleet, full-day, memory-safety or disk
steady-state claim follows from these numbers.

## Finite kept-history contract

The owner must choose the horizon length; it is `null` in the proposal metadata.
A **test-only** last-two-window subset keeps 840 memberships exactly expired.
The 20,160 older memberships return `history-unavailable`, as do unresolved foreign
hashes. Old IDs are never reported as never having existed. All 6,000 operation
lookups obey the corresponding boundary as well.

This conservative result matters: an opaque hash contains no trustworthy age.
After membership history is dropped, it cannot in general distinguish an old
legitimate ID from a never-published foreign ID. Returning unavailable for both
is honest and prevents history/count leakage. A caller-supplied slot is not proof.

The prototype is a **read-only sealed reader**, not an admission protocol. It
does not prove that arbitrary new writes remain safe after history is forgotten.
Under today's generic arbitrary-ID contract, deleting membership can permit old
operation/reference resurrection. Keep exact history or retire/fence the entire
old namespace. Continuing with a deterministic profile requires a new, validated
epoch/slot identity contract owned by the library, including rules for rejecting
historical IDs. That is a separate reviewed semantic change, not compaction.

## Smallest library slice to review

Recommend the **generic exact sealed segment first**. It achieves most measured
savings, supports arbitrary native operations and preserves all internal fields.
The poll-specific profile costs extra versioning/exception logic for a small
additional reduction; do not require it for every product adapter.

1. Keep pending/live operations in the existing hot ledger. An opt-in, bounded
   maintenance operation selects terminal, deleted operations through library-owned
   APIs, not application SQL. Preserve every original reference and public receipt.
2. Write a new exact segment, sync it, reopen/verify hashes and row equivalence,
   then atomically publish a versioned, synced catalogue. Only after that may a
   transaction remove the corresponding hot rows. No app chooses the rows itself.
3. During transition, lookups and admission check hot **and** sealed membership.
   Equal duplicates are safe; a disagreement or uncertain catalogue is unavailable,
   never absent. A crash before hot-row removal leaves duplicates; a crash after
   catalogue publication must still find all sealed identities. Prove every cut.
4. Design bounded catalogue/index lookup and SQLite free-page/WAL reclamation.
   Never leave an unbounded per-reference routing table beside the compressed
   segment and claim total bookkeeping is bounded. Account for temporary copies
   and both storage areas in headroom reservations and measurements.
5. Preserve the current perpetual exact contract unless a finite-history mode is
   explicitly selected and its retirement/admission semantics reviewed. Horizon,
   independent health reporting and namespace fencing are owner decisions.

Ordinary per-window **payload** packing is a different change. Waiting five minutes
to publish would weaken per-poll crash durability; an appendable window needs a new
durable protocol. Cold metadata segmentation can preserve the current per-poll
collection facts without that change. No new graph format, walker or agent tool
is needed; access checks and shared claim requirements remain in their layers.

## Proposed interfaces, for review only

No exported type or API is implemented by this draft. An exact sealed-history
maintenance option must be off by default. It would preserve current operation
and reference IDs, commit reconciliation, conflict checks and expiry receipts.
Old readers must be upgraded before any namespace writes the sealed format.

```ts
// Direction only: names and compatibility are not a released contract.
type HistoryLookup<T> =
  | { status: 'found'; value: T }
  | { status: 'history-unavailable' }
  | { status: 'unavailable' };

interface SealedHistoryOptions {
  enabled: boolean;
  maxMaintenanceOperations: number;
  maxSegmentBytes: number;
  maxInflatedBytes: number;
  maxBookkeepingBytes: number;
  history: { mode: 'exact' };
}
```

Finite history is deliberately excluded from the first implementation. Merely
adding a millisecond horizon would allow forgotten IDs to be reused under today's
API. Its later contract must include a validated namespace/epoch boundary and
admission fence. An unknown opaque hash cannot establish its own age: after
history is dropped, unknown and older references both return history unavailable,
never an invented proof that a poll never existed. The owner chooses the period
only after that separate contract is reviewed; no default period is proposed.

A hard cap includes hot rows, indexes, sealed files, catalogue, WAL and temporary
compaction copies. Pending work cannot be silently forgotten to meet it. If a
new durable publication cannot be reserved, stop admission before source access
and use the independent host health sink. Neither a missing failure receipt nor
a metadata-only identity proves device success, row contents or physical cause.

## Review and implementation order

This draft asks for agreement on the exact generic sealed format first, not
permission to activate a collector. Require crash-cut tests for file sync,
catalogue publication, hot-row removal and restart, plus corrupt/foreign/pending
IDs and unchanged replay/cause answers on actual saved operations. Measure total
allocated bytes, memory, CPU, lookup/compaction latency and temporary headroom on
the shared-process layout before claiming capacity or a steady state. Native
cause replay is not proven by the metadata-only experiment.

The [portable measurement record](../measurements/bookkeeping-segments-2026-10-21.json)
keeps exact hashes, all counted sections, proof totals and the local reproduction
recipe. Large synthetic captures and encoded artifacts remain in the local
archive; this PR changes no runtime, dependency, saved record or retention policy.
