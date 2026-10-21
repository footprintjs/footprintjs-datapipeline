# Exact sealed metadata snapshots (opt in)

This first implementation is a bounded codec and a metadata-only, read-only
snapshot store. It does **not** compact a retained repository. No hot/cold read
routing, migration, hot-row deletion, admission integration or namespace-wide
completeness check is implemented. Existing repositories and expiry stay unchanged.

## Explicit boundary

`createSealedFilesystemHistory` is exported from `/filesystem`. Unless
`enabled: true`, it returns `undefined` before validation or filesystem access.
It starts no timer, owns no collector and is not a `Repository` replacement.

An enabled store requires an existing empty private directory, an explicit
`retentionMs`, and a local POSIX filesystem with working hard links and `fsync`.
It refuses the filesystem root, symlinks and group/world-accessible storage.
A separate filesystem is required unless the host explicitly sets
`requireSeparateFilesystem: false`, as the offline tests do.

`retentionMs` is the **original payload-expiry interval**, carried forward in
each receipt. It is not a history horizon. This implementation adds no history
TTL, deletion, retirement policy or default retention period. Sealed metadata is
kept indefinitely.

```ts
import { createSealedFilesystemHistory } from 'footprintjs-datapipeline/filesystem';

const history = createSealedFilesystemHistory({
  enabled: true,
  directory: hostOwnedEmptyDirectory,
  retentionMs: originalPayloadPolicy.retentionMs,
});
// selectedMetadata is an exact, validated retained-ledger snapshot supplied by
// the owner. This release does not export or select it from a live repository.
await history!.publish(selectedMetadata);
const view = await history!.open();
const receipt = view.receipt(originalOperationId);
const owner = view.lookupReference(originalRecordRef);
```

The public native-ID lookups reuse the existing library operation/reference hash
helpers internally. Callers do not reproduce private identity formulas. Native
IDs identify logical work; they do not regenerate request fingerprints, write
hashes, publication flags or original receipt times from a clock.

## What is preserved

Every operation keeps its exact operation hash, request fingerprint, write hash,
accepted/expiry times, status, publication acknowledgement and data-deleted flag.
Every reference keeps its full hash and exact owning operation. Kinds remain
part of native reference identity. No bitmap or probabilistic membership filter
replaces that index.

The internal codec can round-trip pending, stored and expired metadata. The
publisher accepts **only expired, acknowledged operations whose payload has been
deleted**. Live, pending, uncertain or not-deleted operations are refused before
any namespace write. They must remain hot in a future integration.

`lookupOperation` and `lookupReference` return `{ state: 'found', operation }`
with detached frozen metadata, or `{ state: 'not-in-snapshot' }`. `receipt`
returns `{ state: 'found', receipt }` with the existing retention-receipt fields
exactly, or the same explicit miss. A miss means only **not in this selected
snapshot**, never "never existed". The selected set may omit operations. This
store cannot authorize a retry or rule out a conflict elsewhere. Existing hot
admission checks do not consult it yet; disagreement/unavailable routing belongs
to that later integration.

An expired operation remains expired. No source payload, collection result,
missing-poll reason, native trace or claim authority can be recovered from this
metadata. The checksum establishes consistent bytes, not source authenticity.
Application authorization and value hiding still apply when using a view.

## Format, bounds and publication

One namespace publishes one immutable selected snapshot. Input order is
canonicalized; an identical selection reconciles, while different content is a
conflict. Invalid rows, holes, indexed accessors, extra row fields, duplicate
operations/references or foreign owners are refused without dropping a prefix.

The binary format uses a 64-byte header, 115 bytes per operation, 36 bytes per
reference (32-byte hash plus exact owner ordinal), and a 32-byte SHA-256 footer.
All timestamps must round-trip as nonnegative safe integers. The entire snapshot
is bounded to 16 MiB; `maxSnapshotBytes` may lower that limit. Reads are bounded
even if a file grows while being read. Decode checks lengths, counts, offsets,
reserved fields, sorted uniqueness, flags, owners and the checksum before lookup.
Each view owns a bounded detached copy; the host controls how many views it keeps.

Publication writes and synchronizes a segment, reopens and verifies it, publishes
its immutable content-addressed inode, then synchronizes a catalogue referencing
it. Acknowledgement follows synchronization of all required directory entries.
The catalogue and SQLite marker overhead are included in measurements below.

Failures use fixed `SealedHistoryError` codes/text, with no raw exception, path,
value or secret included. A read prevented by I/O or a busy SQLite lock is
`SEALED_HISTORY_UNAVAILABLE`. Changed ownership, unreadable owner permissions,
group/world permissions, access rejection (`EACCES`/`EPERM`), links or unexpected
unmarked namespace entries are
`SEALED_HISTORY_UNMANAGED`. Invalid bytes or a missing snapshot named by a valid
catalogue are `SEALED_HISTORY_CORRUPT`. A failure after publication is unconfirmed
rather than proof of no write. An identical retry verifies and synchronizes saved
metadata; it does not replace an existing selected snapshot.

Marker creation uses a private `.staging/marker-<pid>-<uuid>/` directory on the
same filesystem. While no marker exists, a check clears only recognized marker
files and SQLite journals belonging to dead publishers. Live or reused PIDs keep
their staging files. Unknown names, links, owners and permissions are refused;
cleanup never recursively removes arbitrary contents. Tests use actual child
SIGKILLs at sampled initialization boundaries and varied publication delays,
and keep two live publishers' marker stages separate.

Interrupted `.stage-*` snapshot/catalogue files and a losing publisher's `.bin`
file are **never automatically removed**. The 16 MiB ceiling bounds only the
selected snapshot: the real namespace limit is **one snapshot plus leftovers**.
A hard cap counting staging, leftovers and SQLite sidecars, and the future
hot-to-sealed migration transaction, remain later work.

## Old-reader gate

Only a new v2 namespace is supported; v1 and nonempty unmarked directories are
never adopted. `retention.sqlite` carries `policy.version = 2`. Its exact schema,
single policy/descriptor rows and empty guard tables are checked on each open.

The empty legacy tables/index are deliberate. The unchanged 0.9 retained reader
creates them before checking its policy. Predeclaring their exact empty schema
lets that constructor reject version 2 while preserving the declared schema.
The old constructor first switches the marker from DELETE to WAL journal mode:
SQLite header bytes 18–19 change from `1,1` to `2,2`, so the marker bytes change.
The new schema check explicitly accepts both DELETE and WAL. Tests run
byte-identical, hash-pinned 0.9 source in a child process, assert that marker side
effect, then reopen and verify unchanged catalogue/snapshot bytes and metadata.
Extra schema or guard content is refused.

This is a constructor gate, **not fencing for already-open old handles**. A plain
`FilesystemRepository` does not inspect this marker and is unsupported for v2.
There is no in-place rollout or production activation in this change.

## Offline implementation measurement

The historical [recorded report](measurements/sealed-history-2026-10-21.json)
records replays of two closed synthetic 3,000-poll diagnostics. For **each** case,
it records exact matches for all 6,000 operation metadata rows and 21,000 native
reference memberships, with input hashes checked before and after replay. The
two cases have identical row counts and sizes: they are **one size data point,
not two**. The original ledgers/exports are unavailable in this checkout; this
revision has **not rerun that historical replay**. Its original input and code
hashes remain unchanged in the report.

| Complete files per case | Bytes |
| --- | ---: |
| Original two closed SQLite ledgers | 6,696,960 |
| Two binary snapshots, including headers/checksums/index | 1,446,192 |
| Two version-marker databases, including empty guard indexes | 65,536 |
| Two catalogues | 348 |
| Total logical files | **1,512,076** |
| File allocation on the test filesystem | 1,523,712 |

The ratio of original closed SQLite ledger bytes to complete logical files is
**4.43×** (`6,696,960 / 1,512,076`, rounded). The exact reference index alone is
756,000 bytes, roughly half the representation, and is deliberately retained.
This is a smaller standalone representation, not disk reclaimed from running
repositories. Directory allocation, leftovers, hot working data, payloads,
traces and process memory are not included in these file-byte totals.

Repeating these fixed 3,000-poll units at 14 sources every 5 seconds extrapolates
to about **122 MB/day or 44.5 GB/year** of complete logical files. It is not a
measured fleet rate or a capacity claim. The earlier design's approximately
117 MB/day / 43 GB/year projection excluded the new per-snapshot marker overhead.
Future segment-catalogue integration can amortize it; that is unimplemented.
The projection uses decimal MB/GB and 365 days/year. An automated contract
recomputes this table, the ratio and the projection from the historical report,
and pins the current layout at 669,096 B for 3,000 operations/9,000 references
and 777,096 B for 3,000 operations/12,000 references. It verifies recorded
arithmetic and the current format, not the unavailable original input ledgers.

At the projected rate, a namespace receives about 242,000 operations/day; one
16 MiB snapshot holds roughly 6–7 hours. The fixed 3,000-poll projection also
implies about 160 new namespaces/day. A later append-only segment catalogue and
its crash/concurrency protocol are required before this becomes a day-scale
store. The separate-filesystem guard currently compares only with `/`, and open
revalidates/copies a complete snapshot; neither is a fleet capacity guarantee.

Reproduce with closed synthetic exports (large inputs/binaries are not committed):

```sh
node --import tsx examples/measure-sealed-history.ts \
  closed-ledgers.json public-id-capture.json NEW_OUTPUT_DIRECTORY
npm exec -- vitest run test/sealed-history.test.ts \
  test/sealed-history-codec.test.ts test/sealed-history-faults.test.ts \
  test/sealed-history-process.test.ts test/sealed-history-io.test.ts \
  test/sealed-history-measurement.test.ts
```

The report contains input hashes and file sizes. This replay proves metadata
and ownership equivalence, **not native cause replay**. Payload expiry has already
removed the source values. The next separately reviewed slice must connect a
consistent selected snapshot to hot reads/admission, publish it crash-safely,
and only then remove eligible hot rows. No period-retirement decision is made.
