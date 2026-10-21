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

`lookupOperation` and `lookupReference` return detached frozen metadata;
`receipt` retains the existing retention-receipt fields exactly. An absent lookup
means only **not in this selected snapshot**, never "never existed". The selected
set may omit operations. This store cannot authorize a retry or rule out a
conflict elsewhere. Existing hot admission checks do not consult it yet.

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
value or secret included. A failure after publication is unconfirmed rather than
proof of no write. An identical retry verifies and synchronizes saved metadata;
it does not replace an existing selected snapshot.

Fault tests cover interrupted marker/segment synchronization and linking, and
catalogue cuts before/after publication. A process kill **before the marker is
published** can leave an unmarked staging file. That nonempty directory is then
refused as unmanaged: the operator must inspect it and choose a fresh namespace.
There is no automatic orphan cleanup or adoption. Process-kill testing and the
future hot-to-sealed migration transaction are outside this slice.

## Old-reader gate

Only a new v2 namespace is supported; v1 and nonempty unmarked directories are
never adopted. `retention.sqlite` carries `policy.version = 2`. Its exact schema,
single policy/descriptor rows and empty guard tables are checked on each open.

The empty legacy tables/index are deliberate. The unchanged 0.9 retained reader
creates them before checking its policy. Predeclaring their exact empty schema
lets that constructor reject version 2 without changing metadata schema. Tests
run byte-identical, hash-pinned 0.9 source in a child process, then reopen and
verify the snapshot after child exit. Extra schema or guard content is refused.

This is a constructor gate, **not fencing for already-open old handles**. A plain
`FilesystemRepository` does not inspect this marker and is unsupported for v2.
There is no in-place rollout or production activation in this change.

## Offline implementation measurement

The [recorded report](measurements/sealed-history-2026-10-21.json) replays each of
two closed synthetic 3,000-poll diagnostics. For **each** case, all 6,000 operation
metadata rows and 21,000 native reference memberships match exactly. Input
database hashes are checked before and after replay; sources are untouched.

| Complete files per case | Bytes |
| --- | ---: |
| Original two closed SQLite ledgers | 6,696,960 |
| Two binary snapshots, including headers/checksums/index | 1,446,192 |
| Two version-marker databases, including empty guard indexes | 65,536 |
| Two catalogues | 348 |
| Total logical files | **1,512,076** |
| File allocation on the test filesystem | 1,523,712 |

The exact reference index alone is 756,000 bytes, roughly half the representation,
and is deliberately retained. This is a smaller standalone representation, not
disk reclaimed from running repositories. Directory allocation, hot working data,
payloads, traces and process memory are not included in these file-byte totals.

Repeating these fixed 3,000-poll units at 14 sources every 5 seconds extrapolates
to about **122 MB/day or 44.5 GB/year** of complete logical files. It is not a
measured fleet rate or a capacity claim. The earlier design's approximately
117 MB/day / 43 GB/year projection excluded the new per-snapshot marker overhead.
Future segment-catalogue integration can amortize it; that is unimplemented.

Reproduce with closed synthetic exports (large inputs/binaries are not committed):

```sh
node --import tsx examples/measure-sealed-history.ts \
  closed-ledgers.json public-id-capture.json NEW_OUTPUT_DIRECTORY
npm exec -- vitest run test/sealed-history.test.ts \
  test/sealed-history-codec.test.ts test/sealed-history-faults.test.ts
```

The report contains input hashes and file sizes. This replay proves metadata
and ownership equivalence, **not native cause replay**. Payload expiry has already
removed the source values. The next separately reviewed slice must connect a
consistent selected snapshot to hot reads/admission, publish it crash-safely,
and only then remove eligible hot rows. No period-retirement decision is made.
