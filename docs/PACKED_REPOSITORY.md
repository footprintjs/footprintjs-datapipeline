# Optional packed filesystem operations

```ts
import { createFilesystemRepository } from 'footprintjs-datapipeline/filesystem';

const repository = createFilesystemRepository({
  directory: '/application-owned/evidence',
  packing: 'gzip', // Default: 'none'. New operations only.
  // maxPackedBytes: 8 * 1024 * 1024, // Optional lower ceiling.
});
```

Upgrade **every reader** before enabling packed writes. New readers open both
formats even with packing disabled. Old readers cannot discover packed operations;
do not roll them back into a namespace containing packed data. To stop new packed
writes, use `packing: 'none'` with the upgraded package. Nothing migrates, rewrites,
samples or deletes existing records. No setting is enabled automatically.

## Same logical records, different physical encoding

One operation directory contains either the existing `manifest.json` and individual
record files, or one `operation.json.gz`. The packed version-1 envelope contains
the original manifest and canonical UTF-8 record strings, indexed by their existing
hashed filenames. Record references, original record bytes and hashes, request
fingerprints, batch membership, structured evidence and public storage ports stay
unchanged. An operation with both manifests is refused as ambiguous; corruption
never triggers fallback to a different format. Files are decoded in memory, not
extracted by supplied pathnames.

This is **one bundle per committed operation**, not one delayed write per whole
collector run. Capture, derivation and send-intent operations remain independently
durable. The repository snapshots input, validates membership and computes record
hashes before asynchronous writes. Packed content is checked against its limit,
compressed, written and synced in a private staging directory, then reopened and
every original record hash and the intended manifest are verified. Only then is
the directory atomically published and its parent synced. Only a committed receipt
allows the application to continue its existing save-before-send sequence.

Publication followed by sync failure stays `unknown`; restart reconciles the
published operation. It never means “not written” and causes no automatic resend.
Concurrent plain/packed writers retain the original immutable-winner/idempotency
rules. A later reconciliation returns the saved receipt, not a new capture.

## Bounds and consistency

`maxPackedBytes` defaults to **16 MiB** and may only lower that limit. It covers
the complete uncompressed envelope, including escaped record strings and index.
The existing gzip codec bounds compressed input to that ceiling plus 64 KiB and
enforces the output limit while inflating. Invalid UTF-8, invalid JSON, wrong
versions, duplicate/missing/extra entries, unsafe indexed names and mismatched
hashes are refused. Writes over the content limit fail as `packed_limit` before
publication; packed read failures use fixed `storage_corrupt` errors.

At most four inflates run concurrently per repository instance. Overlapping reads
of the same operation share that in-flight decode; completed data and failures are
not cached. Returned records/receipts are caller-owned copies. Applications still
must authorize each access: this internal I/O sharing grants no access or claim
authority. Existing plain-file size behavior is unchanged.

The filesystem backend remains a single-host reference backend with atomic rename
and working `fsync`. Lookup still scans operations, and discovering packed indexes
requires inflation. Packing reduces bytes/files; it does not prove sustained
collection throughput or solve historical indexing. Measure the intended host,
namespace size and recovery cost before service deployment. Content checks are
consistency checks, not source authentication or a tamper-evident archive.

## Actual writer execution proof

Run the inactive Neo client writer against saved synthetic input, with a fixed
poll clock, filesystem stores and a simulated acknowledged sink:

```sh
node --import tsx docs/measurements/measure-packed-repository.mts \
  /path/to/neo/packages/powerscale-datapipeline /new/output/directory
```

The script runs the real writer once per profile, tees the same immutable commits
to plain and packed repositories, reads the writer's inputs through the packed
backend, checks intent is durable before HTTP, and reopens every record and
operation in a fresh reader. It compares exact per-record canonical bytes/hashes,
and four first/last-point cause outputs before/after trace reopening. Existing
incomplete-cause gaps remain unchanged; no physical cause is established.

| Saved profile | Plain bytes / files | Packed bytes / files | Allocated file bytes: plain → packed |
| --- | ---: | ---: | ---: |
| 50 returned, 50 retained clients + coverage | 202,243 / 75 | 27,452 / 6 | 360,448 → 40,960 |
| 256 returned, 154 retained clients + coverage | 618,011 / 179 | 71,388 / 6 | 966,656 → 86,016 |

These are measured runs, not production captures or daily/retention forecasts.
The implementation stores canonical UTF-8 strings instead of the earlier
experiment's base64 files and duplicate file hashes, so its numbers differ.
Acknowledgement times differ between two physical commits; all logical records,
references and request fingerprints match, and each backend recovers its own
exact saved receipt. Native traces are not repacked by this option.

All **242 records and 12 operations** reopened. Restart made no new capture or
send. A 50,000-byte content ceiling refused typed-point publication, retained the
saved capture, and produced no extra sink call. See the
[measurement JSON](measurements/packed-repository-2026-10-20.json).

Fault tests also verify damaged staged bytes, failed publication, uncertain sync,
mixed-format races, corrupt/oversized input, and bounded concurrent reads.
Neutralization checks disable packing, record-hash verification, staged readback
and the concurrency bound separately; the corresponding regression tests fail.

Repository expiry, compact logical point schemas, access policy and Neo adoption
are separate changes. This option changes none of them and starts no service.
