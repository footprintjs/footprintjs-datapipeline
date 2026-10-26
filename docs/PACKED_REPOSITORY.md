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

## Validation and capacity planning

The self-contained tests use synthetic library records. Packed repository tests
compare reopened records and references with the same logical input; publication
fault tests cover damaged staged bytes, failed publication, uncertain sync and
mixed-format races. Read tests exercise corrupt/oversized input, bounded concurrent
inflation, caller-owned receipts and reopening without a completed-result cache.

Run them from this checkout:

```sh
npm test -- test/packed-repository.test.ts test/packed-publication.test.ts test/packed-reads.test.ts test/packed-reader-integration.test.ts
```

These tests verify storage behavior, not a particular application's collection
rate or disk requirement. Measure representative successful, partial and failed
operations on the intended filesystem. Record logical and allocated bytes per
operation, file counts, read/write latency and peak memory. Include native traces,
metadata, journal space, raw archives and backups in the total storage budget.
Small-fixture compression ratios and fixed startup costs are not daily forecasts.

Repository expiry, compact logical point schemas, access policy and application
adoption are separate changes. This option changes none of them and starts no service.
