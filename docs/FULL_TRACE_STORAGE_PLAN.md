# Full traces with lossless compression

The offline run/window example retains the complete trace for every recorded
run and window assessment using the existing lossless gzip trace store.
Window summaries are additional indexed evidence; they do not replace run traces.

## Storage contract

| Evidence | Plan |
| --- | --- |
| Run receipts and exact statistical contributions | Keep every recorded run through existing repository APIs. A missing collection remains a gap. |
| Full recorded collection/processing traces | Keep every trace; gzip changes encoding only. No sampling, truncation, age-based expiry or automatic deletion. |
| Full window traces | Keep every requested assessment, including its cutoff, decisions and input references. Late data creates a new assessment. |
| Raw device responses | Separate application permission and retention policy. The example does not expand raw capture or alter archive limits. A trace cannot recover raw bytes that were never saved. |

“Every run” means every execution that actually produced a record, including
recorded failed/partial outcomes; it cannot manufacture a record for a process
that never ran or died before saving. Trace persistence acknowledgements remain
separate from data acknowledgements. Disk exhaustion or a size-limit refusal
must be surfaced as unavailable evidence; a missing file is never proof that no
problem occurred. Existing record, decompression and integrity bounds stay in
force. This plan introduces no guarantee that storage is unlimited or that every
attempt survives a crash.

## Reuse and reader rollout

Configure the existing `createFilesystemTraceStore({ directory, compression:
'gzip' })` for the native window transform and the pipeline's existing trace
store hook. Application writers must retain their own native per-run traces too;
this library example does not enable or replace those writers.

Upgrade **every reader before enabling new compressed writes**. Datapipeline
0.7.0 understands both plain and compressed files. Original canonical bytes
still determine each fingerprint; reopening verifies the original content.
Existing plain files are not rewritten, and the generic store's default stays
uncompressed for compatibility. This is explicit host configuration, not a
library-wide default change. Already compressed files remain readable if new
writes later return to plain encoding, provided readers stay upgraded.

The offline `examples/run-window.ts` configures gzip for every produced trace:
59 collection traces, one native window trace and one outer derive trace. All
59 collection traces reopen with a fresh reader; the native record is reopened
and used by the existing cause proof. The missing scheduled poll remains absent,
not a fabricated sixtieth run. These traces describe the synthetic adapter's
work; they do not claim to record a separate collector's device acquisition.

## Capacity planning, not an approved quota

Estimate storage from the application's measured allocated bytes per operation,
operation rate and chosen retention policy. Include successful, partial and failed
runs, window traces, receipt/index growth, raw archives, journal reserves and backups.
Measure logical bytes and filesystem allocation separately. Compression ratios and
fixed namespace overhead from small fixtures are not production capacity forecasts.

Before deployment, measure write/read time and peak memory at representative row
counts and concurrency. Monitor used/free bytes and persistence failures. A quota
or operator intervention must not silently turn into sampling or deletion. The
plain gzip store implements no automatic cleanup service or retention policy;
optional guarded storage has its own explicit policy and rollout requirements.

## Application boundary

The offline example and this storage guidance start no production service and
change no deployed collector, mapping or raw-archive policy. Library release,
application adoption and deployment remain separate steps.
