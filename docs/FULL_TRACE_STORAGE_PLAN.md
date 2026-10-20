# Full traces with lossless compression

Owner decision, 2026-10-20: retain the complete trace for every recorded run and
window assessment, using the existing lossless gzip trace store. This supersedes
the earlier proposal to retain representative runs or only exceptional traces.
Window summaries are additional indexed evidence; they do not replace run traces.

## Storage contract

| Evidence | Plan |
| --- | --- |
| Run receipts and exact statistical contributions | Keep every recorded run through existing repository APIs. A missing collection remains a gap. |
| Full recorded collection/processing traces | Keep every trace; gzip changes encoding only. No sampling, truncation, age-based expiry or automatic deletion. |
| Full window traces | Keep every requested assessment, including its cutoff, decisions and input references. Late data creates a new assessment. |
| Raw device responses | Separate archive permission and policy; this decision does not expand raw capture or remove its existing 24-hour ceiling. A trace cannot recover raw bytes that were never saved. |

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
store hook. PowerScale writers must retain their own native per-run traces too;
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
work; they do not claim to record Python's original device acquisition.

## Capacity planning, not an approved quota

The reported workload is 14 clusters, client polls every 5 seconds and full
performance every 5 minutes. This gives 241,920 scheduled client polls and 4,032
five-minute fleet windows per day. The operator reports that repository and
deployed scripts match; this has not been independently inspected. Top-N,
sentinel-ID and page-limit settings are still needed for representative sizing.

A previously measured **0.63 MB compressed client trace** multiplied by that
poll count is **152.41 GB/day of traces alone** (decimal units), approximately
4.57 TB per 30 days. This is an illustration for that synthetic profile, not a
production forecast. Real trace size depends on retained clients and settings.
Receipts, inputs, window traces, raw archives, filesystem allocation, indexes
and backups add storage. Compression ratios must be measured, never assumed.

Before deployment, measure compressed bytes/run and bytes/day across real row
counts and failure cases, plus write/read time and peak memory. Monitor used and
free bytes and alert on persistence failures. Capacity increases or operator
intervention must not silently turn into sampling or deletion. No automatic
cleanup service or quota policy is implemented by this PR.

## Scope of this PR

Default-off run/window foundation, documented storage policy and offline proof
only. No production service starts; no deployed setting, Python collection,
Neo original flow, mapping flag, raw-archive policy or paid model use changes.
The PR remains open for independent review. Library release, application
adoption and deployment remain separate steps.
