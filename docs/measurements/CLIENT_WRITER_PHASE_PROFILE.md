# Isolated client-writer phase profile

This is a local synthetic measurement, not a capacity pass or a real-cluster result. It follows the failed 14-cluster capacity test retained in Neo #43. No production schedule, limit, writer activation, Python collector or mapping setting changes.

## Method

Twelve fresh processes: three trials per combination of 50/256 source rows and published/candidate trace storage, alternating the implementations. Each process runs Neo's actual inactive client writer at commit `0a24b36807a0b62999f6408819f8fdfa24d9a727`, top-N 50 and sentinel 25, with synthetic saved responses and a synthetic HTTP 204 sink. Published trace storage is datapipeline 0.8.0; the candidate implementation is commit `6ab38802d3742903bcd063769ee2381042df7dea`. Both use the same retained repository implementation, settings and unchanged 2 GB Node heap ceiling. Fixture generation, namespace setup and Response initialization precede timing. No other test suite ran alongside these trials.

The diagnostic harness temporarily instruments the actual ESM Footprint executor and native compression/file operations in its own process. It never installs a candidate package in Neo. CPU sampling is at 1 ms. Probes add overhead; native compression-worker CPU is not attributed by the main-thread CPU profile. Filesystem probes exclude SQLite's internal I/O. Peak RSS is process high-water memory captured before peer replay, including module startup. These are medians of three trials, not confidence intervals or production promises.

## Results

All durations below are milliseconds. Columns are **inclusive and overlap**; do not sum them. In particular, trace storage includes compression and filesystem operations, and repository reads can overlap.

| Source rows | Trace store | Whole writer | Evidence chart | Record preparation | Trace preparation/compression/save | Gzip across all writes | File sync across all writes | Peak RSS, decimal MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 50 | Published | 302.03 | 30.02 | 23.22 | 121.85 | 12.00 | 77.73 | 325.6 |
| 50 | Candidate | 234.62 | 29.00 | 23.26 | 62.23 | 7.44 | 67.62 | 289.2 |
| 256 | Published | 1819.94 | 176.41 | 120.38 | 1319.27 | 47.56 | 75.76 | 1436.3 |
| 256 | Candidate | 1000.79 | 178.82 | 122.87 | 486.99 | 26.97 | 79.87 | 1310.0 |

The evidence chart is the nested per-point mapping flow, not the outer delivery orchestration. Record preparation measures the handoff from the chart completing to its first trace-store call, including snapshot/serialization/freezing. Synthetic fetch medians were 0.026–0.030 ms; typed-point repository commits were 18.31–38.08 ms; sink encoding plus synthetic acknowledgement was 1.48–4.26 ms. Those fetch/send numbers say nothing about real device or InfluxDB network latency.

The full per-trial phase totals, memory, source/probe hashes and original report hashes are in [client-writer-phase-profile.json](client-writer-phase-profile.json). Main-thread profiles identify repeated canonical traversal and allocation during guarded trace storage; the measured trace-store interval is the largest phase at 256 rows.

## Targeted change

The guarded store formerly canonicalized a full trace, parsed a clone, canonicalized it again for its fingerprint, compressed for reservation, then asked the basic store to canonicalize and compress it again for publication. The candidate captures canonical bytes once before awaiting, derives the same fingerprint from those bytes, and compresses once. Reservation and durable publication use the very same compressed buffer. Shared private publication helpers retain the existing staging, file synchronization, rename and directory synchronization sequence. Existing-file validation, including corrupt orphan refusal, remains in place.

This is reuse of validated bytes, not reduced evidence. No sampling, grouping, new format, weaker validation, changed budget or larger timeout was introduced. Each run still saves both complete traces.

## Evidence equivalence and regression checks

All 12 runs were acknowledged. After measured resources were captured, every saved trace was reopened through the other implementation: **24 equal references and records, and 24 equal cause answers**. Cause comparison uses Neo's existing test-helper limits; it does not prove completeness under the cause tool's default limits. The raw private CPU profiles and event reports remain outside the repository; only synthetic aggregate results are committed.

New unit tests require exactly one full-trace canonicalization and one compression, verify exact canonical bytes/fingerprints on reopen, preserve the snapshot against caller mutation, and refuse a corrupt orphan without replacing it. Deliberately adding a second canonicalization or compression separately makes the regression test fail. The full library suite passes: 446 tests, with nine existing optional integration tests skipped.

## Reproduce

From a built datapipeline checkout, with a Neo package checkout and its published 0.8.0 dependencies installed:

```sh
node --max-old-space-size=2048 --import tsx docs/measurements/profile-client-writer.mts /absolute/neo/packages/powerscale-datapipeline /fresh/private/output 50 published
```

Repeat with `candidate`, and with `256`. Use a fresh output directory and process every time. Alternate published/candidate and run no other suite concurrently. The script asserts its baseline version, actual ESM/native probes, delivery and replay equality. It writes private `profile.json` and `cpu.cpuprofile` files; source and harness hashes identify the measured implementation.

## Remaining capacity work

This fixes the largest measured isolated hotspot first. It **does not** establish that 14 clusters can keep up, and 256-row peak RSS is still about 1.31 GB. Keep the failed capacity result as the baseline; do not increase its memory ceiling or deadlines.

Separate small changes remain: a compact per-poll path, explicit per-window/unusual-run evidence policy, fixed source offsets, and a separate summary worker. A compact poll without per-point execution steps has less execution-history detail; it must not pretend that a later assessment reconstructs the original execution. Saved inputs, policy version, recorded decisions, event time, learned time and explicit missing evidence must remain traceable. This policy change is distinct from this lossless storage fix.

Repeat the capacity test after those changes, then run on the actual collector host for at least one hour: sustained 14 sources every five seconds, 95% finishing within five seconds, flat memory and no missed collections. No configured collector-host target is available in this workspace, so that host test has not run. Everything remains inactive.
