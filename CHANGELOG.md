# Changelog

## Unreleased

## [0.11.1] - 2026-11-15

### Changed

- Admit FootPrint 12 alongside the supported FootPrint 11 range, retaining
  Foottrace `^1.11.1`, existing repository formats and public pipeline APIs.
- Qualify both exact public engine floors through normal independent installed
  consumers of the same packed source bytes. Checks include single physical
  engine/record ownership, strict public declarations and replay of the actual
  stored collection/derivation records. Engine tracking/privacy laws remain
  those of the selected engine; stored history is not rewritten.

## [0.11.0] - 2026-11-07

### Changed

- Migration: require `footprintjs ^11.0.0` and `foottrace ^1.11.1` for new
  computations. FootPrint 9/10 runtime cohorts are no longer supported by this
  source. Hosts exchanging charts and native records must share one physical
  engine and record-reader installation with the pipeline. Published 0.10.1
  retains its original FootPrint 10 range; this source change is not a claim
  that the published package has changed.
- Preserve existing repository formats, retained reader fixtures, optional
  feature defaults and public pipeline/storage APIs. Native tracked reads use
  FootPrint 11's deletion rules; stored history is not rewritten or backfilled.

## [0.10.1] - 2026-10-28

### Changed

- `footprintjs` and `foottrace` are now version ranges (`^10.7.1` and `^1.8.0`)
  instead of exact pins, so an app that installs its own copies shares them
  instead of getting a nested second copy of each. The lowest versions accepted
  are now Footprint 10.7.1 and Foottrace 1.8.0. No source change.

## [0.10.0] - 2026-10-26

### Added

- Opt-in verified-uptime expiry for fresh retained namespaces. Recorded elapsed
  age excludes wall-clock corrections and unknown restart downtime.
- Immutable metadata limits for operation counts, reference counts and SQLite
  control files, plus `metadataCapacity()` snapshots. Caps alone preserve exact
  history by refusing new identities when full; payload cleanup alone does not
  recover that capacity.
- Optional daily-period history retirement with `bindPeriod()`, `historyStatus()`
  and `retireHistory()`. Bounded, resumable verification and reclamation preserve
  a permanent retired-prefix fence. Unconfirmed publication stays unconfirmed
  and is counted separately. The seven-day laptop profile is an example policy.
- Stable managed staging for interrupted retries, transactional membership
  counters, next-period scope preparation and committed pending-work health
  observations. Hosts remain responsible for maintenance and backlog monitoring.

### Fixed

- Retained operations wait asynchronously for bounded transaction admission and
  retry a busy final COMMIT. Publication, cleanup and uncertain external writes
  are never replayed automatically.
- Slow payload deletion runs outside the database writer transaction. Expiry is
  durable first; deletion is confirmed only after unlink and synchronization.
- A durable acceptance revision prevents an idle older handle from charging
  pre-acceptance time to a newer record. Acceptance rounds up, expiry rounds down,
  and failed transactions never gain new clock authority.
- Durable initializing/active authority and matching ledger identities distinguish
  recoverable interrupted setup from missing active metadata. Lost active
  authority is refused rather than recreated as empty history.
- Configured metadata limits classify native SQLite FULL consistently during
  setup, activation and normal ledger writes. BUSY and publication uncertainty
  retain their distinct meanings.
- Transaction guards and guarded rollback recovery support Node 22.13, including
  clock failures and SQLite's implicit rollback on FULL.

### Compatibility and rollout

- Existing entry points, ordinary IDs, wall-clock namespaces and defaults remain
  unchanged. Footprint 10.2.1, Foottrace 1.6.0 and Node >=22.13 remain required.
- Fresh verified-age namespaces use format 5; finite-history namespaces use
  format 6. Earlier unpublished verified formats are refused without migration.
  Upgrade all managed readers, publishers and cleanup workers before enabling
  a new profile. Exhaustive health-code handlers must cover the added codes.
- Alternating publishers can repeatedly discard ambiguous elapsed intervals,
  delaying expiry and retirement indefinitely. There is no minimum age-progress
  rate or wall-time capacity guarantee. Historical large-volume measurements
  predate this repair and were not rerun; host-specific capacity validation is
  still required.

### Packaging

- Supporting documents use an explicit file list. Application-specific sizing
  assets and references are excluded from the source and published package.

## [0.9.0] - 2026-10-21

### Added

- Optional retained filesystem repository for new namespaces: explicit retention period, pending intent/recovery, exact packed records, hashed ownership index, bounded read batching, expired references and durable tombstones before bounded cleanup. No automatic policy, timer, migration, repository disk quota or activation.

## [0.8.0] - 2026-10-20

### Added

- Add an opt-in saved run/window evidence transform: exact contribution support, named native completeness decisions, knowledge-cutoff assessments and individual missing-poll gaps. It starts no collector and performs no retention deletion. Existing callers are unchanged.
- Optional, default-off packed filesystem operations, preserving logical records/references and atomic publication. Readers support old and packed operations with bounded decompression, hash verification and no migration. Reader-first rollout and actual synthetic writer measurements are documented; repository retention and application adoption are unchanged.

- Optional guarded gzip trace store with allocated-byte quotas, free-space/receipt headroom, durable pressure gaps and independent health events. Thirty-day trace expiry uses bounded explicit maintenance and preserves small expiry receipts; no collector, timer, archive policy or default activation changes.

- Optional InfluxDB reader and source adapter with registered versioned templates, literal bindings, bounded responses and fixed read diagnostics. Saved query rows use ordinary observations and one database-boundary receipt, with explicit mutable-source and unknown upstream coverage semantics. Database calculation delegation remains application-owned and requires matching tests.

### Changed

- Document full recorded run/window trace retention with opt-in lossless gzip; the offline run/window example retains every produced trace and reopens run and native window evidence. No sampling, deletion, production configuration or generic compression default changes.

- Derivation can use an optional bounded repository bulk-read capability. Filesystem lookup validates each requested record while scanning manifests once per chunk, avoiding repeated manifest scans for large saved windows. Scalar-only ports and existing saved records remain supported.

- Share the existing writer connection validation and credential/body handling with the reader; write behavior and public write contracts are unchanged.

## [0.7.0] - prepared 2026-10-20 (not published; included in 0.8.0)

### Added

- Optional gzip encoding for new filesystem traces. Trace references still fingerprint the original canonical JSON; reopening verifies those bytes and preserves the native record and cause explanations exactly. Plain JSON remains the default, and existing files are not migrated. Upgrade all readers before enabling compressed writes.
- Bounded compressed reads and decompression, a lowerable 16 MiB gzip content ceiling, and fixed corruption/limit errors. No data-store encoding, dependency, retention or collection behavior changes.

## [0.6.0] - 2026-10-19

### Changed

- Pin the published Footprint 10.2.1 and Foottrace 1.6.0. Native window execution now uses Footprint's built-in control snapshot and terminal-failure accessor; both temporary recorder implementations are removed.
- Window control snapshots use the native `control-dependencies` bundle name. Readers upgrading to 0.6.0 must also recognize historical `window-control-dependencies` bundles without rewriting saved records or combining ambiguous duplicates. Public collection, derivation, scheduling and sink contracts are unchanged.

### Fixed

- A contained optional branch error no longer fails an otherwise completed window or steals the category and execution identity of a later terminal failure. Whole-window cancellation keeps priority over earlier branch errors; native pauses, initialization and serialization remain separately classified.

## [0.5.0] - 2026-10-19

### Added

- Optional `footprintjs-datapipeline/influxdb` output: typed line-protocol encoding and bounded, sequential HTTP delivery. Float, signed int64, boolean and string fields are explicit; integer fields and nanosecond timestamps retain their exact supplied values. [PR #6](https://github.com/footprintjs/footprintjs-datapipeline/pull/6).
- Whole-call validation before credentials or HTTP, hard limits, redirect refusal and a deadline covering credential resolution and delivery. Receipts distinguish acknowledged, uncertain and unattempted submissions; only HTTP 204 acknowledges a batch.
- Value-free `rejectedPointIndex` diagnostics for point-local preflight failures. Aggregate limits and other whole-call failures do not blame a point. The host decides admission policy; the writer never drops points or retries automatically. [PR #7](https://github.com/footprintjs/footprintjs-datapipeline/pull/7).

### Documentation

- Documented conditional replay, including the risk of overwriting newer corrections, and a host receipt-storage plan using the existing repository and trace store. Durable intent/result integration remains application work; returned delivery receipts are not automatically persisted.

### Compatibility

- The new output is opt-in. Existing collect/derive flows, schedules, storage contracts and pinned dependencies are unchanged. No vendor collector, application integration, background service, automatic retry or exactly-once delivery is added.

## [0.4.1] - 2026-10-18

### Changed

- Refactored the collection-window runner into focused internal modules and reused Foottrace's public record freezer. The public API, behavior, error codes, evidence limits and pinned dependencies are unchanged. [PR #4](https://github.com/footprintjs/footprintjs-datapipeline/pull/4).

### Implementation notes

The window runner's
implementation is split under `src/window/` by validation, deadlines, execution,
recording and persistence; its public API, error codes and evidence limits stay
unchanged. Record freezing delegates to the public `deepFreeze` export from
`foottrace/write`. It receives only freshly parsed ordinary JSON, so the
freezer's documented exceptions for already-frozen or unsealable values do not
apply at this boundary.

The temporary control snapshot bridge tracks [Footprint issue 108](https://github.com/footprintjs/footPrint/issues/108).
The safe failure observer tracks [Footprint issue 109](https://github.com/footprintjs/footPrint/issues/109)
and can be removed after a compatible native terminal-failure API is released.
Neither follow-up changes the pinned engine or record-library versions.
