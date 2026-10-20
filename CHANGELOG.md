# Changelog

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

- The new output is opt-in. Existing collect/derive flows, schedules, storage contracts and pinned dependencies are unchanged. No vendor collector, Neo integration, background service, automatic retry or exactly-once delivery is added.

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
