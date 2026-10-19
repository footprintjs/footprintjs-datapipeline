# Changelog

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
