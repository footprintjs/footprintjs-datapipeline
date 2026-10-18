# Changelog

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
