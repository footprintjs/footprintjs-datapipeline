# Run/window timing review

Measured locally on 2026-10-20, Node 24.21.0, macOS arm64, using the actual
filesystem repository and native Footprint record. Synthetic public inputs;
no device, database, model or application collector was contacted.

## What was measured

`test/run-window.test.ts`'s 64-slot capacity case contains **128 observations**
(one receipt and one contribution per slot), not 8,192. Its explicit 30-second
budget includes creating and durably publishing all fixture inputs. There is no
elapsed-time assertion or retry. The full scan limit was measured separately:
64 slots, each containing one receipt and 127 series contributions, totaling
**8,192 observations**, with **one declared series summarized**. This is not a
claim to summarize 127 series simultaneously or an arbitrary maximum-byte input.

Run the reproducible benchmark from a built checkout with development tools:

```sh
node --import tsx examples/run-window-timing.ts /tmp/window-timing.json
```

It measures fixture ingestion separately, then three distinct derivations from
those saved inputs. Component timings reopen inputs outside their timed section;
end-to-end timings include repository loading, validation, transformation and
saving. The first derivation is after ingestion, not a cold operating-system
cache. These are three sequential trials on one laptop, not a fleet capacity or
production latency guarantee.

## Results

| Profile | Fixture ingestion | Derive trial 1 | Trial 2 | Trial 3 |
| --- | ---: | ---: | ---: | ---: |
| 64 slots / 128 observations, before bulk reads | 2,171 ms | 820 ms | 838 ms | 802 ms |
| 64 slots / 128 observations, bounded bulk reads | 2,173 ms | 133 ms | 108 ms | 103 ms |
| 64 slots / 8,192 observations, bounded bulk reads | 89,265 ms | 1,317 ms | 1,260 ms | 1,237 ms |

The initial 8,192-observation scalar-read run was interrupted; it has **no
completed baseline timing**. The before/after comparison is only for the
128-observation profile.

At 8,192 observations, the end-to-end breakdown was:

- Repository reads: 698–765 ms, for 8,256 logical references (64 batches plus observations).
- Admission after loading: 27–31 ms.
- Native flow execution: 8–9 ms.
- Native record serialization, parsing and freezing: 7–8 ms.
- Result publication: 69–71 ms; native trace saving: 25–28 ms.
- Native component record: 806,387 bytes, below the 8 MiB limit.

Components are separate trials and do not sum exactly to end-to-end timings;
those also include cloning, validation and outer pipeline recording.

## Fix and integrity boundary

Scalar filesystem reads previously rescanned every operation manifest for every
reference. Derivation now uses an **optional** repository `readMany` capability,
in requests of at most 256 references; scalar-only repositories remain supported.
The filesystem implementation scans manifests once per request and reads at
most eight record files concurrently. There is no persistent approval cache.
Every requested copy is still hash checked, conflicting copies are rejected,
and input order, missing entries and duplicate references are preserved. A
short bulk reply is refused, never accepted as a partial input set. Cancellation
prevents scheduling subsequent chunks; it does not cancel already-started I/O.

Tests compare scalar and bulk summary values and exact support references,
verify fresh tamper checks and conflicting-copy rejection, assert one manifest
scan per bulk request, and cover bounds, cancellation and malformed bulk replies.
The native cause and missing-poll tests remain unchanged. No slot, observation,
record-size or production deadline limit was raised.
