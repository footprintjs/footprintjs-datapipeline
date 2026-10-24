# Finite-history review: workload and measurement contract

This revision addresses whole-day write-lock scans and indefinite blocking by
unconfirmed saves. Small native cause/expiry/crash fixtures remain separate from
metadata scale and from real collector capacity. No production deployment is
measured or enabled.

## Reproduce

```sh
npm run check
node --import tsx examples/measure-laptop-history.ts 50 32 32
node --import tsx examples/measure-laptop-history.ts 256 64 32
node --import tsx examples/measure-history-volume.ts 241920 10 3
node --import tsx examples/measure-history-volume.ts 241920 1 4
```

Run the large profiles sequentially on a shared laptop. Each uses actual SQLite,
seven verified-day history and hard row/page
allowances and a 2 GiB real-free-space stop. The three-reference run spans ten
periods with eight kept simultaneously. The four-reference run checks one retired
period and does not measure its eight-period plateau. Bulk insertion is fixture setup, not
production throughput. Setup uses 10,000-row transactions and an 8 MiB cache
request on its own connection. With spill disabled, dirty transaction pages can
exceed that cache; reported RSS includes setup and is not a production memory
benchmark. The repository and publisher keep their original connection settings.
Each old period's files-gone states are explicitly seeded;
this benchmark does not delete millions of native payload files. One in every
1,000 intents is unconfirmed, and remains unconfirmed in the retired counters.

During first retirement, another OS process publishes actual native records at
350 ms spacing. Its failures, save durations and alert codes are reported. This
is a database overlap probe, not 14 real devices or a fleet-capacity pass.
Transaction timing begins after successful `BEGIN IMMEDIATE` and ends after
successful `COMMIT`, including the elapsed-age checkpoint. Bulk setup and seeded
expiry updates are excluded from that lock-duration series.

## Interpreting size

The original 2.09 / 10.16 MB plateaus described eight operations per day. They
never sized 241,920 operations per day. At the latter rate, exact metadata is
GB-scale. Count raw and compact stores separately: a single saved response uses
three references; a receipt plus one watched series uses four. More series add
memberships. Device response bytes, compact payloads, compressed traces, filesystem
directory allocation and the two-store host total are additional to SQLite size.

The 12 GiB metadata allowance in each scale fixture is a maximum that reserves
main-file and rollback space; it is not preallocated or an approved host budget.
The actual daily/high-water sizes and lock-duration results are emitted as JSON.
Do not treat an incomplete/free-space-stopped run as a plateau or a capacity pass.

## Maintenance is required for availability

A pass is bounded to 100 operations and 1,600 references, or a lower caller limit,
with a cooperative 10 ms verification/reclamation work target. The worker yields
outside the transaction. Results report remaining work; the host must keep draining
it, separately from polls. Whole-period retirement waits for all verification,
but each transaction releases the lock and its cursor survives a crash.

A single pass every 35 seconds does not prove sufficient maintenance: payload
expiry, verification and row reclamation all need to keep up. The operation,
reference and page caps still bound allocation if maintenance stops, but then
new work is refused with an alert. These PRs install no timer, worker cadence,
production policy or service. Review/adoption and sustained poll/cleanup/summary
capacity on the actual host remain activation gates.

## Completed measurements

Node 24.21.0, local APFS, synthetic metadata at 241,920 operations per day:

| Profile | Retired operations | Of which unconfirmed, deleted | Longest write lock | Concurrent saves |
| --- | ---: | ---: | ---: | ---: |
| 3 references, 10 seeded periods | 483,840 | 484 | 581.00 ms | 436/436, 0 failed |
| 4 references, 1 seeded periods | 241,920 | 242 | 27.03 ms | 235/235, 0 failed |

The raw profile retains 1,935,360 seeded operations (eight periods), plus 436
native probe saves. Its metadata high-water observations were:

| Current period | Live operations | Main file, logical bytes | Control files, allocated bytes |
| --- | ---: | ---: | ---: |
| 7 | 1,935,360 | 3,150,508,032 | 3,154,579,456 |
| 8 | 1,935,796 | 3,193,253,888 | 3,204,911,104 |
| 9 | 1,935,796 | 3,219,165,184 | 3,221,688,320 |

Live membership remains constant across retirement/repopulation; file high-water
still warms by about 2% across these last observations. This is not a claim of a
perfect byte plateau from three samples. Actual row reclamation and hard
row/page caps provide the allocation bound; a sufficiently draining worker is
still necessary for continued admission.

The compact profile's one-period main file was
483,811,328 bytes. Its eight-period high-water was
**not** measured. More watched series add references; do not apply this one-series
profile to all sixteen. Payloads and traces are additional.

The bulk-seeding process's maximum sampled RSS was
1,134,739,456 bytes in the raw profile. This includes dirty setup
pages and is not a steady-state production memory result. Runtime cleanup,
connection settings and the one-second retry window were not relaxed.

A prior twelve-period attempt was interrupted before its final JSON was saved.
Its partial logs are excluded from the completed results above. The completed
rerun uses ten periods and has an explicit success exit.

[Machine-readable summary](repository-history-review-2026-10-23.json) records the
commands, code snapshot, exact counts, checkpoints, per-period sizes, probes and
source-report hashes. The small 32-day native-record tests separately protect
cause replay and missing-poll gaps.

## Native records and allocation warm-up

The revised native fixture measured 50 rows over 32 days and 256 rows over 64
(days contain eight publications). Allocated bytes were flat in their late
halves: 3,059,712 B and 12,480,512 B respectively. The largest case covers 512
native publications and four full late retention cycles; 64 operations and
16,512 references remain exact, with 58 files. Its tight 128 KiB late-allocation
range assertion is unchanged and independently recalculated from samples.

An initial 256-row, 32-day run **failed** that assertion: the late allocation
range was 1,048,576 B. Its saved breakdown shows fixed live membership and
operation-file allocation, with a 1 MiB APFS allocation extent added at day 21
as SQLite's larger covering index warmed. Logical total growth over days 16–31
was about 115 KB. Rather than increasing budgets or retry/test timeouts, the
largest fixture was extended to 64 days, with the same strict range check over
the final 32. The failed diagnostic is kept in the local validation evidence.
Nine deliberate regressions of uncertainty, deletion proof, cursor progress,
operation/reference bounds, live counts, reclamation and file-check ordering were
all caught; sources were restored before final validation.
