# PowerScale storage sizing experiment, 2026-10-20

**LOCAL SEED**, synthetic saved responses, simulated InfluxDB 204. No real source,
database, credentials, Python change or deployment. Reported workload:
14 clusters, client polls every 5 seconds (241,920 fleet polls/day), top-N 50,
sentinel setting `25`, reported page limit 100. Python splits sentinel settings
on commas, so this experiment uses the single sentinel string `25`, preserving
its existing substring-matching semantics.

## Compatibility blockers found before measurement

The current Node writer accepts top-N **at most 25**; its actual public call with
50 was refused. The experiment ran an isolated copy with only
`MAX_CLIENT_TOP_N` raised from 25 to 50, leaving the product checkout unchanged.
That is an unreviewed capacity candidate, **not a deployable writer**. The real
source must gain explicit compatibility/capacity tests in a separate PR.

This repository's Python performance script does not reference the reported
`POWERSCALE_PAGE_LIMIT` setting. The experiment does not pretend that 100 means
100 clients or that it reproduces 100 pages. Deployed setting semantics still
need confirmation. No original deployed response was provided, so this cannot
be called a measured production storage-per-day figure.

Current Neo main was also checked: it retains the top-N-25 admission cap, and
the current Python file does not reference `POWERSCALE_PAGE_LIMIT`. Its Git blob
is `39c6eec106bd286149a219eb29ac92d5a9f9bda8`.

## Method and measured bytes

Ran the actual client writer with fixed synthetic responses of 50 and 256 rows,
using deterministic test addresses and padded names. Four existing selectors
keep the stable top-N union. Saved all actual repository files, each full trace
both plain and gzip; compared reopened JSON and canonical fingerprints for all
four traces. Plain copies are measurement controls, excluded from forecasts.
The data category includes the captured response, receipts, typed points and
manifest/journal evidence. The new trace-guard SQLite metadata is not included.

| Returned rows | Retained rows | Data JSON bytes/run | Gzip traces bytes/run | Plain trace bytes/run | Reopened traces |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 | 50 | 202,243 | 50,578 | 1,268,502 | 2 |
| 256 | 154 | 618,011 | 217,193 | 6,194,230 | 2 |

Top-N 50 is applied across four selectors; it is **not a cap of 50 stored rows**.
These are two content profiles, not observed production row-count bounds.

| Illustration at 241,920 polls/day | 50 returned rows | 256 returned rows |
| --- | ---: | ---: |
| Saved data + gzip traces, decimal GB/day | 61.16 | 202.05 |
| Same content over 30 days, decimal TB | 1.83 | 6.06 |
| Actual filesystem blocks extrapolated, decimal GB/day | 100.08 | 287.36 |

Allocation is filesystem-specific and is not additive to logical content: it is
an alternate measurement of those same files. These illustrations exclude new
guard metadata, window summaries, other collectors, indexes outside the writer,
backups and ongoing InfluxDB growth. Linear extrapolation does not demonstrate
sustained write throughput. Do not multiply compression into these figures
again: gzip trace sizes are **already compressed**.

## Capacity conclusion

The reported `/data` volume has 147G free, shared with a 593G Influx engine.
Neither measured profile is supportable for a 30-day rollout in that free space.
Even dividing all 147 reported GB by 30 allows only 4.9 reported GB/day before
headroom or database growth. Actual daily use requires representative saved
responses and deployed row counts; disk expansion alone is not yet a sizing plan.

The guarded store is therefore default-off and requires explicit quotas,
headroom and independent health. Pausing trace saves creates explicit gaps; it
is not a claim of complete evidence retention. Python keeps collecting, mapping
stays off, and this PR does not approve production activation.

## Reproduce offline

The [recorded report](measurements/powerscale-top50-2026-10-20.json) and
[measurement script](measurements/measure-powerscale-storage.mts) are included.
From a source checkout with development dependencies, run:

```sh
node --import tsx docs/measurements/measure-powerscale-storage.mts /path/to/neo/packages/powerscale-datapipeline /tmp/top50-report.json
```

The Neo package must already have its dependencies installed. The script copies
only its source to a temporary directory, changes the one admission constant
there, and removes its own temporary output afterward. It never edits the
original package or contacts a real endpoint. Current-source changes may make
it refuse rather than silently produce a different benchmark.
