# Opt-in trace budget and thirty-day retention

This is a local-host `TraceStore`, not a collector service or a distributed
storage coordinator. Existing stores, Python, Neo's original flow and mapping
are unchanged. Nothing starts a timer or performs cleanup unless the host
explicitly calls it. It uses existing gzip trace files and Node's built-in
SQLite for small receipts/accounting; no new dependency or trace graph format.

## Host integration

```ts
import { createGuardedFilesystemTraceStore } from 'footprintjs-datapipeline/filesystem';
const store = createGuardedFilesystemTraceStore({
  enabled: configuration.traceStorageEnabled, // omitted/false: undefined, no I/O
  directory: configuration.evidenceDirectory, // private, new namespace on /data
  maxTraceBytesOnDisk: configuration.traceBudgetBytes,
  warningAtBytes: configuration.traceWarningBytes,
  minFreeBytes: configuration.filesystemHeadroomBytes,
  receiptReserveBytes: configuration.receiptReserveBytes, // at least 16 MiB
  onHealth: event => independentHealthChannel.report(event),
});
// Existing pipeline and native writer/transform traceStore hooks accept store.
// The host's maintenance lifecycle must call bounded cleanup repeatedly:
if (store) await store.sweep({ limit: 100 });
// On shutdown, after in-flight work finishes: store?.close().
```

No example quota is a recommended production budget. `/data` also contains
InfluxDB: its growth needs its own headroom. By default, the directory must be
on a different filesystem/device from `/`; root paths, symlink paths and
non-private directories are refused before trace writes. Use the canonical
absolute path. `requireSeparateFilesystem: false` is an explicit host exception
used only for synthetic local tests here. Upgrade all readers before adoption.

## Pressure and failure contract

- Gzip is lossless; references still hash the original canonical JSON. There is
  no sampling or silent truncation. Existing 16 MiB content bounds remain.
- Charge actual allocated trace-file blocks, with a conservative reservation
  before each write. SQLite transactions share reservations across instances;
  a concurrent conflicting metadata transaction fails closed rather than waiting.
  Only one in-flight operation is accepted per store instance, bounding memory.
- Stop admitting full traces when their budget would be exceeded, or free space
  would consume the configured headroom plus receipt reserve and pending writes.
  Save a small fixed-code gap, alert, and throw a typed error. `receipt(ref)`
  exposes the gap; `read(ref)` reports its reason instead of ordinary absence.
- A failure after reserving leaves a **pending/unconfirmed** receipt and charged
  bytes. It is not retried or acknowledged automatically after restart. Pending
  entries require operator investigation; expiry can later remove their own
  trace file. This favors conservative accounting over filling the filesystem.
- Warning/pressure/error health events contain fixed codes, IDs and receipt
  outcome only. Callback failures cannot authorize a write or leak private text.
  If metadata persistence also fails, independent health reports
  `receiptSaved: false`; receipt durability is never promised under total disk
  exhaustion. The reserve is not preallocated and cannot constrain other apps.
- Existing pipeline behavior keeps committed data when optional trace saving
  fails. A focused test proves the data acknowledgement remains committed and
  trace outcome becomes unavailable. Production collection still needs host
  supervision; this feature alone cannot guarantee uptime or reserve space from
  an unrelated InfluxDB writer.

The trace budget excludes receipts, SQLite/WAL, the data repository, raw archives,
other processes and backups. Those require a total-volume budget and monitoring.
No automatic deletion is used to make room before expiry. New namespaces only:
unmanaged old files are refused rather than assigned invented retention dates.
Persisted quota settings cannot silently change on a second instance.

## Thirty days and explicit expiry

Retention starts at the host's saved acceptance intent, using the trusted host
clock, not a source-supplied timestamp. Duplicate writes, retries and restarts do
not extend it. At exactly thirty days reads return `TRACE_EXPIRED`, even before
maintenance physically deletes the file. `sweep({limit})` processes at most 100
expired files, durably marks each expired before unlinking, and retains its small
receipt. Interrupted cleanup stays charged and can be safely called again. It
never follows trace symlinks or deletes a different namespace.

This store expires **full trace files only**. It does not expire repository
observations or operate Rahul's optional Python raw-response archives. His
30-day raw-retention request remains an application/archive integration step;
the existing Python archive's 24-hour cap is unchanged. Do not deploy believing
this PR implements the entire evidence retention service.

## Validation and rollout gates

Tests cover off/default behavior, pressure gaps, quota warnings, concurrent
reservations, immutable reopening, exact expiry, duplicate writes, restart,
failed write/deletion recovery, symlinks, namespace/policy checks, independent
health failure and preserved collection acknowledgements. Mutating quota,
free-space admission or thirty-day expiry makes its regression test fail.

The [PowerScale sizing experiment](POWERSCALE_STORAGE_MEASUREMENT.md) uses reported
settings but synthetic responses. It reveals a top-N compatibility blocker and
capacity far above current free space. Deployment still requires top-N support,
confirmed page-limit semantics, representative saved responses, sustainable
write-rate measurements, sufficient capacity, host supervision and archive/data
retention integration. No device access, service rollout or mapping activation
is authorized by this PR.
