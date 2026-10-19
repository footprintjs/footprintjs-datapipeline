# Host receipt storage plan

This is the integration plan for the first PowerScale shadow writer, using the
existing datapipeline contracts. It is not persistence implemented by the Influx
sink, a new generic record format, or a production cutover. Python continues to
collect independently; Neo's original flow and mapping settings stay unchanged.

## Storage and order

Use the existing filesystem `Repository` for durable `Observation`, `Batch` and
`Evidence` records, and the existing `TraceStore` for native Footprint records.
The host owns an acquisition namespace, access checks and retention, independently
of a conversation's life. Do not create another receipt database or overwrite
published observations. Use versioned application schemas for these run facts:

| Record | What it retains |
|---|---|
| Run start | Stable caller-owned run ID, source/config revision, collection start time |
| Capture | Original admitted response and digest; start/end, success/empty/partial/timeout/error, row counts and fixed failure category |
| Selected samples | Exact source response reference, digest and row pointer; selection/drop reasons |
| Typed points | One observation per emitted point, pinned mapper revision, exact timestamp and exact sample `inputRefs` |
| Publish intent | Attempt ID, ordered point-observation references and manifest digest, encoder revision and public destination-policy reference |
| Attempt result | The exact returned `InfluxWriteReceipt` and its intent reference |
| Run summary | References to capture, mapping gaps, manifests and attempts, with distinct collection and delivery coverage |

Commit run-start before polling. Capture, point-manifest and publish-intent
publication must be acknowledged before dispatch. If evidence publication is
failed, partial or unknown, send no Influx
request; reconcile storage first. The capture's failed/empty outcomes are facts
too. Absence of samples never becomes a zero measurement.

Build capture, selection, point mapping, admission decisions, intent publication,
delivery and result publication as named Footprint steps. Policies use native
decisions and each point/claim reads only its exact inputs. Keep pure point mapping
in `derive()`. A coverage point names its response/selection inputs, rather than
inventing a source row. Preserve the existing Python cleanup, selection, types
and timestamp meaning, including known bugs until separately approved.

Persist local run events through a small versioned application `SourceAdapter`:
the source is the host's immutable event snapshot, not the storage device.
Use stable requests such as `{ runId, stage, attemptId }` so `reconcileCollect()`
can look up their receipts after restart without collecting or sending anything.
Outcomes come from trusted host state, never model-supplied request values.
Do not duplicate private core hash/reference formulas. A stable run locator is
required; random-only run IDs without a recovery locator are insufficient.

## Links, counts and gaps

An attempt's returned HTTP batch index/count identifies a contiguous range in
its immutable ordered point manifest. Counts describe submissions, not unique
stored points. A `rejectedPointIndex` resolves within that call's manifest to the
point and exact source support: original rows for client points, response/selection
evidence for coverage points. No values go in diagnostics. If the
host's admission policy permits omission, record the gap and create a new
manifest for the remaining points; never silently erase the rejected observation.
Required coverage-point failure must not become a falsely successful run.

Keep credentials and secret connection details out of every record. Store only
a public destination-policy reference. Apply the existing evidence access and
value-hiding rules to source bodies, point payloads and relationships. Expired
evidence is an explicit gap; a receipt does not grant permission or establish
physical cause. Bound manifests/observations to existing repository and native
record limits; overflow is recorded, never a silently truncated write.

## Failure and recovery rules

- Before dispatch, unconfirmed capture/manifest/intent publication blocks writing.
- After dispatch, return the observed delivery result and receipt-persistence
  outcome separately; the repository commit receipt reports the latter. An HTTP
  204 cannot become `not-written` because saving its
  receipt failed. Stop further writes; retain committed source and latency facts.
- A durable intent with no durable terminal result after a crash means the whole
  attempt **may have written**, so delivery is unknown. Do not infer zero writes
  or retry automatically. The sink returns sub-batch receipts after its whole
  call: any acknowledgements only held in memory cannot be recovered.
- Trace-store failure is separate: a missing Lens trace does not erase committed
  source or receipt observations.
- An explicitly authorized retry gets a new attempt ID and receipt, referring to
  the same immutable manifest. Preserve timestamps, destination, payload and
  order, and prevent overwriting newer corrections; see the sink's replay rules.

Per-HTTP-request durable checkpoints would need a later generic lifecycle hook.
Do not copy the sink's partitioning or HTTP loop into the application to fake it.
Repository durability, retention, disk quotas, service ownership and restart
recovery still need proof in the independent-service phase before production.

## Tests required in the application writer PR

Saved-response parity must cover Python cleanup and field types; every admitted
point resolves to exact source support: original rows for client points, and
response/selection evidence for coverage points. Inject capture/intent persistence failure and
prove zero dispatches. Inject post-dispatch result/trace persistence failures and
prove partial truth survives. Restart with a saved intent but no result and prove
delivery stays unknown, without dispatch. Test rejected-point coverage, manifest
rebasing and limits, duplicate identities, exact-payload replay, and unchanged
Python/original-flow/mapping behavior. These application tests are not supplied
by this library follow-up.
