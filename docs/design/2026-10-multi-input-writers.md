# Saved multi-input writer derivations

Status: design for review; no API, writer, collection or production policy is implemented here.
Baseline: datapipeline 0.9.0, main `b550436`; the motivating adapter is Neo PowerScale heat.

## Problem and ownership

A typed output may depend on several captured responses, lookup facts and recorded failures.
The application must save those inputs, name the exact support, and publish its send intent
before dispatch. Putting a lookup in configuration does not make it a verified device capture.
This proposal keeps one record format and uses Footprint steps for every material decision.
It adds no graph, walker, database, scheduler, source discovery or automatic retries.

| Already built | Location and boundary |
| --- | --- |
| Multiple saved input batches | `DeriveRequest.inputBatchRefs` in `src/contracts.ts`; not a new engine feature |
| Window selection and foreign-reference refusal | `src/createPipeline.ts`; only selected input references may support an output |
| Pure transform over loaded inputs | `Transform.apply(inputs, request, context)` in `src/ports.ts` |
| Named resolve/apply/prepare/publish steps | `src/flows/derive.ts` |
| Native per-write and decision evidence | `runWindowFlow`, documented in `README.md` |
| Typed Influx points and uncertain delivery | `/influxdb`; no automatic retry or record persistence inside the sink |
| Repository-backed event facade and save-before-send | Neo `performanceWriter/WriterJournal`; ordinary repository records, not a second durable store |
| Packing, compression and explicit expiry | Existing repository and trace stores; no ancestor pinning |

The immediate limitation is Neo's facade: `performanceWriter/contracts.ts` takes one capture,
and `performanceWriter/transform.ts` assigns every point only that capture's `inputRefs`.
Generic `derive()` already accepts several batches. Extending the facade must not duplicate it.

First adopt the shared [attempt/intent ownership decision](2026-10-operation-ownership.md).
The existing repository/pipeline port is the one owner. #21 drops its proposed fleet journal
and cursor; this design reuses the same host-event save/lookup pattern already wrapped by
Neo's writer. No writer- or fleet-specific database may override committed repository facts.

The shared decision also gates this facade on real store admission under
cleanup contention (#25, unmerged/unreleased). A refused manifest or send-intent
save emits independent health and blocks dispatch; it never moves the same
attempt to a private writer journal. Request/manifest bytes and target-window
selection remain core-owned rules, tested before any adapter may use them.

## Options and recommendation

1. Put all lookups in writer configuration. Smallest code change, but the lookup remains
   host-supplied metadata, uses policy-size budgets and cannot claim device-response lineage.
2. Build a second join/provenance subsystem. Rejected: duplicates existing derivation and traces.
3. Assemble a bounded saved-input manifest, then call existing `derive()`. Recommended.

Use an opt-in composition around existing contracts, first proven as an adapter integration.
A manifest describes roles and membership; it does not contain copied response bodies.
Role names, compatibility rules, required claims and vendor meanings belong to the adapter.
The first implementation should export no new writer engine. One library rule must come first:
opt-in core input classification/window admission, so an adapter cannot accidentally include
out-of-window measurements. That is a bounded addition to existing `derive`, not another join
engine. Other generic writer helpers wait until a second adapter can reuse them unchanged.

## Types-only sketch

These proposed integration types are not current exports. Reuse existing reference, time,
JSON and shared requirement types; do not create another reason-code list.

```ts
interface SavedInputMember {
  readonly role: string;                 // fixed, adapter-declared role
  readonly inputClass: 'measurement' | 'reference';
  readonly batchRef: BatchRef;
  readonly observationRefs: readonly ObservationRef[];
  readonly learnedAt: string;            // host-recorded knowledge time
}
interface SavedInputManifest {
  readonly schemaVersion: 1;
  readonly policyRef: VersionedRef;
  readonly knowledgeCutoff: string;
  readonly targetWindow: DeriveWindow;   // must equal DeriveRequest.window
  readonly referenceWindow: DeriveWindow; // bounded lookup read envelope
  readonly members: readonly SavedInputMember[];
  readonly gapRefs: readonly ObservationRef[]; // saved requirement/attempt facts
}
interface SavedInputLimits {
  readonly maxBatches: number;
  readonly maxObservations: number;
  readonly maxManifestBytes: number;
  readonly maxLoadedBytes: number;
}
interface MultiInputAssessmentRequest {
  readonly manifest: SavedInputManifest; // supplied through existing request JSON
  readonly assessmentRef: VersionedRef;
}
interface ProposedInputSelection {
  readonly members: readonly SavedInputMember[];
  readonly referenceWindow: DeriveWindow;
}
// Proposed opt-in field on DeriveRequest, not a current callable API:
// inputSelection?: ProposedInputSelection
```

The manifest is persisted in ordinary versioned observations/evidence, not a new store.
Its request representation is JSON safe and participates in the existing operation fingerprint.
Public identifiers must contain no credentials, private connection strings or source values.
Hashes establish content consistency; they do not authenticate a source or grant permission.

## Admission, bounds and time

1. The host authorizes the source namespaces and resolves only adapter-declared roles.
2. Load published observations with existing repository checks and bounded `readMany` chunks.
3. Verify reference membership, expected schema revisions, duplicate roles/members, byte budgets,
   known knowledge times and the adapter's compatibility policy before admitting the manifest.
4. Save unresolved input requirements with `affects` and exact attempt references. A required
   missing input blocks its dependent output; an optional gap need not block independent facts.
5. Derive from the admitted saved members and recorded gap facts. Never fill a missing input
   by querying a device or by silently substituting the latest inventory.

Limits must be explicit and validated before expanding input lists. Reuse the core candidate
limits, repository chunk limit and native record ceiling; add only admission limits they lack.
The current default core ceiling is 10,000 output observations / 8 MiB of candidate JSON;
it does not by itself bound request JSON, loaded input bytes or input batch count.
Budget these separately; do not claim those guards already exist.
Do not copy the native-record ceiling into a claim that all input allocations fit in memory.
Over-limit input records a fixed gap/refusal, never a silently retained prefix.

### One owner for the measurement boundary

Keep `DeriveRequest.window` as the target measurement window. **Do not widen it and ask every
adapter to filter numbers back down.** The proposed opt-in `inputSelection` declares saved
members as measurement or reference inputs and a separate bounded reference read envelope.
Its manifest must agree exactly with the request's batch refs, roles, classes and windows.

The core validates every classification against immutable transform-registered source/schema
role rules; callers cannot relabel a latency measurement as a reference to bypass the window.
Reject duplicates, conflicting classes, unknown roles/schemas and foreign refs before calling
the transform. For admitted members, core applies the target half-open window to measurement
observations and the reference envelope to lookup observations, on their explicit clocks.
Unselected observations never reach `Transform.apply` or its allowed output-support set.
Preserve input order and record exclusions/coverage; an excluded measurement adds no number.
In this opt-in path require exact admitted observation support for each output. A whole-batch
reference or implicit all-batches support cannot reintroduce rows that the core excluded;
refuse those broader claims rather than weakening the window boundary. Existing legacy
batch-level lineage keeps its current behavior outside this opt-in path.

This reuses existing point/interval rules: unknown source time is excluded and a straddling
interval is not split or weighted. The core operates on standard observation timestamps;
an adapter still validates any domain timestamps hidden inside a raw response payload.
Reference admission does not make a mapping fresh or prove continuous validity. The adapter's
named decisions still check actual validity, knowledge time, compatibility and required claims.
Never manufacture a newer timestamp to make an old fact pass.

Without this opt-in field, existing derive selection/fingerprints remain unchanged. Its
implementation and library mutation tests must land before the new writer facade uses it.
There is no per-adapter fallback to the earlier broad-window/filter-it-yourself proposal.

### Manifest bytes belong to the operation budget

`maxManifestBytes` is an additional cap, not a second free allowance. For this opt-in path,
measure canonical UTF-8 request JSON (including the manifest, selection and wrappers) before
fingerprinting/cloning/dispatch, then reserve those bytes against the operation's `maxBytes`.
Validate candidate JSON against the remaining budget: `requestBytes + candidateBytes <= maxBytes`.
Any manifest copy in a candidate counts again because it is actually stored again. A future
reference-only request can avoid that copy; it must not silently drop this accounting.

Count/ref/string bounds must be checked before expanding lists, not after an unbounded clone.
Loaded-input bytes and native record size keep their independent caps. Their safety is not
proved merely by this serialized budget. Overflow fails before derivation/sink dispatch and
records a bounded refusal through the same repository event port. Legacy non-opt-in request
and candidate limits retain their existing behavior.

`knowledgeCutoff` limits learned information separately from domain validity time.
The host must supply trustworthy recorded knowledge times; a caller-provided timestamp is
not proof that a fact was available then. Unknown knowledge time stays an explicit gap.
The manifest fixes exact members and order, so replay uses the same saved inputs rather than
searching current history. Same cutoff plus the same saved membership must give the same result.
Same cutoff alone cannot guarantee a stable result from a mutable external history query.
Late information requires a new assessment identity and manifest; old findings are not rewritten.
Retention expiry may make exact replay unavailable; it never authorizes recollection as history.

## Execution and delivery sequence

```text
save run start and attempts -> save each admitted response / supplied-lookup fact
  -> record role selection, time/compatibility decisions and scoped requirements
  -> freeze and save manifest -> derive typed points with exact saved support
  -> acknowledge point manifest -> acknowledge publish intent -> existing sink
  -> save attempt result and independent trace outcome
```

Use ordinary collect/derive operations through the shared repository event/reconciliation
port. The existing writer journal becomes its product facade, not another durable owner or
attempt database. Keep current versioned run/stage/attempt identities and old records readable.
Their source is explicitly a host event where appropriate.
Such an event records what the host supplied, not authentication of a remote response.
Keep the generic manifest tied to acknowledged operations; a caller's arbitrary reference
does not establish that a response was published or admitted.

Current `derive()` refuses a missing input batch as a whole. Before constructing its request,
resolve an unavailable optional role into a saved gap fact and omit that unavailable batch.
Do not pass a missing batch and expect the core to recover partial truth automatically.
Expiry between admission and derive loading may still refuse the derivation; fail without
dispatch and preserve earlier facts. This proposal adds no snapshot lease or ancestor pin.

Native steps import each relevant item separately, read only the facts they use and record
policy decisions with `decide()`. Use full read tracking and `reads-prefix` write provenance.
Helpers receive only step-read data. One large scope object must not connect unrelated items.
An output's `inputRefs` names exactly its supporting saved observations, with row pointers
in the adapter payload when a response contains several rows. A pointer is not a new record ID.
The native record explains the selection/computation; the structured evidence names its inputs.

Unconfirmed input, point-manifest or intent publication prevents dispatch. A crash after a
saved intent leaves delivery uncertain and never triggers an automatic resend. Later success
cannot erase an earlier uncertain attempt. Receipt persistence and observed delivery remain
separate; trace persistence failure cannot undo a data acknowledgement.

## Adapter example: PowerScale heat and shares

This is adapter behavior outside the generic core. Heat reads
`/platform/7/statistics/summary/heat`; share lookup uses already-collected per-zone
`/platform/3/protocols/smb/shares` responses and recorded paging/discovery outcomes.
The first slice does not add live discovery, historical topology selection or a schedule.

Import each heat row, relevant share candidate and lookup failure into separate native keys.
Named decisions select admissible prefixes, classify a path, choose the matching share and
admit a group. A group is share / operation class / event / node, not one source row.
Its summed rate and row count depend on all that group's admitted source rows.
The compatibility top-object field is chosen across the whole share: it can depend on a
different node/event group. Record those additional dependencies rather than pointing only
to the winning group's first row. Preserve stable input order for ties.

For a match, capture the candidates/completeness evidence needed to justify longest-prefix
selection. An unmatched result depends on the bounded lookup population and its coverage;
failure of one zone must not look like proof that no share exists anywhere.
Rate arithmetic can remain supported while a stronger share-attribution claim is unresolved.
Failed, partial or cut-off zones produce scoped requirements linked to their attempt receipts.
If an output follows a compatibility fallback, label it as such; do not treat it as a verified
relationship merely because Python historically emitted that tag.

Python's later-zone name overwrite, catchall exclusion, tie rules, two-place rounding,
float `hot_objects`, integer coverage counts and shortened top-object text are adapter
compatibility decisions. They are not library rules or automatic authorization to fix Python.
An externally supplied share map is explicitly weaker than saved device-response evidence.
Heat measures activity rate, not latency, and neither its trace nor a matched prefix proves
physical cause, VM identity, continuous validity or an instruction's authority.

## Claim and access boundaries

Reuse foottrace `RequirementResult` records and agentfootprint's existing claim gate.
Each unresolved requirement blocks only the claims listed in `affects`; no satisfied record
overrides an unresolved one. Record requirements in the native state and durable outputs.
The application projects them into the existing cause reader's attention/gap interface.
The generic manifest does not infer causal edges, rank suspects or approve claim language.
Keep source strings untrusted. Apply the application's access/value-hiding policy before
selection and on every evidence page. A read grants no additional claim authority.
Do not expose forbidden input identities, counts or error text through a gap diagnostic.
Use fixed codes; permission-denied facts themselves must be safe for that reader to see.

## Test plan and first implementation slice

- Prove generic `derive()` already combines two saved batches, keeps exact output support,
  refuses foreign references and conservatively reports input coverage; reuse existing tests.
- Test manifest size/count/loaded-byte limits, duplicates, wrong schema, altered references,
  unknown knowledge time and post-cutoff facts. Every refusal must prevent sink dispatch.
- A **library** test puts a measurement before the target and one exactly at its exclusive
  end beside an older valid reference. Neither measurement reaches the transform or changes
  its numbers; the reference remains available. Reject a measurement schema labelled as a
  reference. Removing the core class/window guard must fail these tests, even if the test
  transform simply sums everything it receives. Legacy non-opt-in behavior remains pinned.
- Test combined request/manifest/candidate bytes at and one byte over the same operation
  ceiling, including a request that would fit `maxManifestBytes` alone. Disabling subtraction
  must fail. Refuse before transform/sink work and do not increase existing ceilings.
- Test stale/ambiguous/partial reference decisions. A raw zero stays zero; a gap adds no number.
- Replay the same manifest and cutoff; compare saved fingerprints and cause answers.
  Introduce late information under a new identity and prove the old assessment is unchanged.
- Heat parity uses the real isolated Python oracle and synthetic saved responses. Add
  cross-group top-object support, stable ties, row-count behavior and scoped failed-zone gaps.
- Remove one tracked read, bundle resource keys, or disable per-write provenance: each must
  break its focused cause test. Unrelated groups must remain outside the explanation.
- Reopen packed/compressed records, then expire an input: previously saved unexpired statistics survive while
  replay/drill-down reports expiry honestly. Missing, expired, corrupt and forbidden differ
  internally; the app must not reveal forbidden records through externally different errors.
- Inject failures before/after intent and dispatch; retain uncertain delivery without retries.
  Measure maximum-case record bytes, loaded memory and time; do not raise limits to hide a failure.

First settle the shared owner with #21. The first library PR after approval adds opt-in
class/window admission and combined operation-size tests using synthetic saved inputs;
no heat writer, live reader, new journal or generic writer loop. Then prove saved-input manifest
admission, native cause and save-before-send in the product facade through that same event port.
Then adapt one inactive heat path in Neo, preserving every existing profile when opted out.
Production bounds, retention, disk budgets, watch lists and deployment require separate
host decisions and measurements. This document settles none of them and switches on nothing.
Bounded ownership/expiry bookkeeping is also a pre-activation gate; payload expiry alone
does not bound it. Neither design adds another attempt store to work around that growth.
