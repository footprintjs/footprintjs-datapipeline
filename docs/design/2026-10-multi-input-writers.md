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
| Application journal and save-before-send | Neo `performanceWriter/`; integration described in `docs/INFLUXDB_HOST_RECEIPTS.md` |
| Packing, compression and explicit expiry | Existing repository and trace stores; no ancestor pinning |

The immediate limitation is Neo's facade: `performanceWriter/contracts.ts` takes one capture,
and `performanceWriter/transform.ts` assigns every point only that capture's `inputRefs`.
Generic `derive()` already accepts several batches. Extending the facade must not duplicate it.

## Options and recommendation

1. Put all lookups in writer configuration. Smallest code change, but the lookup remains
   host-supplied metadata, uses policy-size budgets and cannot claim device-response lineage.
2. Build a second join/provenance subsystem. Rejected: duplicates existing derivation and traces.
3. Assemble a bounded saved-input manifest, then call existing `derive()`. Recommended.

Use an opt-in composition around existing contracts, first proven as an adapter integration.
A manifest describes roles and membership; it does not contain copied response bodies.
Role names, compatibility rules, required claims and vendor meanings belong to the adapter.
The first implementation should export no new writer engine. Promote a small generic helper
only after the integration identifies behavior that another adapter can use unchanged.

## Types-only sketch

These proposed integration types are not current exports. Reuse existing reference, time,
JSON and shared requirement types; do not create another reason-code list.

```ts
interface SavedInputMember {
  readonly role: string;                 // fixed, adapter-declared role
  readonly batchRef: BatchRef;
  readonly observationRefs: readonly ObservationRef[];
  readonly learnedAt: string;            // host-recorded knowledge time
}
interface SavedInputManifest {
  readonly schemaVersion: 1;
  readonly policyRef: VersionedRef;
  readonly knowledgeCutoff: string;
  readonly targetWindow: DeriveWindow;   // what the answer is about
  readonly readWindow: DeriveWindow;     // what existing derive() may load
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
The default core ceiling is 10,000 output observations / 8 MiB of candidate JSON; it does not
by itself bound total loaded input bytes or input batch count. Budget these separately.
Do not copy the native-record ceiling into a claim that all input allocations fit in memory.
Over-limit input records a fixed gap/refusal, never a silently retained prefix.

Existing `derive()` filters every observation against one half-open window before the transform.
An older valid lookup would disappear if that window were just the target measurement window.
For the first integration, use an explicit collection-clock `readWindow` spanning the admitted
members; keep the target's source/window semantics in the manifest and adapter decisions.
The transform must reject unexpected members and independently select which facts apply.
Only measurements inside the target window contribute numbers; the broader read envelope
exists to admit lookup evidence, never to widen the statistic accidentally.
A wide read window does not make old mappings fresh or prove continuous topology coverage.
Do not manufacture a new source timestamp merely to bypass existing window selection.

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

Use ordinary collect/derive operations for those facts and the existing writer journal for
run and attempt identities. Their source is explicitly a host event where appropriate.
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
- Test older lookups inside the explicit read envelope but outside the target window,
  plus stale/ambiguous/partial lookup decisions. A raw zero stays zero; a gap adds no number.
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

First PR after design approval: saved-input manifest admission and a synthetic two-input
transform, with native cause and save-before-send tests; no live reader or generic writer loop.
Then adapt one inactive heat path in Neo, preserving every existing profile when opted out.
Production bounds, retention, disk budgets, watch lists and deployment require separate
host decisions and measurements. This document settles none of them and switches on nothing.
