# Sealed periods for expired metadata

Draft integration contract, types only. This is the missing growth work for #27,
not code, a chosen history horizon, or a production policy. Hard caps remain a
last resort; they do not reclaim capacity and must not be presented as readiness.

## Reuse and ownership

The retained repository's existing control ledger remains the sole authority for
intents, exact owners, expiry and committed deletion proof. No fleet journal or
second metadata database. Reuse #24's bounded, exact binary codec as an internal
shared component after its format/read redesign is settled; do not publish or
depend on its standalone snapshot constructor. Extend the codec version to keep
#26's elapsed-age fields exactly. Existing values, IDs and receipts are not rewritten.

```ts
// Illustrative internal types, no exports or runnable configuration.
interface SealedPeriod {
  version: number;
  period: number;
  generation: number;
  inventorySha256: string;
  operationCount: number;
  referenceCount: number;
  chunks: readonly { sha256: string; bytes: number }[];
}
```

## Close, prove, publish, then reclaim

1. A period groups **acceptance age** from #26, not device time or the wall clock.
   Its explicit size is namespace policy. No retrospective timestamps, guesses
   about downtime, or automatic backfilling of old periods.
2. Closing is monotone in the same ledger. New first admissions cannot enter a
   closed period; an earlier pending intent retains its identity. A period may
   seal only when its complete inventory is expired, acknowledged and confirmed
   data-deleted. Pending, uncertain, undeleted or live rows block sealing.
3. Freeze the complete inventory under a generation. Encode bounded sorted chunks
   with full operation/reference hashes, exact owners, original receipt times,
   request/write fingerprints and elapsed-age fields. No probabilistic membership
   or caller-supplied "complete" label. An oversized period splits explicitly
   into bounded chunks; no prefix is silently accepted.
4. Write, sync, reopen, decode and verify all chunks. The catalogue covers the
   whole frozen period. Existing hot rows remain authoritative until an atomic
   control-ledger commit names that verified catalogue and advances generation.
5. Reclaim duplicate hot rows only after that commit. SQLite free pages are not
   claimed as recovered disk: measure allocated files and use bounded maintenance
   to reuse/reclaim space. Account for staging, duplicate copies, indexes,
   journals and failed cleanup throughout. No filesystem I/O holds a long ledger
   transaction; #25's correction is a prerequisite.

On restart, before the catalogue commit hot rows remain truth. After it, the
verified cold catalogue is truth even if duplicate hot bytes remain. Failed
physical cleanup stays charged. A missing/corrupt catalogue refuses reads and
admission; it is never treated as an empty history. Retirement or hot-row removal
must not race an unpublished candidate into reusing an accepted identity.

## Exact lookup and capacity

Every lookup/reconciliation checks the current catalogue generation. Reads use
the complete hot/cold ownership union. An individual chunk miss cannot mean
"never recorded". A known deleted record still reports its exact deletion receipt;
an expired reference cannot be published under another operation. Late knowledge
creates a new assessment and never edits an earlier saved finding.

Use the codec's exact reference index initially; measure it before redesigning
opaque IDs. Routing must bound bytes and work per lookup; scanning all old
periods or loading the entire history is not a production solution. Counts must
include both tiers. #27's `max(rowid)` shortcut is valid only while ownership
tables are append-only and **must be replaced** before hot-row deletion.

Sealing reduces representation cost, but exact membership still grows. A finite
byte cap will eventually stop work unless a separately reviewed period-retirement
rule can retire old identities without admitting them again. That rule needs a
versioned period identity/fence and honest "history unavailable" answers; it must
not invent per-record deletion proof after membership is forgotten. No horizon,
permission to forget IDs, or change to existing lookup meaning is selected here.

## Proof required before runtime adoption

- Replay the actual retained ledgers and every receipt/owner field exactly;
  re-run native cause and missing-poll-gap comparisons from saved inputs.
- Measure allocated hot rows/indexes, cold chunks/indexes/catalogue, transient
  journal/staging peaks and lookup cost at the real source rate. Report measured
  before/after bytes; quote no reduction until those results exist.
- Kill at freeze, chunk write/sync, catalogue commit, hot-row deletion and physical
  reclamation. Competing publishers, pollers, readers and cleanup must preserve
  one authoritative generation and original deletion proof.
- Neutralize full-inventory checks, generation checks, journal charging and the
  no-reuse fence; each must fail a test. Include expired, unknown, late, corrupt,
  duplicate and unconfirmed cases, clock jumps and restart.
- Gate the new namespace format so older readers refuse it. Upgrade main process
  and cleanup worker together and explicitly choose verified-uptime expiry in
  both. No adoption of an old wall-clock namespace or activation by default.

This contract needs review before the integrated compactor is built. #24's
selected-snapshot tests and #27's admission caps do not establish this protocol.
