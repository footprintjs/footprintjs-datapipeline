import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, describe, expect, it } from 'vitest';
import type { CollectionSchedule, PreparedPoll, PreparedSummary, SummaryStore } from '../src/prepared/contracts.js';
import { preparedSummaryId } from '../src/prepared/validation.js';
import { createSqliteSummaryStore } from '../src/storage/sqliteSummaries.js';

const directories: string[] = [];
const stores: SummaryStore[] = [];
afterEach(async () => {
  for (const store of stores.splice(0)) await store.close?.();
  await Promise.all(directories.splice(0).map(path => rm(path, { recursive: true, force: true })));
});
async function location(): Promise<string> {
  const path = await mkdtemp(join(tmpdir(), 'prepared-summary-'));
  directories.push(path);
  return join(path, 'nested', 'summaries.sqlite');
}
function open(filename: string): SummaryStore {
  const store = createSqliteSummaryStore({ filename }); stores.push(store); return store;
}
function schedule(overrides: Partial<CollectionSchedule> = {}): CollectionSchedule {
  return {
    ref: { id: 'sample-every-five-minutes', version: '1' }, namespace: 'tenant-one',
    sourceRef: { id: 'source-one', version: 'config-one' }, methodRef: { id: 'mean', version: '1' },
    summaryRef: { id: 'summary', version: '1' }, startAt: '2026-10-16T12:00:00.000Z', intervalMs: 300_000,
    request: { limit: 10 }, ...overrides,
  };
}
function poll(s: CollectionSchedule, slotAt = s.startAt, collectedAt = slotAt, value = 10): PreparedPoll {
  const suffix = Date.parse(slotAt).toString();
  const collectionBatchRef = { kind: 'batch' as const, id: 'collection-' + suffix };
  const collectionEvidenceRef = { kind: 'evidence' as const, id: 'collection-evidence-' + suffix };
  const batchRef = { kind: 'batch' as const, id: 'derived-' + suffix };
  const evidenceRef = { kind: 'evidence' as const, id: 'derived-evidence-' + suffix };
  const observationRef = { kind: 'observation' as const, id: 'stat-' + suffix };
  const candidate: Omit<PreparedSummary, 'id'> = {
    key: 'client-one', subject: { client: 'client-one' }, payload: { sum: value, count: 1 }, collectedAt,
    namespace: s.namespace, scheduleRef: s.ref, sourceRef: s.sourceRef, methodRef: s.methodRef, summaryRef: s.summaryRef,
    slotAt, observationRef, batchRef, evidenceRef, collectionBatchRef, collectionEvidenceRef,
    inputRefs: [{ kind: 'observation', id: 'original-' + suffix }],
  };
  return {
    namespace: s.namespace, scheduleRef: s.ref, slotAt, status: 'collected', attemptedAt: slotAt, finishedAt: collectedAt,
    collection: { commit: { operationId: 'collect-' + suffix, status: 'committed', batchRef: collectionBatchRef, evidenceRef: collectionEvidenceRef }, trace: { status: 'disabled' } },
    derivation: { commit: { operationId: 'derive-' + suffix, status: 'committed', batchRef, evidenceRef, observationRefs: [observationRef] }, trace: { status: 'disabled' } },
    summaries: [{ ...candidate, id: preparedSummaryId(candidate) }],
  };
}

describe('indexed prepared summary store', () => {
  it('durably publishes receipts and summaries, without any raw repository port', async () => {
    const filename = await location(); const first = open(filename); const s = schedule(); const p = poll(s);
    await first.register(s); await first.publish(s, p); await first.close?.();
    const restarted = open(filename);
    expect(await restarted.readPoll(s, s.startAt)).toEqual(p);
    expect(await restarted.readSummary(s, p.summaries[0]!.id)).toEqual(p.summaries[0]);
    expect(await restarted.latestSlot(s)).toBe(s.startAt);
    const result = await restarted.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:10:00.000Z' });
    expect(result.summaries).toEqual(p.summaries);
    expect(result.coverage).toMatchObject({ expectedPolls: 2, collectedPolls: 1, pendingPolls: 1 });
  });

  it('queries receipt times half-open while coverage uses scheduled slots and identifies boundary receipts', async () => {
    const store = open(await location()); const s = schedule(); await store.register(s);
    await store.publish(s, poll(s, s.startAt, '2026-10-16T12:02:00.000Z'));
    await store.publish(s, poll(s, '2026-10-16T12:05:00.000Z', '2026-10-16T12:06:00.000Z'));
    const result = await store.query({ schedule: s, start: '2026-10-16T12:01:00.000Z', end: '2026-10-16T12:06:00.000Z' });
    expect(result.summaries.map(item => item.collectedAt)).toEqual(['2026-10-16T12:02:00.000Z']);
    expect(result.polls).toHaveLength(2);
    expect(result.coverage).toEqual({ expectedPolls: 1, collectedPolls: 1, emptyPolls: 0, failedPolls: 0, missedPolls: 0, pendingPolls: 0, additionalReceiptPolls: 1, meaning: 'scheduled-slots-and-collection-receipts' });
  });

  it('separates empty, failed, missed and pending polls instead of inventing zero statistics', async () => {
    const store = open(await location()); const s = schedule(); await store.register(s);
    const empty = poll(s); const { derivation: _ignored, ...emptyBase } = empty;
    await store.publish(s, { ...emptyBase, status: 'empty', summaries: [] });
    await store.publish(s, { namespace: s.namespace, scheduleRef: s.ref, slotAt: '2026-10-16T12:05:00.000Z', status: 'failed', reason: 'Collection unavailable.', summaries: [] });
    await store.publish(s, { namespace: s.namespace, scheduleRef: s.ref, slotAt: '2026-10-16T12:10:00.000Z', status: 'missed', reason: 'Collector was offline.', summaries: [] });
    const result = await store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:20:00.000Z' });
    expect(result.summaries).toEqual([]);
    expect(result.coverage).toMatchObject({ expectedPolls: 4, emptyPolls: 1, failedPolls: 1, missedPolls: 1, pendingPolls: 1 });
  });

  it('is idempotent for the same immutable schedule and poll and rejects changed content', async () => {
    const filename = await location(); const left = open(filename); const right = open(filename); const s = schedule();
    await Promise.all([left.register(s), right.register(s)]);
    await Promise.all([left.publish(s, poll(s)), right.publish(s, poll(s))]);
    await expect(right.publish(s, poll(s, s.startAt, s.startAt, 99))).rejects.toMatchObject({ code: 'poll_conflict' });
    await expect(right.register(schedule({ intervalMs: 60_000 }))).rejects.toMatchObject({ code: 'schedule_conflict' });
    expect((await left.readPoll(s, s.startAt))?.summaries[0]?.payload).toEqual({ sum: 10, count: 1 });
  });

  it('keeps namespace and source/method revisions isolated and fails rather than mixing a changed schedule', async () => {
    const store = open(await location()); const a = schedule(); const b = schedule({ namespace: 'tenant-two' });
    const c = schedule({ ref: { id: a.ref.id, version: '2' }, sourceRef: { ...a.sourceRef, version: 'config-two' }, methodRef: { ...a.methodRef, version: '2' } });
    for (const s of [a, b, c]) { await store.register(s); await store.publish(s, poll(s)); }
    for (const s of [a, b, c]) {
      const result = await store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:05:00.000Z' });
      expect(result.summaries).toHaveLength(1); expect(result.summaries[0]?.namespace).toBe(s.namespace);
      expect(result.summaries[0]?.sourceRef).toEqual(s.sourceRef); expect(result.summaries[0]?.methodRef).toEqual(s.methodRef);
    }
    expect(await store.readSummary(b, poll(a).summaries[0]!.id)).toBeUndefined();
    await expect(store.query({ schedule: schedule({ methodRef: { id: 'mean', version: 'other' } }), start: a.startAt, end: '2026-10-16T12:05:00.000Z' })).rejects.toMatchObject({ code: 'schedule_conflict' });
  });

  it('clones published inputs and all returned snapshots', async () => {
    const store = open(await location()); const s = schedule(); const p = poll(s); await store.register(s); await store.publish(s, p);
    (p.summaries[0]!.payload as { sum: number }).sum = 999;
    const first = await store.readSummary(s, p.summaries[0]!.id); (first!.payload as { sum: number }).sum = 888;
    expect((await store.readSummary(s, p.summaries[0]!.id))?.payload).toEqual({ sum: 10, count: 1 });
  });

  it('reports truncation explicitly and bounds query range and expected poll counts', async () => {
    const store = open(await location()); const s = schedule(); await store.register(s);
    await store.publish(s, poll(s)); await store.publish(s, poll(s, '2026-10-16T12:05:00.000Z'));
    const result = await store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:10:00.000Z', limit: 1 });
    expect(result.summaries).toHaveLength(1); expect(result.truncated).toBe(true); expect(result.coverage.collectedPolls).toBe(2);
    await expect(store.query({ schedule: s, start: s.startAt, end: '2026-11-17T12:00:00.000Z' })).rejects.toMatchObject({ code: 'invalid_prepared_contract' });
    await expect(store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:10:00.000Z', limit: 50_001 })).rejects.toMatchObject({ code: 'invalid_prepared_contract' });
    const fast = schedule({ ref: { id: 'fast', version: '1' }, intervalMs: 1000 }); await store.register(fast);
    await expect(store.query({ schedule: fast, start: fast.startAt, end: '2026-10-17T12:00:00.000Z' })).rejects.toMatchObject({ code: 'invalid_prepared_contract' });
  });

  it('rejects malformed summary IDs, namespace, revisions, JSON and poll states atomically', async () => {
    const store = open(await location()); const s = schedule(); await store.register(s); const valid = poll(s);
    const invalid = [
      { ...valid, summaries: [{ ...valid.summaries[0]!, id: '0'.repeat(64) }] },
      { ...valid, namespace: 'another' },
      { ...valid, status: 'failed', summaries: valid.summaries },
      { ...valid, summaries: [{ ...valid.summaries[0]!, payload: { invalid: Number.NaN } }] },
      { ...valid, slotAt: '2026-10-16T12:01:00.000Z' },
      { ...valid, scheduleRef: undefined },
      { ...valid, coverage: null },
    ];
    for (const item of invalid) await expect(store.publish(s, item as PreparedPoll)).rejects.toMatchObject({ code: 'invalid_prepared_contract' });
    expect(await store.readPoll(s, s.startAt)).toBeUndefined();
    expect(await store.latestSlot(s)).toBeUndefined();
    await store.publish(s, valid); expect(await store.readPoll(s, s.startAt)).toEqual(valid);
  });

  it('rolls back both the poll receipt and summaries when publication fails after inserting the poll', async () => {
    const filename = await location(); const store = open(filename); const s = schedule(); await store.register(s);
    const external = new DatabaseSync(filename);
    external.exec("CREATE TRIGGER fail_summary BEFORE INSERT ON prepared_summaries BEGIN SELECT RAISE(ABORT, 'private internal error'); END");
    await expect(store.publish(s, poll(s))).rejects.toMatchObject({ code: 'storage_failure', message: 'Prepared storage operation failed.' });
    expect(await store.readPoll(s, s.startAt)).toBeUndefined();
    expect(await store.readSummary(s, poll(s).summaries[0]!.id)).toBeUndefined();
    external.exec('DROP TRIGGER fail_summary'); external.close();
    await store.publish(s, poll(s)); expect(await store.readPoll(s, s.startAt)).toEqual(poll(s));
  });

  it('rejects invalid schedule bounds and mismatched lineage even when the content hash is recomputed', async () => {
    const store = open(await location()); const s = schedule();
    for (const invalid of [schedule({ intervalMs: 999 }), schedule({ intervalMs: 1.1 }), schedule({ maxLatenessMs: s.intervalMs + 1 }), schedule({ startAt: '2026-02-30T12:00:00Z' }), schedule({ request: { large: 'x'.repeat(65_536) } })]) {
      await expect(store.register(invalid)).rejects.toMatchObject({ code: 'invalid_prepared_contract' });
    }
    await store.register(s);
    const p = poll(s); const original = p.summaries[0]!;
    for (const changes of [
      { methodRef: { id: 'mean', version: 'changed' } },
      { summaryRef: { id: 'summary', version: 'changed' } },
      { batchRef: { kind: 'batch' as const, id: 'unrelated-batch' } },
      { observationRef: { kind: 'observation' as const, id: 'unlisted-observation' } },
      { collectionEvidenceRef: { kind: 'evidence' as const, id: 'unrelated-receipt' } },
      { inputRefs: [] },
    ]) {
      const candidate = { ...original, ...changes };
      const changed = { ...candidate, id: preparedSummaryId(candidate) };
      await expect(store.publish(s, { ...p, summaries: [changed] })).rejects.toMatchObject({ code: 'invalid_prepared_contract' });
    }
    expect(await store.latestSlot(s)).toBeUndefined();
  });

  it('uses the receipt-time index and exposes no source repository dependency', async () => {
    const filename = await location(); const store = open(filename); const s = schedule(); await store.register(s); await store.publish(s, poll(s));
    const external = new DatabaseSync(filename);
    const plan = external.prepare('EXPLAIN QUERY PLAN SELECT id FROM prepared_summaries WHERE scope_key = ? AND collected_ms >= ? AND collected_ms < ? ORDER BY collected_ms, id LIMIT ?').all('scope', 0, 1, 100);
    expect(plan.map(row => row.detail).join(' ')).toContain('prepared_summaries_time');
    expect(external.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all().map(row => row.name)).toEqual(['prepared_polls', 'prepared_schedules', 'prepared_summaries']);
    external.close();
    expect((await store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:05:00.000Z' })).summaries).toHaveLength(1);
  });

  it('bounds total summary bytes with explicit truncation while retaining complete poll coverage', async () => {
    // Aggregate setup writes more than 8 MiB before exercising the byte cap.
    // Allow parallel history/crash fixtures without changing any query bound.
    const store = open(await location()); const s = schedule(); await store.register(s);
    for (let index = 0; index < 36; index++) {
      const slotAt = new Date(Date.parse(s.startAt) + index * s.intervalMs).toISOString();
      const p = poll(s, slotAt); const value = { ...p.summaries[0]!, payload: { boundedMetadata: 'x'.repeat(240_000) } };
      await store.publish(s, { ...p, summaries: [{ ...value, id: preparedSummaryId(value) }] });
    }
    const result = await store.query({ schedule: s, start: s.startAt, end: '2026-10-16T15:00:00.000Z', limit: 50_000 });
    expect(result.truncated).toBe(true); expect(result.summaries.length).toBeGreaterThan(0); expect(result.summaries.length).toBeLessThan(36);
    expect(Buffer.byteLength(JSON.stringify(result.summaries))).toBeLessThan(8 * 1024 * 1024);
    expect(result.coverage).toMatchObject({ expectedPolls: 36, collectedPolls: 36, pendingPolls: 0 });
  }, 15_000);

  it('checks persisted content hashes on summary and poll reads and time queries', async () => {
    const filename = await location(); const store = open(filename); const s = schedule(); const p = poll(s);
    await store.register(s); await store.publish(s, p);
    const external = new DatabaseSync(filename);
    external.prepare('UPDATE prepared_summaries SET body = ?').run('{"secret":"must not be echoed"}');
    external.close();
    await expect(store.readSummary(s, p.summaries[0]!.id)).rejects.toMatchObject({ code: 'storage_corrupt' });
    await expect(store.readPoll(s, s.startAt)).rejects.toMatchObject({ code: 'storage_corrupt' });
    await expect(store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:05:00.000Z' })).rejects.toThrow('Stored prepared data failed verification.');
  });

  it('checks schedule and poll metadata hashes independently of summaries', async () => {
    const filename = await location(); const store = open(filename); const s = schedule(); await store.register(s);
    await store.publish(s, { namespace: s.namespace, scheduleRef: s.ref, slotAt: s.startAt, status: 'missed', summaries: [] });
    const external = new DatabaseSync(filename);
    external.exec("UPDATE prepared_polls SET body = '{}'");
    await expect(store.query({ schedule: s, start: s.startAt, end: '2026-10-16T12:05:00.000Z' })).rejects.toMatchObject({ code: 'storage_corrupt' });
    external.exec("UPDATE prepared_schedules SET body = '{}'"); external.close();
    await expect(store.readSummary(s, '0'.repeat(64))).rejects.toMatchObject({ code: 'storage_corrupt' });
  });

  it('fails closed after close and never leaks underlying storage details', async () => {
    const store = open(await location()); await store.close?.(); await store.close?.();
    await expect(store.register(schedule())).rejects.toMatchObject({ code: 'store_closed' });
  });
});
