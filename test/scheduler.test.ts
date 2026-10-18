import { describe, expect, it, vi } from 'vitest';
import type { Batch, Observation, PipelineResult, RecordRef, StoredRecord } from '../src/contracts.js';
import type { Pipeline } from '../src/createPipeline.js';
import type { CollectionSchedule, PreparedPoll, SummaryProjector, SummaryStore } from '../src/prepared/contracts.js';
import { createCollectionScheduler } from '../src/prepared/scheduler.js';

const at = (minutes: number, milliseconds = 0) => new Date(Date.parse('2026-10-16T12:00:00Z') + minutes * 60_000 + milliseconds).toISOString();
const schedule: CollectionSchedule = {
  ref: { id: 'hourly-host', version: '1' }, namespace: 'tenant-a', sourceRef: { id: 'device', version: 'config-1' },
  methodRef: { id: 'statistics', version: '1' }, summaryRef: { id: 'projection', version: '1' },
  startAt: at(0), intervalMs: 60_000, request: { limit: 5 }, timeoutMs: 2_000,
};
const receipt = (batch: Batch): PipelineResult => ({ commit: { operationId: batch.operationId, status: 'committed', batchRef: batch.ref,
  evidenceRef: batch.evidenceRef, observationRefs: batch.observationRefs, committedAt: batch.finishedAt }, trace: { status: 'disabled' } });

function fixture(options: { empty?: boolean; collectedAt?: string; secondCollectedAt?: string; publish?: () => void } = {}) {
  const polls = new Map<string, PreparedPoll>();
  const records = new Map<string, StoredRecord>();
  const operations = new Map<string, PipelineResult>();
  const acquisition = vi.fn(); const calculation = vi.fn();
  const rawRef = { kind: 'batch' as const, id: 'raw-batch' };
  const rawEvidence = { kind: 'evidence' as const, id: 'raw-evidence' };
  const raw: Observation[] = options.empty ? [] : [options.collectedAt ?? at(2, 10), options.secondCollectedAt ?? at(2, 15)].map((collectedAt, index) => ({
    kind: 'observation', ref: { kind: 'observation', id: `raw-${index}` }, key: `raw-${index}`, subject: { client: 'a' },
    schemaRef: { id: 'reading', version: '1' }, time: { kind: 'source-time-unknown' }, collectedAt,
    payload: { value: 10 + index * 10 }, batchRef: rawRef, evidenceRef: rawEvidence,
    support: { kind: 'source', sourceRef: schedule.sourceRef },
  }));
  const collectionBatch: Batch = { kind: 'batch', ref: rawRef, key: 'capture', operationId: 'collect-operation', observationRefs: raw.map(item => item.ref),
    evidenceRef: rawEvidence, coverage: { status: 'partial', time: { kind: 'source-time-unknown' }, selection: { topN: 5 } }, outcome: 'partial',
    sourceRef: schedule.sourceRef, startedAt: at(2), finishedAt: at(2, 20) };
  const derivedBatch: Batch = { ...collectionBatch, ref: { kind: 'batch', id: 'derived-batch' }, operationId: 'derive-operation', key: 'statistics',
    observationRefs: [{ kind: 'observation', id: 'summary-observation' }], evidenceRef: { kind: 'evidence', id: 'derived-evidence' }, methodRef: schedule.methodRef };
  const derived: Observation = { kind: 'observation', ref: derivedBatch.observationRefs[0]!, key: 'statistic', subject: { client: 'a' },
    schemaRef: { id: 'stats', version: '1' }, time: { kind: 'source-time-unknown' }, payload: { mean: 15, count: 2 },
    batchRef: derivedBatch.ref, evidenceRef: derivedBatch.evidenceRef,
    support: { kind: 'inputs', inputRefs: raw.map(item => item.ref) } };
  records.set(rawRef.id, collectionBatch); records.set(derivedBatch.ref.id, derivedBatch);
  raw.forEach(item => records.set(item.ref.id, item)); records.set(derived.ref.id, derived);
  const pipeline: Pipeline = {
    collect: vi.fn(async request => {
      let found = operations.get(request.idempotencyKey);
      if (!found) { acquisition(); found = receipt(collectionBatch); operations.set(request.idempotencyKey, found); }
      return found;
    }),
    reconcileCollect: vi.fn(async request => operations.get(request.idempotencyKey)),
    derive: vi.fn(async request => {
      let found = operations.get(request.idempotencyKey);
      if (!found) { calculation(); found = receipt(derivedBatch); operations.set(request.idempotencyKey, found); }
      return found;
    }),
    read: vi.fn(async (ref: RecordRef) => records.get(ref.id)),
  };
  const store: SummaryStore = {
    register: vi.fn(async () => {}),
    readPoll: vi.fn(async (_schedule, slot) => polls.get(slot)),
    publish: vi.fn(async (_schedule, poll) => { options.publish?.(); polls.set(poll.slotAt, structuredClone(poll)); }),
    latestSlot: vi.fn(async () => [...polls.keys()].sort().at(-1)),
    query: vi.fn(async () => { throw new Error('Query must not be called by ingestion'); }),
    readSummary: vi.fn(async () => undefined),
  };
  const projector: SummaryProjector = { ref: schedule.summaryRef, project: vi.fn(async observation => ({
    key: observation.key, subject: observation.subject, collectedAt: options.collectedAt ?? at(2, 10),
    payload: observation.payload as { mean: number; count: number },
  })) };
  const create = (overrides: Partial<Parameters<typeof createCollectionScheduler>[0]> = {}) => createCollectionScheduler({
    pipeline, store, schedules: [schedule], projectors: [projector], clock: () => at(2, 20), ...overrides,
  });
  return { create, pipeline, store, projector, polls, records, raw, collectionBatch, derivedBatch, acquisition, calculation };
}

describe('host-owned scheduled collection', () => {
  it('prepares one native window after commits and shares only its compact metadata with projections', async () => {
    const f = fixture();
    const metadata = { trace: { status: 'stored', ref: { kind: 'trace', id: 'native-window' } } };
    const prepareWindow = vi.fn<NonNullable<SummaryProjector['prepareWindow']>>(async context => {
      expect(f.calculation).toHaveBeenCalledTimes(1);
      expect(context.slotAt).toBe(at(2));
      expect(context.collection).toEqual({ batch: f.collectionBatch, observations: f.raw });
      expect(context.derivation.batch).toEqual(f.derivedBatch);
      expect(context.derivation.observations).toHaveLength(1);
      return metadata;
    });
    const projector = { ...f.projector, prepareWindow, project: vi.fn<SummaryProjector['project']>(async (observation, context) => ({
      key: observation.key, subject: observation.subject, collectedAt: at(2, 10), payload: { ...observation.payload as object, window: context.window ?? null },
    })) };
    const result = await f.create({ projectors: [projector] }).runDue({ now: at(2) });
    expect(prepareWindow).toHaveBeenCalledTimes(1);
    expect(result.polls[2]!.summaries[0]!.payload.window).toEqual(metadata);
    expect(Object.isFrozen(vi.mocked(projector.project).mock.calls[0]![1].window)).toBe(true);
    await f.create({ projectors: [projector] }).runDue({ now: at(2) });
    expect(prepareWindow).toHaveBeenCalledTimes(1);
  });

  it('bounds window metadata and isolates committed inputs from hook mutation', async () => {
    const f = fixture();
    await f.create({ projectors: [{ ...f.projector, async prepareWindow(context) {
      expect(Object.isFrozen(context.collection.observations[0])).toBe(true);
      return { oversized: 'x'.repeat(64 * 1024) };
    } }] }).runDue({ now: at(2) });
    expect(f.polls.get(at(2))?.status).toBe('failed');
    expect(f.raw[0]!.payload).toEqual({ value: 10 });
  });

  it('does not publish untraced claims after failed window preparation or cancellation', async () => {
    const f = fixture();
    const project = vi.fn(f.projector.project);
    const prepareWindow = vi.fn(async () => { throw Error('private source details'); });
    const result = await f.create({ projectors: [{ ...f.projector, prepareWindow, project }] }).runDue({ now: at(2) });
    expect(result.polls[2]).toMatchObject({ status: 'failed', summaries: [] });
    expect(JSON.stringify(result)).not.toContain('private source');
    expect(project).not.toHaveBeenCalled();
    const cancelled = fixture(); const controller = new AbortController();
    await expect(cancelled.create({ projectors: [{ ...cancelled.projector, async prepareWindow() { controller.abort(); return {}; } }] })
      .runDue({ now: at(2), signal: controller.signal })).rejects.toThrow();
    expect(cancelled.polls.has(at(2))).toBe(false);
  });

  it('records missed slots and acquires only the newest due slot with exact collection-time bounds', async () => {
    const f = fixture();
    const result = await f.create().runDue({ now: at(2, 5) });
    expect(result.hasMore).toBe(false);
    expect(result.polls.map(poll => [poll.slotAt, poll.status])).toEqual([[at(0), 'missed'], [at(1), 'missed'], [at(2), 'collected']]);
    expect(f.acquisition).toHaveBeenCalledTimes(1); expect(f.calculation).toHaveBeenCalledTimes(1);
    expect(f.pipeline.collect).toHaveBeenCalledWith(expect.objectContaining<unknown>({ sourceRef: schedule.sourceRef, request: schedule.request }), expect.objectContaining({ timeoutMs: 2_000 }));
    expect(f.pipeline.derive).toHaveBeenCalledWith(expect.objectContaining({ inputBatchRefs: [f.collectionBatch.ref],
      window: { basis: 'collection', start: at(2, 10), end: at(2, 16) } }), expect.objectContaining({ timeoutMs: 2_000 }));
    const summary = result.polls[2]!.summaries[0]!;
    expect(summary).toMatchObject({ namespace: schedule.namespace, scheduleRef: schedule.ref, sourceRef: schedule.sourceRef,
      methodRef: schedule.methodRef, summaryRef: schedule.summaryRef, collectedAt: at(2, 10), slotAt: at(2),
      collectionBatchRef: f.collectionBatch.ref, collectionEvidenceRef: f.collectionBatch.evidenceRef,
      batchRef: f.derivedBatch.ref, evidenceRef: f.derivedBatch.evidenceRef, inputRefs: f.raw.map(item => item.ref), payload: { mean: 15, count: 2 } });
    expect(summary.id).toMatch(/^[a-f0-9]{64}$/);
    expect(result.polls[2]!.coverage).toEqual(f.collectionBatch.coverage);
  });

  it('does not start itself, collect future slots, or replay a restart or a rolled-back clock', async () => {
    const f = fixture(); const scheduler = f.create();
    expect(f.store.register).not.toHaveBeenCalled(); expect(f.pipeline.collect).not.toHaveBeenCalled();
    expect(await scheduler.runDue({ now: at(-1) })).toEqual({ polls: [], hasMore: false });
    await scheduler.runDue({ now: at(2) });
    expect(await f.create().runDue({ now: at(2, 30) })).toEqual({ polls: [], hasMore: false });
    expect(await scheduler.runDue({ now: at(1) })).toEqual({ polls: [], hasMore: false });
    expect(f.acquisition).toHaveBeenCalledTimes(1);
  });

  it('bounds catch-up work and never uses present data to fill older due slots', async () => {
    const f = fixture(); const scheduler = f.create({ maxSlotsPerTick: 2 });
    expect(await scheduler.runDue({ now: at(3) })).toMatchObject({ hasMore: true, polls: [{ status: 'missed' }, { status: 'missed' }] });
    expect(f.pipeline.collect).not.toHaveBeenCalled();
    expect(await scheduler.runDue({ now: at(3) })).toMatchObject({ hasMore: false, polls: [{ status: 'missed' }, { status: 'collected' }] });
    expect(f.acquisition).toHaveBeenCalledTimes(1);
  });

  it('marks even the newest slot missed when it exceeds the configured lateness', async () => {
    const f = fixture(); const custom = { ...schedule, maxLatenessMs: 1_000 };
    const result = await f.create({ schedules: [custom] }).runDue({ now: at(0, 1_001) });
    expect(result.polls).toMatchObject([{ status: 'missed' }]); expect(f.pipeline.collect).not.toHaveBeenCalled();
  });

  it('rechecks the live clock before acquisition when catch-up work crosses into a newer slot', async () => {
    const f = fixture(); let live = at(0);
    vi.mocked(f.store.register).mockImplementation(async () => { live = at(1, 10); });
    const scheduler = f.create({ clock: () => live });
    const result = await scheduler.runDue();
    expect(result).toMatchObject({ hasMore: true, polls: [{ slotAt: at(0), status: 'missed' }] });
    expect(f.pipeline.collect).not.toHaveBeenCalled();
    expect((await scheduler.runDue()).polls).toMatchObject([{ slotAt: at(1), status: 'collected' }]);
    expect(f.acquisition).toHaveBeenCalledTimes(1);
  });

  it('rechecks a tighter lateness bound even when work stays within the same interval', async () => {
    const f = fixture(); let live = at(0);
    vi.mocked(f.store.register).mockImplementation(async () => { live = at(0, 1_001); });
    const result = await f.create({ clock: () => live, schedules: [{ ...schedule, maxLatenessMs: 1_000 }] }).runDue();
    expect(result.polls).toMatchObject([{ status: 'missed' }]); expect(f.pipeline.collect).not.toHaveBeenCalled();
  });

  it('leaves a slot pending if the clock rolls back before acquisition', async () => {
    const f = fixture(); let live = at(0);
    vi.mocked(f.store.register).mockImplementation(async () => { live = at(-1); });
    const result = await f.create({ clock: () => live }).runDue();
    expect(result).toEqual({ polls: [], hasMore: false });
    expect(f.pipeline.collect).not.toHaveBeenCalled(); expect(f.store.publish).not.toHaveBeenCalled();
  });

  it('reconciles an unpublished poll from committed collect and derive receipts with stable identities', async () => {
    let rejectPublication = true;
    const f = fixture({ publish: () => { if (rejectPublication) throw new Error('Disk unavailable'); } });
    await expect(f.create().runDue({ now: at(0) })).rejects.toThrow('Disk unavailable');
    expect(f.polls.size).toBe(0); rejectPublication = false;
    const retried = await f.create().runDue({ now: at(0) });
    expect(retried.polls[0]!.status).toBe('collected');
    expect(f.pipeline.collect).toHaveBeenCalledTimes(2); expect(f.pipeline.derive).toHaveBeenCalledTimes(2);
    expect(f.acquisition).toHaveBeenCalledTimes(1); expect(f.calculation).toHaveBeenCalledTimes(1);
    expect(vi.mocked(f.pipeline.collect).mock.calls[0]![0]).toEqual(vi.mocked(f.pipeline.collect).mock.calls[1]![0]);
    expect(vi.mocked(f.pipeline.derive).mock.calls[0]![0]).toEqual(vi.mocked(f.pipeline.derive).mock.calls[1]![0]);
  });

  it('keeps slot identities separate for different namespaces and immutable schedule revisions', async () => {
    const f = fixture(); await f.create().runDue({ now: at(0) });
    f.polls.clear(); await f.create({ schedules: [{ ...schedule, namespace: 'tenant-b' }] }).runDue({ now: at(0) });
    const calls = vi.mocked(f.pipeline.collect).mock.calls;
    expect(calls[0]![0].idempotencyKey).not.toBe(calls[1]![0].idempotencyKey);
  });

  it('recovers a committed capture after restart even when its original slot is now historical', async () => {
    let rejectPublication = true;
    const f = fixture({ publish: () => { if (rejectPublication) throw new Error('Summary disk unavailable'); } });
    await expect(f.create().runDue({ now: at(0) })).rejects.toThrow('Summary disk unavailable');
    rejectPublication = false;
    const recovered = await f.create().runDue({ now: at(2) });
    expect(recovered.polls.map(poll => [poll.slotAt, poll.status])).toEqual([[at(0), 'collected'], [at(1), 'missed'], [at(2), 'collected']]);
    expect(f.pipeline.reconcileCollect).toHaveBeenCalledTimes(2);
    expect(f.pipeline.collect).toHaveBeenCalledTimes(2);
    expect(f.acquisition).toHaveBeenCalledTimes(2);
    expect(f.calculation).toHaveBeenCalledTimes(2);
    expect(recovered.polls[0]!.summaries[0]!.collectedAt).toBe(at(2, 10));
  });

  it('does not advance a historical slot when lookup cannot establish whether capture committed', async () => {
    const f = fixture(); vi.mocked(f.pipeline.reconcileCollect).mockResolvedValue({ commit: { operationId: 'lookup', status: 'failed',
      error: { code: 'OPERATION_FAILED', message: 'repository inaccessible' } }, trace: { status: 'disabled' } });
    await expect(f.create().runDue({ now: at(2) })).rejects.toThrow(/reconciliation/i);
    expect(f.store.publish).not.toHaveBeenCalled(); expect(f.pipeline.collect).not.toHaveBeenCalled();
  });

  it('publishes empty captures without asking a transform to invent statistics', async () => {
    const f = fixture({ empty: true });
    const result = await f.create().runDue({ now: at(0) });
    expect(result.polls).toMatchObject([{ status: 'empty', summaries: [], collection: { commit: { status: 'committed' } } }]);
    expect(f.pipeline.derive).not.toHaveBeenCalled(); expect(f.projector.project).not.toHaveBeenCalled();
  });

  it.each(['failed', 'partial', 'unknown'] as const)('records a %s collection acknowledgement as failure without deriving or leaking its error', async status => {
    const f = fixture(); vi.mocked(f.pipeline.collect).mockResolvedValue({ commit: { operationId: 'bad', status,
      error: { code: 'remote', message: 'https://user:secret@example.test/private' } }, trace: { status: 'disabled' } });
    const result = await f.create().runDue({ now: at(0) });
    expect(result.polls[0]!.status).toBe('failed'); expect(result.polls[0]!.summaries).toEqual([]);
    expect(JSON.stringify(result)).not.toContain('secret'); expect(f.pipeline.derive).not.toHaveBeenCalled();
  });

  it('records a failed derivation as failed coverage, not an empty or zero-valued summary', async () => {
    const f = fixture(); vi.mocked(f.pipeline.derive).mockResolvedValue({ commit: { operationId: 'bad', status: 'failed' }, trace: { status: 'disabled' } });
    expect((await f.create().runDue({ now: at(0) })).polls).toMatchObject([{ status: 'failed', summaries: [] }]);
    expect(f.projector.project).not.toHaveBeenCalled();
  });

  it.each([at(3), at(2, 12)])('rejects projected time %s unless it is an actual capture receipt', async collectedAt => {
    const f = fixture(); vi.mocked(f.projector.project).mockResolvedValue({ key: 'a', subject: {}, collectedAt, payload: { mean: 15 } });
    expect((await f.create().runDue({ now: at(0) })).polls).toMatchObject([{ status: 'failed', summaries: [] }]);
  });

  it('stops at cancellation fences before acquisition, after acquisition, and after projection', async () => {
    const before = fixture(); const stopped = new AbortController(); stopped.abort();
    await expect(before.create().runDue({ now: at(0), signal: stopped.signal })).rejects.toThrow();
    expect(before.pipeline.collect).not.toHaveBeenCalled(); expect(before.store.publish).not.toHaveBeenCalled();
    const during = fixture(); const abortCollect = new AbortController();
    vi.mocked(during.pipeline.collect).mockImplementation(async () => { abortCollect.abort(); return receipt(during.collectionBatch); });
    await expect(during.create().runDue({ now: at(0), signal: abortCollect.signal })).rejects.toThrow();
    expect(during.pipeline.derive).not.toHaveBeenCalled(); expect(during.store.publish).not.toHaveBeenCalled();
    const projected = fixture(); const abortProject = new AbortController();
    vi.mocked(projected.projector.project).mockImplementation(async observation => { abortProject.abort(); return { key: observation.key, subject: {}, collectedAt: at(2, 10), payload: {} }; });
    await expect(projected.create().runDue({ now: at(0), signal: abortProject.signal })).rejects.toThrow();
    expect(projected.store.publish).not.toHaveBeenCalled();
  });

  it('rejects overlapping ticks while preserving the active tick', async () => {
    const f = fixture(); let release!: () => void;
    vi.mocked(f.pipeline.collect).mockImplementation(async () => { await new Promise<void>(resolve => { release = resolve; }); return receipt(f.collectionBatch); });
    const scheduler = f.create(); const active = scheduler.runDue({ now: at(0) });
    await vi.waitFor(() => expect(release).toBeTypeOf('function'));
    await expect(scheduler.runDue({ now: at(0) })).rejects.toThrow(/running|overlap|active/i);
    release(); expect((await active).polls[0]!.status).toBe('collected');
  });

  it('propagates durable schedule conflicts without publishing a successful failure poll', async () => {
    const f = fixture(); vi.mocked(f.store.register).mockRejectedValue(new Error('schedule_conflict'));
    await expect(f.create().runDue({ now: at(0) })).rejects.toThrow('schedule_conflict');
    expect(f.pipeline.collect).not.toHaveBeenCalled(); expect(f.store.publish).not.toHaveBeenCalled();
  });

  it('starts only when called and exits promptly when the host aborts', async () => {
    const f = fixture(); const controller = new AbortController();
    const scheduler = f.create({ clock: () => at(-1) });
    const running = scheduler.start({ signal: controller.signal, pollIntervalMs: 60_000 });
    await vi.waitFor(() => expect(f.store.register).toHaveBeenCalled());
    controller.abort(); await running;
    expect(f.pipeline.collect).not.toHaveBeenCalled();
  });

  it('rejects invalid bounds and missing exact projector registrations at construction', () => {
    const f = fixture();
    for (const intervalMs of [0, -1, 999, Number.POSITIVE_INFINITY]) expect(() => f.create({ schedules: [{ ...schedule, intervalMs }] })).toThrow();
    for (const maxSlotsPerTick of [0, 1.5, 1_001]) expect(() => f.create({ maxSlotsPerTick })).toThrow();
    expect(() => f.create({ projectors: [] })).toThrow();
    expect(() => f.create({ projectors: [f.projector, f.projector] })).toThrow();
  });
});
