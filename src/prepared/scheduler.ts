import type { Batch, CollectRequest, Observation, PipelineResult, RecordRef, VersionedRef } from '../contracts.js';
import type { Pipeline } from '../createPipeline.js';
import { assertRef, assertTimestamp, canonical, cloneJson, ensure } from '../validation.js';
import type { CollectionSchedule, PreparedPoll, PreparedSummary, SummaryProjector, SummaryStore } from './contracts.js';
import { preparedSummaryId, scheduleFingerprint, validatePoll, validateSchedule } from './validation.js';

const MAX_WINDOW_METADATA_BYTES = 64 * 1024;

export interface CollectionSchedulerOptions {
  readonly pipeline: Pipeline;
  readonly store: SummaryStore;
  readonly schedules: readonly CollectionSchedule[];
  readonly projectors: readonly SummaryProjector[];
  readonly clock?: () => string;
  /** Bounds both missed-slot publication and acquisition during one tick. */
  readonly maxSlotsPerTick?: number;
}
export interface CollectionScheduler {
  runDue(options?: { readonly now?: string; readonly signal?: AbortSignal }): Promise<{ readonly polls: readonly PreparedPoll[]; readonly hasMore: boolean }>;
  /** The host owns this loop and its lifetime. Construction never starts it. */
  start(options?: { readonly signal?: AbortSignal; readonly pollIntervalMs?: number }): Promise<void>;
}

function freeze<T>(value: T): T {
  if (value && typeof value === 'object') { Object.freeze(value); for (const child of Object.values(value)) freeze(child); }
  return value;
}
function timestamp(value: string): number { assertTimestamp(value); return Date.parse(value); }
function iso(value: number): string { ensure(Number.isSafeInteger(value) && Math.abs(value) <= 8_640_000_000_000_000, 'Scheduled timestamp exceeds the supported clock'); return new Date(value).toISOString(); }
function sameRef(left: VersionedRef | undefined, right: VersionedRef): boolean { return !!left && canonical(left) === canonical(right); }
function sanitizedResult(result: PipelineResult, operation: 'Collection' | 'Derivation'): PipelineResult {
  return {
    commit: { ...result.commit, ...(result.commit.error ? { error: { code: 'PREPARED_OPERATION_FAILED', message: `${operation} did not complete` } } : {}) },
    trace: result.trace.status === 'unavailable' ? { status: 'unavailable', error: { code: 'PREPARED_TRACE_UNAVAILABLE', message: 'Execution trace is unavailable' } } : result.trace,
  };
}
function pause(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.resolve();
  return new Promise(resolve => {
    const finish = () => { clearTimeout(timer); signal?.removeEventListener('abort', finish); resolve(); };
    const timer = setTimeout(finish, ms);
    signal?.addEventListener('abort', finish, { once: true });
    if (signal?.aborted) finish();
  });
}

/** Coordinates existing collect/derive flows; it neither knows a vendor nor queries raw data. */
export function createCollectionScheduler(options: CollectionSchedulerOptions): CollectionScheduler {
  const pipeline = options.pipeline;
  const store = options.store;
  const schedules = options.schedules.map(value => freeze(validateSchedule(value)));
  const maxSlots = options.maxSlotsPerTick ?? 100;
  ensure(Number.isSafeInteger(maxSlots) && maxSlots >= 1 && maxSlots <= 1_000, 'maxSlotsPerTick must be an integer from 1 through 1000');
  const projectorRegistry = new Map<string, SummaryProjector>();
  for (const projector of options.projectors) {
    assertRef(projector.ref);
    const key = canonical(projector.ref);
    ensure(!projectorRegistry.has(key), 'Duplicate summary projector revision');
    ensure(typeof projector.project === 'function', 'A summary projector implementation is required');
    ensure(projector.prepareWindow === undefined || typeof projector.prepareWindow === 'function', 'Window preparation must be a function');
    projectorRegistry.set(key, { ref: freeze(cloneJson(projector.ref)), project: projector.project.bind(projector),
      ...(projector.prepareWindow ? { prepareWindow: projector.prepareWindow.bind(projector) } : {}) });
  }
  const identities = new Set<string>();
  for (const schedule of schedules) {
    const identity = canonical([schedule.namespace, schedule.ref]);
    ensure(!identities.has(identity), 'Duplicate collection schedule revision'); identities.add(identity);
    ensure(projectorRegistry.has(canonical(schedule.summaryRef)), 'Exact summary projector revision is not registered');
  }
  const clock = options.clock ?? (() => new Date().toISOString());
  const now = () => { const value = clock(); assertTimestamp(value); return new Date(Date.parse(value)).toISOString(); };
  let running = false;
  let starting = false;

  async function committedBatch(result: PipelineResult): Promise<Batch> {
    ensure(result.commit.status === 'committed' && result.commit.batchRef && result.commit.evidenceRef && result.commit.observationRefs,
      'An acknowledged committed batch and its references are required');
    const batch = await pipeline.read(result.commit.batchRef);
    ensure(batch?.kind === 'batch' && batch.outcome !== 'failed', 'The committed batch is missing or failed');
    ensure(canonical(batch.ref) === canonical(result.commit.batchRef) && canonical(batch.evidenceRef) === canonical(result.commit.evidenceRef)
      && canonical(batch.observationRefs) === canonical(result.commit.observationRefs), 'The committed batch does not match its receipt');
    return batch;
  }

  async function observations(batch: Batch, signal: AbortSignal): Promise<readonly Observation[]> {
    const result: Observation[] = [];
    for (const ref of batch.observationRefs) {
      signal.throwIfAborted();
      const value = await pipeline.read(ref);
      signal.throwIfAborted();
      ensure(value?.kind === 'observation' && canonical(value.ref) === canonical(ref)
        && canonical(value.batchRef) === canonical(batch.ref) && canonical(value.evidenceRef) === canonical(batch.evidenceRef), 'A committed observation is missing or has different lineage');
      result.push(value);
    }
    return result;
  }

  function collectRequest(schedule: CollectionSchedule, slotAt: string): CollectRequest {
    return { sourceRef: schedule.sourceRef, request: schedule.request, idempotencyKey: `prepared:collect:${scheduleFingerprint(schedule)}:${slotAt}` };
  }

  async function acquire(schedule: CollectionSchedule, slotAt: string, signal: AbortSignal, recovered?: PipelineResult): Promise<PreparedPoll> {
    const base = { namespace: schedule.namespace, scheduleRef: schedule.ref, slotAt };
    const attemptedAt = now();
    let collection: PipelineResult | undefined;
    let derivation: PipelineResult | undefined;
    let collectionBatch: Batch | undefined;
    try {
      signal.throwIfAborted();
      const identity = `${scheduleFingerprint(schedule)}:${slotAt}`;
      collection = recovered ?? await pipeline.collect(collectRequest(schedule, slotAt),
        { signal, ...(schedule.timeoutMs === undefined ? {} : { timeoutMs: schedule.timeoutMs }) });
      signal.throwIfAborted();
      collectionBatch = await committedBatch(collection);
      signal.throwIfAborted();
      ensure(sameRef(collectionBatch.sourceRef, schedule.sourceRef), 'Collection source revision does not match schedule');
      const captured = await observations(collectionBatch, signal);
      if (captured.length === 0) return validatePoll(schedule, { ...base, status: 'empty', attemptedAt, finishedAt: now(), collection: sanitizedResult(collection, 'Collection'),
        coverage: collectionBatch.coverage, summaries: [] });
      const capturedTimes = captured.map(observation => {
        ensure(observation.collectedAt !== undefined, 'A collection receipt timestamp is required');
        return timestamp(observation.collectedAt);
      });
      const capturedTimeSet = new Set(capturedTimes);
      const allowedInputs = new Set([canonical(collectionBatch.ref), ...captured.map(observation => canonical(observation.ref))]);
      const minTime = Math.min(...capturedTimes); const maxTime = Math.max(...capturedTimes);
      const window = { basis: 'collection' as const, start: iso(minTime), end: iso(maxTime + 1) };
      signal.throwIfAborted();
      derivation = await pipeline.derive({ methodRef: schedule.methodRef, idempotencyKey: `prepared:derive:${identity}`,
        inputBatchRefs: [collectionBatch.ref], window }, { signal, ...(schedule.timeoutMs === undefined ? {} : { timeoutMs: schedule.timeoutMs }) });
      signal.throwIfAborted();
      const derivedBatch = await committedBatch(derivation);
      signal.throwIfAborted();
      ensure(sameRef(derivedBatch.methodRef, schedule.methodRef), 'Derivation method revision does not match schedule');
      const output = await observations(derivedBatch, signal);
      const projector = projectorRegistry.get(canonical(schedule.summaryRef))!;
      const preparedWindow = projector.prepareWindow ? freeze(cloneJson(await projector.prepareWindow({
        schedule, slotAt, collection: freeze(cloneJson({ batch: collectionBatch, observations: captured })),
        derivation: freeze(cloneJson({ batch: derivedBatch, observations: output })), read: pipeline.read.bind(pipeline), signal,
      }))) : undefined;
      signal.throwIfAborted();
      if (preparedWindow !== undefined) {
        ensure(preparedWindow !== null && typeof preparedWindow === 'object' && !Array.isArray(preparedWindow)
          && Buffer.byteLength(canonical(preparedWindow)) <= MAX_WINDOW_METADATA_BYTES, 'Window metadata must be a bounded JSON object');
      }
      const summaries: PreparedSummary[] = [];
      const keys = new Set<string>();
      for (const observation of output) {
        signal.throwIfAborted();
        const candidate = await projector.project(observation, { schedule, collectionBatch, collection, derivation, read: pipeline.read.bind(pipeline), signal,
          ...(preparedWindow === undefined ? {} : { window: preparedWindow }) });
        signal.throwIfAborted();
        if (candidate === undefined) continue;
        const projected = cloneJson(candidate);
        ensure(Object.keys(projected).every(key => ['key', 'subject', 'collectedAt', 'payload'].includes(key)), 'A projector may only supply summary candidate fields');
        ensure(typeof projected.key === 'string' && projected.key.trim() && !keys.has(projected.key), 'Summary keys must be nonempty and unique in one poll');
        keys.add(projected.key);
        const collectedTime = timestamp(projected.collectedAt);
        ensure(collectedTime >= minTime && collectedTime <= maxTime && capturedTimeSet.has(collectedTime), 'A summary must retain an actual collection receipt timestamp');
        ensure(projected.subject !== null && typeof projected.subject === 'object' && !Array.isArray(projected.subject)
          && projected.payload !== null && typeof projected.payload === 'object' && !Array.isArray(projected.payload), 'Summary subject and payload must be JSON objects');
        const support = observation.support;
        ensure(support.kind !== 'source', 'A derived summary needs committed input lineage');
        const inputRefs: readonly RecordRef[] = support.kind === 'inputs' ? support.inputRefs : support.inputBatchRefs;
        ensure(inputRefs.length > 0, 'A derived summary needs nonempty input lineage');
        ensure(inputRefs.every(ref => allowedInputs.has(canonical(ref))), 'Summary lineage must point into this collection');
        const withoutId = { ...projected, namespace: schedule.namespace, scheduleRef: schedule.ref, sourceRef: schedule.sourceRef,
          methodRef: schedule.methodRef, summaryRef: schedule.summaryRef, slotAt, observationRef: observation.ref, batchRef: observation.batchRef,
          evidenceRef: observation.evidenceRef, collectionBatchRef: collectionBatch.ref, collectionEvidenceRef: collectionBatch.evidenceRef, inputRefs };
        summaries.push({ ...withoutId, id: preparedSummaryId(withoutId) });
      }
      signal.throwIfAborted();
      const coverage = collectionBatch.coverage.status === 'complete' && derivedBatch.coverage.status !== 'complete'
        ? { ...collectionBatch.coverage, status: derivedBatch.coverage.status, reason: 'Derivation reports incomplete coverage' } : collectionBatch.coverage;
      return validatePoll(schedule, { ...base, status: summaries.length ? 'collected' : 'empty', attemptedAt, finishedAt: now(), collection: sanitizedResult(collection, 'Collection'),
        derivation: sanitizedResult(derivation, 'Derivation'), coverage, summaries });
    } catch {
      signal.throwIfAborted();
      return { ...base, status: 'failed', attemptedAt, finishedAt: now(), reason: 'Collection or summary preparation did not complete',
        ...(collection ? { collection: sanitizedResult(collection, 'Collection') } : {}), ...(derivation ? { derivation: sanitizedResult(derivation, 'Derivation') } : {}),
        ...(collectionBatch ? { coverage: collectionBatch.coverage } : {}), summaries: [] };
    }
  }

  function nextSlot(schedule: CollectionSchedule, latest: string | undefined): number {
    const start = timestamp(schedule.startAt);
    if (latest === undefined) return start;
    const value = timestamp(latest);
    ensure(value >= start && (value - start) % schedule.intervalMs === 0, 'Stored schedule cursor is not an aligned slot');
    return value + schedule.intervalMs;
  }

  async function runDue(execution: { readonly now?: string; readonly signal?: AbortSignal } = {}) {
    ensure(!running, 'A collection scheduler tick is already running'); running = true;
    const signal = execution.signal ?? new AbortController().signal;
    try {
      signal.throwIfAborted();
      const tick = timestamp(execution.now ?? now());
      const polls: PreparedPoll[] = [];
      let visited = 0;
      for (const schedule of schedules) {
        signal.throwIfAborted(); await store.register(schedule); signal.throwIfAborted();
        const start = timestamp(schedule.startAt);
        if (tick < start) continue;
        const newest = start + Math.floor((tick - start) / schedule.intervalMs) * schedule.intervalMs;
        let next = nextSlot(schedule, await store.latestSlot(schedule));
        signal.throwIfAborted();
        while (next <= newest && visited < maxSlots) {
          signal.throwIfAborted();
          visited++;
          const slotAt = iso(next);
          const existing = await store.readPoll(schedule, slotAt);
          signal.throwIfAborted();
          if (existing === undefined) {
            // Catch-up/storage work or an earlier source may have consumed an
            // interval. Explicit `now` is a trusted host's pinned simulation clock.
            const acquisitionTick = execution.now === undefined ? timestamp(now()) : tick;
            if (acquisitionTick < next) break;
            const currentSlot = start + Math.floor((acquisitionTick - start) / schedule.intervalMs) * schedule.intervalMs;
            let poll: PreparedPoll;
            if (next !== currentSlot || acquisitionTick - next > (schedule.maxLatenessMs ?? schedule.intervalMs)) {
              // A previous process may have committed capture data before its summary publication.
              // Lookup never acquires present data to fill a historical slot.
              const recovered = await pipeline.reconcileCollect(collectRequest(schedule, slotAt));
              signal.throwIfAborted();
              ensure(recovered === undefined || recovered.commit.status === 'committed', 'Historical collection reconciliation did not establish a committed or absent capture');
              poll = recovered ? await acquire(schedule, slotAt, signal, recovered)
                : { namespace: schedule.namespace, scheduleRef: schedule.ref, slotAt, status: 'missed', reason: 'Scheduled acquisition was not performed within its allowed slot', summaries: [] };
            } else poll = await acquire(schedule, slotAt, signal);
            signal.throwIfAborted();
            // Store/configuration errors escape: an unpublished poll never advances the durable cursor.
            await store.publish(schedule, poll);
            polls.push(poll);
          }
          next += schedule.intervalMs;
        }
      }
      let hasMore = false;
      const completedTick = execution.now === undefined ? timestamp(now()) : tick;
      for (const schedule of schedules) {
        signal.throwIfAborted();
        if (nextSlot(schedule, await store.latestSlot(schedule)) <= completedTick) hasMore = true;
      }
      signal.throwIfAborted();
      return { polls, hasMore };
    } finally { running = false; }
  }

  return {
    runDue,
    async start(execution = {}) {
      ensure(!starting, 'The collection scheduler loop is already active');
      const pollIntervalMs = execution.pollIntervalMs ?? 1_000;
      ensure(Number.isSafeInteger(pollIntervalMs) && pollIntervalMs >= 1 && pollIntervalMs <= 2_147_483_647, 'pollIntervalMs must be a positive timer interval');
      starting = true;
      try {
        while (!execution.signal?.aborted) {
          try { await runDue(execution.signal ? { signal: execution.signal } : {}); }
          catch (error) { if (execution.signal?.aborted) return; throw error; }
          await pause(pollIntervalMs, execution.signal);
        }
      } finally { starting = false; }
    },
  };
}
