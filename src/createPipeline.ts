import { FlowChartExecutor } from 'footprintjs';
import { inOutRecorder } from 'footprintjs/trace';
import type {
  Batch, BatchInput, CandidateBatch, CollectRequest, CommitReceipt, Coverage, DeriveRequest,
  EvidenceOrigin, JsonValue, Observation, PipelineError, PipelineResult,
  RecordRef, Source, StoredRecord, VersionedRef,
} from './contracts.js';
import type { CommitWrite, OperationContext, Repository, SourceAdapter, TraceStore, Transform } from './ports.js';
import { readRepositoryRecords } from './readRepository.js';
import { collectFlow } from './flows/collect.js';
import { deriveFlow } from './flows/derive.js';
import { assertRef, assertTimestamp, canonical, cloneJson, digest, ensure, PipelineFault, validateCandidate, type PayloadLimits } from './validation.js';

export interface ExecutionOptions { readonly signal?: AbortSignal; readonly timeoutMs?: number }
export interface PipelineOptions {
  readonly sources: readonly Source[];
  readonly adapters: readonly SourceAdapter[];
  readonly transforms?: readonly Transform[];
  readonly repository: Repository;
  readonly traceStore?: TraceStore;
  readonly clock?: () => string;
  readonly limits?: Partial<PayloadLimits>;
}
export interface Pipeline {
  collect(request: CollectRequest, options?: ExecutionOptions): Promise<PipelineResult>;
  /** Lookup-only reconciliation. Missing operations never invoke an adapter or write a trace. */
  reconcileCollect(request: CollectRequest): Promise<PipelineResult | undefined>;
  derive(request: DeriveRequest, options?: ExecutionOptions): Promise<PipelineResult>;
  read(ref: RecordRef): Promise<StoredRecord | undefined>;
}
function freeze<T>(value: T): T {
  if (value && typeof value === 'object') { Object.freeze(value); for (const child of Object.values(value)) freeze(child); }
  return value;
}
function registry<T extends { readonly ref: VersionedRef }>(values: readonly T[]): Map<string, T> {
  const result = new Map<string, T>();
  for (const value of values) {
    assertRef(value.ref); const key = canonical(value.ref);
    ensure(!result.has(key), `Duplicate registration: ${value.ref.id}@${value.ref.version}`);
    result.set(key, value);
  }
  return result;
}
function errorRecord(error: unknown): PipelineError {
  return { code: error instanceof PipelineFault ? error.code : 'OPERATION_FAILED', message: error instanceof Error ? error.message : String(error) };
}
function abortable<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(new PipelineFault('ABORTED', 'Operation cancelled or timed out'));
    signal.addEventListener('abort', abort, { once: true });
    work.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
  });
}

export function createPipeline(options: PipelineOptions): Pipeline {
  const sources = registry(options.sources.map(source => freeze(cloneJson(source))));
  // Capture the exact implementation at construction; later registration mutation cannot change a revision.
  const adapters = registry(options.adapters.map(adapter => ({ ref: freeze(cloneJson(adapter.ref)), collect: adapter.collect.bind(adapter) })));
  const transforms = registry((options.transforms ?? []).map(transform => ({ ref: freeze(cloneJson(transform.ref)), apply: transform.apply.bind(transform) })));
  const repository = options.repository;
  const identity = repository.identity;
  const traceStore = options.traceStore;
  const clock = options.clock ?? (() => new Date().toISOString());
  const now = () => { const value = clock(); assertTimestamp(value); return value; };
  const limits = { maxObservations: options.limits?.maxObservations ?? 10_000, maxBytes: options.limits?.maxBytes ?? 8 * 1024 * 1024 };
  ensure(Number.isSafeInteger(limits.maxObservations) && limits.maxObservations > 0 && Number.isSafeInteger(limits.maxBytes) && limits.maxBytes > 0, 'Limits must be positive integers');
  const active = new Map<string, { fingerprint: string; promise: Promise<PipelineResult> }>();

  function validatedRequest<T extends CollectRequest | DeriveRequest>(input: T): T {
    const request = freeze(cloneJson(input));
    ensure(typeof request.idempotencyKey === 'string' && request.idempotencyKey.trim(), 'An idempotencyKey is required');
    return request;
  }
  function resolveCollect(input: CollectRequest): { source: Source; adapter: SourceAdapter; origin: EvidenceOrigin } {
    assertRef(input.sourceRef);
    const source = sources.get(canonical(input.sourceRef)); ensure(source, 'Source revision is not registered');
    const adapter = adapters.get(canonical(source.adapterRef)); ensure(adapter, 'Adapter revision is not registered');
    ensure(Object.hasOwn(input, 'request'), 'Collection request is required');
    return { source, adapter, origin: { kind: 'collect', sourceRef: source.ref, adapterRef: source.adapterRef,
      sourceConfigFingerprint: digest(source.config ?? {}), request: input.request } };
  }

  async function reconcileCollect(requestInput: CollectRequest): Promise<PipelineResult | undefined> {
    let operationId = 'invalid-request';
    try {
      const request = validatedRequest(requestInput);
      const logicalId = digest({ kind: 'collect', key: request.idempotencyKey });
      operationId = identity ? identity.operationId(logicalId) : logicalId;
      if (identity) await identity.assertOperation(operationId, 'lookup');
      const { origin } = resolveCollect(request);
      const fingerprint = digest({ request, origin });
      const existing = await repository.findOperation(operationId);
      if (existing === undefined) return undefined;
      if (existing.fingerprint !== fingerprint) throw new PipelineFault('IDEMPOTENCY_CONFLICT', 'This idempotency key was used for a different request');
      return { commit: cloneJson(existing.receipt), trace: traceStore
        ? { status: 'unavailable', error: { code: 'LOOKUP_ONLY', message: 'Lookup-only reconciliation does not read or write execution traces' } }
        : { status: 'disabled' } };
    } catch (error) { return { commit: { operationId, status: 'failed', error: errorRecord(error) }, trace: { status: 'disabled' } }; }
  }

  async function execute(kind: 'collect' | 'derive', requestInput: CollectRequest | DeriveRequest, execution: ExecutionOptions = {}): Promise<PipelineResult> {
    // Validation errors are returned in the same result envelope as acquisition errors.
    let operationId = 'invalid-request';
    try {
      const request = validatedRequest(requestInput);
      const logicalId = digest({ kind, key: request.idempotencyKey });
      operationId = identity ? identity.operationId(logicalId) : logicalId;
      let source: Source | undefined;
      let adapter: SourceAdapter | undefined;
      let transform: Transform | undefined;
      let origin: EvidenceOrigin;
      if (kind === 'collect') {
        ({ source, adapter, origin } = resolveCollect(request as CollectRequest));
      } else {
        const input = request as DeriveRequest; assertRef(input.methodRef);
        transform = transforms.get(canonical(input.methodRef)); ensure(transform, 'Transform revision is not registered');
        ensure(Array.isArray(input.inputBatchRefs) && input.inputBatchRefs.length > 0, 'Derivation needs input batches');
        for (const ref of input.inputBatchRefs) ensure(ref.kind === 'batch' && typeof ref.id === 'string' && ref.id.length, 'Invalid batch reference');
        ensure(new Set(input.inputBatchRefs.map(ref => ref.id)).size === input.inputBatchRefs.length, 'Duplicate input batches');
        ensure(input.window && (input.window.basis === 'source' || input.window.basis === 'collection'), 'Window basis is required');
        assertTimestamp(input.window.start); assertTimestamp(input.window.end);
        ensure(Date.parse(input.window.start) < Date.parse(input.window.end), 'Window must have positive duration');
        origin = { kind, methodRef: input.methodRef, inputBatchRefs: input.inputBatchRefs, window: input.window, ...(input.request === undefined ? {} : { request: input.request }) };
      }
      const fingerprint = digest({ request, origin });
      if (execution.timeoutMs !== undefined) ensure(Number.isFinite(execution.timeoutMs) && execution.timeoutMs > 0, 'timeoutMs must be positive');
      const pending = active.get(operationId);
      if (pending) {
        if (pending.fingerprint !== fingerprint) throw new PipelineFault('IDEMPOTENCY_CONFLICT', 'This idempotency key was used for a different request');
        return pending.promise;
      }
      const promise = run();
      active.set(operationId, { fingerprint, promise });
      try { return await promise; } finally { active.delete(operationId); }

      async function run(): Promise<PipelineResult> {
        const startedAt = now();
        const controller = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        const cancel = () => controller.abort(execution.signal?.reason);
        if (execution.signal?.aborted) cancel(); else execution.signal?.addEventListener('abort', cancel, { once: true });
        if (execution.timeoutMs !== undefined) {
          timer = setTimeout(() => controller.abort(new PipelineFault('ABORTED', 'Operation timed out')), execution.timeoutMs);
        }
        const finish = (result: PipelineResult): PipelineResult => {
          if (timer) clearTimeout(timer);
          execution.signal?.removeEventListener('abort', cancel);
          return result;
        };
        const context: OperationContext = { signal: controller.signal, operationId, startedAt, now };
        let commitAttempted = false;
        let acknowledgedReceipt: CommitReceipt | undefined;
        let selectedInputs: readonly BatchInput[] = [];
        let excludedUnknownTimes = 0;
        let executor: FlowChartExecutor | undefined;
        let receipt: CommitReceipt;
        try {
          controller.signal.throwIfAborted();
          if (identity) await abortable(identity.assertOperation(operationId, 'admit'), controller.signal);
          const existing = await abortable(repository.findOperation(operationId), controller.signal);
          if (existing) {
            if (existing.fingerprint !== fingerprint) throw new PipelineFault('IDEMPOTENCY_CONFLICT', 'This idempotency key was used for a different request');
            receipt = cloneJson(existing.receipt);
          } else {
            const prepare = (raw: CandidateBatch): CommitWrite => {
              controller.signal.throwIfAborted();
              const candidate = validateCandidate(raw, limits);
              const finishedAt = now();
              const ref = <K extends 'batch' | 'observation' | 'evidence'>(recordKind: K, key: string) => identity
                ? identity.reference(operationId, recordKind, key)
                : ({ kind: recordKind, id: digest({ operationId, kind: recordKind, key }) });
              const batchRef = ref('batch', candidate.key), evidenceRef = ref('evidence', candidate.key);
              const allowed = new Set(selectedInputs.flatMap(input => [canonical(input.batch.ref), ...input.observations.map(item => canonical(item.ref))]));
              const observations: Observation[] = candidate.observations.map(item => {
                if (kind === 'collect') ensure(item.inputRefs === undefined, 'Collected observations cannot claim derived support');
                else if (item.inputRefs) for (const inputRef of item.inputRefs) ensure(allowed.has(canonical(inputRef)), 'Output claims support outside the selected input manifest');
                return { ...item, collectedAt: item.collectedAt ?? finishedAt, kind: 'observation', ref: ref('observation', item.key), batchRef, evidenceRef,
                  support: origin.kind === 'collect' ? { kind: 'source', sourceRef: origin.sourceRef }
                    : item.inputRefs ? { kind: 'inputs', inputRefs: item.inputRefs } : { kind: 'batches', inputBatchRefs: origin.inputBatchRefs } };
              });
              let coverage: Coverage = candidate.coverage;
              if (kind === 'derive') {
                const window = (request as DeriveRequest).window;
                const unknown = excludedUnknownTimes > 0 || selectedInputs.some(input => {
                  const time = input.batch.coverage.time;
                  return input.batch.coverage.status === 'unknown' || time.kind !== 'interval' || time.basis !== window.basis || Date.parse(time.start) > Date.parse(window.start) || Date.parse(time.end) < Date.parse(window.end);
                });
                const partial = selectedInputs.some(input => input.batch.outcome !== 'complete' || input.batch.coverage.status === 'partial');
                if (unknown || partial) coverage = { ...coverage, status: unknown || coverage.status === 'unknown' ? 'unknown' : 'partial', reason: 'Coverage is bounded by the supplied input batches and their declared time coverage' };
              }
              const outcome = candidate.outcome === 'complete' && coverage.status !== 'complete' ? 'partial' : candidate.outcome;
              const observationRefs = observations.map(item => item.ref);
              return {
                operationId, fingerprint,
                batch: { kind: 'batch', ref: batchRef, key: candidate.key, operationId, observationRefs, evidenceRef, coverage, outcome, startedAt, finishedAt,
                  ...(origin.kind === 'collect' ? { sourceRef: origin.sourceRef } : { methodRef: origin.methodRef }), ...(candidate.errors ? { errors: candidate.errors } : {}) },
                observations,
                evidence: { kind: 'evidence', ref: evidenceRef, operationId, fingerprint, origin, startedAt, finishedAt, outcome, outputRefs: [batchRef, ...observationRefs],
                  ...(candidate.errors ? { errors: candidate.errors } : {}), details: { ...(candidate.details ?? {}), ...(kind === 'derive' ? { selectedObservationRefs: selectedInputs.flatMap(input => input.observations.map(item => ({ kind: item.ref.kind, id: item.ref.id }))), excludedUnknownTimes } : {}) } },
              };
            };
            const publish = async (raw: CommitWrite): Promise<CommitReceipt> => {
              controller.signal.throwIfAborted();
              const write = cloneJson(raw); commitAttempted = true;
              const result = cloneJson(await abortable(repository.commit(write), controller.signal));
              ensure(result.operationId === operationId && ['committed', 'partial', 'failed', 'unknown'].includes(result.status), 'Invalid repository receipt');
              if (result.status === 'committed') ensure(canonical(result.batchRef) === canonical(write.batch.ref) && canonical(result.evidenceRef) === canonical(write.evidence.ref) && canonical(result.observationRefs) === canonical(write.observations.map(item => item.ref)), 'Repository receipt does not acknowledge the supplied records');
              acknowledgedReceipt = result;
              return result;
            };
            const chart = kind === 'collect' ? collectFlow({ source: source!,
              acquire: async input => validateCandidate(await abortable(adapter!.collect(source!, freeze(cloneJson(input.request)), context), controller.signal), limits), prepare, publish })
              : deriveFlow({
                load: async input => {
                  const loaded: BatchInput[] = [];
                  const batchRecords = await abortable(readRepositoryRecords(repository, input.inputBatchRefs, controller.signal), controller.signal);
                  const batches: Batch[] = batchRecords.map((batch, i) => {
                    ensure(batch?.kind === 'batch' && canonical(batch.ref) === canonical(input.inputBatchRefs[i]), 'Input batch is missing or mismatched');
                    return batch;
                  });
                  const records = await abortable(readRepositoryRecords(repository, batches.flatMap(batch => batch.observationRefs), controller.signal), controller.signal);
                  let position = 0;
                  for (const batch of batches) {
                    const batchRef = batch.ref;
                    const observations: Observation[] = [];
                    for (const observationRef of batch.observationRefs) {
                      const item = records[position++];
                      ensure(item?.kind === 'observation' && canonical(item.ref) === canonical(observationRef) && canonical(item.batchRef) === canonical(batchRef), 'Input observation is missing or mismatched');
                      const time = item.time, start = Date.parse(input.window.start), end = Date.parse(input.window.end);
                      let include: boolean;
                      if (input.window.basis === 'collection') { assertTimestamp(item.collectedAt); const at = Date.parse(item.collectedAt); include = at >= start && at < end; }
                      else if (time.kind === 'source-time-unknown' || time.basis !== 'source') { excludedUnknownTimes++; include = false; }
                      else {
                        include = time.kind === 'point' ? Date.parse(time.at) >= start && Date.parse(time.at) < end : Date.parse(time.start) >= start && Date.parse(time.end) <= end;
                        // An interval straddling the requested boundary cannot be split without domain-specific weighting.
                        if (time.kind === 'interval' && !include && Date.parse(time.start) < end && Date.parse(time.end) > start) excludedUnknownTimes++;
                      }
                      if (include) observations.push(cloneJson(item));
                    }
                    loaded.push({ batch: cloneJson(batch), observations });
                  }
                  selectedInputs = freeze(loaded); return selectedInputs;
                },
                apply: async (inputs, input) => validateCandidate(await abortable(transform!.apply(freeze(cloneJson(inputs)), freeze(cloneJson(input)), context), controller.signal), limits), prepare, publish,
              });
            executor = kind === 'collect'
              ? new FlowChartExecutor(chart as ReturnType<typeof collectFlow>)
              : new FlowChartExecutor(chart as ReturnType<typeof deriveFlow>);
            executor.setReadTracking('full');
            executor.attachCombinedRecorder(inOutRecorder());
            await executor.run({ input: { invocation: request }, signal: controller.signal });
            const state = executor.getSnapshot().sharedState as { receipt?: CommitReceipt };
            ensure(state.receipt, 'Flow did not produce a commit receipt'); receipt = cloneJson(state.receipt);
          }
        } catch (error) {
          receipt = acknowledgedReceipt ?? { operationId, status: commitAttempted ? 'unknown' : 'failed', error: errorRecord(error) };
          if (commitAttempted && !acknowledgedReceipt && !controller.signal.aborted) {
            try {
              const recovered = await abortable(repository.findOperation(operationId), controller.signal);
              if (recovered?.fingerprint === fingerprint) receipt = cloneJson(recovered.receipt);
              else if (recovered) receipt = { operationId, status: 'failed', error: { code: 'IDEMPOTENCY_CONFLICT', message: 'Another request committed with this idempotency key' } };
            } catch { /* Preserve unknown outcome for reconciliation on retry. */ }
          }
        }
        if (!traceStore) return finish({ commit: receipt, trace: { status: 'disabled' } });
        try {
          controller.signal.throwIfAborted();
          const value = JSON.parse(JSON.stringify(executor ? executor.getSnapshot() : { operationId, request, receipt, recovered: true })) as JsonValue;
          const traceRef = await abortable(traceStore.store({ operationId, recordedAt: now(), value }), controller.signal);
          ensure(traceRef.kind === 'trace' && typeof traceRef.id === 'string' && traceRef.id.length, 'Invalid trace receipt');
          return finish({ commit: receipt, trace: { status: 'stored', ref: cloneJson(traceRef) } });
        } catch (error) { return finish({ commit: receipt, trace: { status: 'unavailable', error: errorRecord(error) } }); }
      }
    } catch (error) { return { commit: { operationId, status: 'failed', error: errorRecord(error) }, trace: { status: 'disabled' } }; }
  }
  return {
    collect: (request, execution) => execute('collect', request, execution),
    reconcileCollect,
    derive: (request, execution) => execute('derive', request, execution),
    read: async ref => { const value = await repository.read(cloneJson(ref)); return value === undefined ? undefined : cloneJson(value); },
  };
}
