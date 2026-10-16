import { describe, expect, it, vi } from 'vitest';
import { createPipeline } from '../src/createPipeline.js';
import type { Source } from '../src/contracts.js';
import type { CommitWrite, Repository } from '../src/ports.js';

const at = '2026-10-16T12:00:00.000Z';
const source: Source = { ref: { id: 'device', version: '1' }, adapterRef: { id: 'samples', version: '1' }, config: { selection: 'bounded' } };
const request = { sourceRef: source.ref, idempotencyKey: 'slot-1', request: { limit: 1 } };
function fixture() {
  const writes = new Map<string, CommitWrite>();
  const collect = vi.fn(async () => ({ key: 'capture', observations: [], outcome: 'complete' as const,
    coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } } }));
  const trace = vi.fn(async () => ({ kind: 'trace' as const, id: 'trace' }));
  const repository: Repository = {
    findOperation: vi.fn(async id => {
      const write = writes.get(id);
      return write ? { fingerprint: write.fingerprint, receipt: { operationId: id, status: 'committed' as const, batchRef: write.batch.ref,
        evidenceRef: write.evidence.ref, observationRefs: write.batch.observationRefs, committedAt: write.batch.finishedAt } } : undefined;
    }),
    commit: vi.fn(async write => {
      writes.set(write.operationId, write);
      return { operationId: write.operationId, status: 'committed' as const, batchRef: write.batch.ref, evidenceRef: write.evidence.ref,
        observationRefs: write.batch.observationRefs, committedAt: write.batch.finishedAt };
    }), read: vi.fn(async () => undefined),
  };
  const build = (registeredSource = source) => createPipeline({ sources: [registeredSource], adapters: [{ ref: source.adapterRef, collect }],
    repository, traceStore: { store: trace }, clock: () => at });
  return { build, collect, trace, repository };
}

describe('lookup-only collection reconciliation', () => {
  it('returns undefined for an absent operation without collecting, publishing, or writing a trace', async () => {
    const f = fixture();
    expect(await f.build().reconcileCollect(request)).toBeUndefined();
    expect(f.collect).not.toHaveBeenCalled(); expect(f.repository.commit).not.toHaveBeenCalled(); expect(f.trace).not.toHaveBeenCalled();
  });

  it('recovers the original committed receipt across instances with zero acquisition and trace writes', async () => {
    const f = fixture(); const captured = await f.build().collect(request);
    f.collect.mockClear(); f.trace.mockClear(); vi.mocked(f.repository.commit).mockClear();
    const recovered = await f.build().reconcileCollect(request);
    expect(recovered?.commit).toEqual(captured.commit);
    expect(recovered?.trace).toMatchObject({ status: 'unavailable', error: { code: 'LOOKUP_ONLY' } });
    expect(f.collect).not.toHaveBeenCalled(); expect(f.repository.commit).not.toHaveBeenCalled(); expect(f.trace).not.toHaveBeenCalled();
  });

  it('uses the same full request and source-configuration fingerprint as collect', async () => {
    const f = fixture(); await f.build().collect(request); f.collect.mockClear();
    expect((await f.build().reconcileCollect({ ...request, request: { limit: 2 } }))?.commit).toMatchObject({ status: 'failed', error: { code: 'IDEMPOTENCY_CONFLICT' } });
    expect((await f.build({ ...source, config: { selection: 'changed' } }).reconcileCollect(request))?.commit).toMatchObject({ status: 'failed', error: { code: 'IDEMPOTENCY_CONFLICT' } });
    expect(f.collect).not.toHaveBeenCalled();
  });

  it('returns the existing failed validation envelope for invalid keys or unresolved source revisions', async () => {
    const f = fixture(); const pipeline = f.build();
    expect((await pipeline.reconcileCollect({ ...request, idempotencyKey: '' }))?.commit.status).toBe('failed');
    expect((await pipeline.reconcileCollect({ ...request, sourceRef: { id: 'missing', version: '1' } }))?.commit.status).toBe('failed');
    expect(f.repository.findOperation).not.toHaveBeenCalled(); expect(f.collect).not.toHaveBeenCalled();
  });
});
