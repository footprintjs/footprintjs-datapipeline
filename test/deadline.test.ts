import { describe, expect, it, vi } from 'vitest';
import { createPipeline } from '../src/createPipeline.js';
import type { CandidateBatch, CommitReceipt, Source, StoredRecord } from '../src/contracts.js';
import type { CommitWrite, OperationCommit, Repository, SourceAdapter, TraceStore } from '../src/ports.js';

const source: Source = {
  ref: { id: 'deadline-source', version: '1' },
  adapterRef: { id: 'deadline-adapter', version: '1' },
};
const request = { sourceRef: source.ref, idempotencyKey: 'deadline-capture', request: {} };
const never = <T>(): Promise<T> => new Promise(() => {});
const candidate: CandidateBatch = {
  key: 'capture',
  outcome: 'complete',
  coverage: { status: 'unknown', time: { kind: 'source-time-unknown' } },
  observations: [],
};
const adapter: SourceAdapter = { ref: source.adapterRef, async collect() { return candidate; } };

function receipt(write: CommitWrite): CommitReceipt {
  return {
    operationId: write.operationId,
    status: 'committed',
    batchRef: write.batch.ref,
    evidenceRef: write.evidence.ref,
    observationRefs: write.observations.map(item => item.ref),
  };
}

/** A broken deadline fails this test promptly instead of hanging the test runner. */
async function bounded<T>(work: Promise<T>): Promise<T> {
  let guard: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        guard = setTimeout(() => reject(new Error('Operation did not return after its deadline')), 500);
      }),
    ]);
  } finally {
    if (guard) clearTimeout(guard);
  }
}

describe('pipeline deadlines at persistence boundaries', () => {
  it('returns unknown for a hung commit without starting unbounded recovery after abort', async () => {
    let lookups = 0;
    const findOperation = vi.fn((): Promise<OperationCommit | undefined> => {
      lookups++;
      return lookups === 1 ? Promise.resolve(undefined) : never();
    });
    const commit = vi.fn(() => never<CommitReceipt>());
    const repository: Repository = { findOperation, commit, async read() { return undefined; } };
    const pipeline = createPipeline({ sources: [source], adapters: [adapter], repository });

    const result = await bounded(pipeline.collect(request, { timeoutMs: 20 }));

    expect(result.commit.status).toBe('unknown');
    expect(result.commit.error).toBeDefined();
    expect(result.trace.status).toBe('disabled');
    expect(commit).toHaveBeenCalledTimes(1);
    expect(findOperation).toHaveBeenCalledTimes(1);
  });

  it('bounds a hung acknowledgement-loss reconciliation with the same deadline', async () => {
    let lookups = 0;
    const findOperation = vi.fn((): Promise<OperationCommit | undefined> => {
      lookups++;
      return lookups === 1 ? Promise.resolve(undefined) : never();
    });
    const repository: Repository = {
      findOperation,
      async commit() { throw new Error('Acknowledgement lost'); },
      async read() { return undefined; },
    };
    const pipeline = createPipeline({ sources: [source], adapters: [adapter], repository });

    const result = await bounded(pipeline.collect(request, { timeoutMs: 20 }));

    expect(result.commit.status).toBe('unknown');
    expect(findOperation).toHaveBeenCalledTimes(2);
  });

  it('reports a proven cross-instance fingerprint conflict as failed', async () => {
    let competing: OperationCommit | undefined;
    const findOperation = vi.fn(async () => competing);
    const commit = vi.fn(async (write: CommitWrite): Promise<CommitReceipt> => {
      // Another writer won after this caller's initial absent-operation lookup.
      competing = { fingerprint: 'b'.repeat(64), receipt: receipt(write) };
      throw Object.assign(new Error('Another request committed this operation'), {
        code: 'idempotency_conflict',
      });
    });
    const repository: Repository = { findOperation, commit, async read() { return undefined; } };
    const pipeline = createPipeline({ sources: [source], adapters: [adapter], repository });

    const result = await bounded(pipeline.collect(request));

    expect(result.commit.status).toBe('failed');
    expect(result.commit.error?.code).toBe('IDEMPOTENCY_CONFLICT');
    expect(commit).toHaveBeenCalledTimes(1);
    expect(findOperation).toHaveBeenCalledTimes(2);
  });

  it('returns committed data and an unavailable trace when trace storage hangs', async () => {
    const records = new Map<string, StoredRecord>();
    const repository: Repository = {
      async findOperation() { return undefined; },
      async commit(write) {
        for (const record of [write.batch, ...write.observations, write.evidence]) {
          records.set(record.ref.id, record);
        }
        return receipt(write);
      },
      async read(ref) { return records.get(ref.id); },
    };
    const store = vi.fn<TraceStore['store']>(() => never());
    const pipeline = createPipeline({ sources: [source], adapters: [adapter], repository, traceStore: { store } });

    const result = await bounded(pipeline.collect(request, { timeoutMs: 20 }));

    expect(result.commit.status).toBe('committed');
    expect(result.trace.status).toBe('unavailable');
    expect(store).toHaveBeenCalledTimes(1);
    expect(await pipeline.read(result.commit.batchRef!)).toMatchObject({ kind: 'batch' });
    expect(await pipeline.read(result.commit.evidenceRef!)).toMatchObject({ kind: 'evidence' });
  });
});
