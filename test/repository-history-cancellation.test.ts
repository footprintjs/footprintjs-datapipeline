import { expect, it } from 'vitest';
import { createPipeline } from '../src/createPipeline.js';
import { fixture, request } from './helpers/history-repository.js';
import type { Repository } from '../src/ports.js';
it('covers identity admission with the caller cancellation boundary and preserves its known ID', async () => {
  const f = await fixture(), store = f.open(0);
  let entered!: () => void, release!: () => void, calls = 0;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  const repository: Repository = {
    identity: { ...store.repository.identity!, async assertOperation() { entered(); await gate; } },
    findOperation: id => store.repository.findOperation(id), commit: write => store.repository.commit(write),
    read: ref => store.repository.read(ref),
  };
  const pipeline = createPipeline({ repository, sources: [{ ref: request('').sourceRef, adapterRef: { id: 'adapter', version: '1' } }],
    adapters: [{ ref: { id: 'adapter', version: '1' }, async collect() { calls++; throw Error('Must not acquire'); } }] });
  const stop = new AbortController();
  try {
    const pending = pipeline.collect(request('cancelled'), { signal: stop.signal });
    await started; stop.abort();
    const result = await pending;
    expect(result.commit.status).toBe('failed'); expect(result.commit.operationId).toMatch(/^fp3:/); expect(calls).toBe(0);
  } finally { release(); await gate; await f.close(); }
});
