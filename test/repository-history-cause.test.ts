import { join } from 'node:path';
import { expect, it } from 'vitest';
import { flowChart, type RuntimeSnapshot } from 'footprintjs';
import { findLastWriter, keysReadFromExecutionTree } from 'foottrace';
import { createRecordGraph, explain } from 'foottrace/cause';
import { runWindowFlow, type CommitWrite, type JsonValue, type ObservationRef } from '../src/index.js';
import { createFilesystemRepository } from '../src/storage/filesystem.js';
import { fixture, scopedWrite, DAY } from './helpers/history-repository.js';
it('reopens the identical cause trail with missing polls separate, then honestly retires its inputs', async () => {
  const f = await fixture(), stores = f.open(0);
  try {
    const source = await scopedWrite(stores.repository, 'source'); await stores.repository.commit(source);
    interface Scope { input: { ref: ObservationRef; value: number }; gap: string; statistics: { count: number; mean: number }; coverage: string }
    const chart = flowChart<Scope>('Read exact saved sample', async scope => {
      const input = await stores.repository.read(source.observations[0]!.ref);
      if (input?.kind !== 'observation') throw Error('Saved input unavailable');
      scope.input = { ref: input.ref, value: (input.payload as { latency_us: number }).latency_us };
    }, 'read-input').addFunction('Record missing poll', scope => { scope.gap = 'not-collected'; }, 'gap')
      .addFunction('Summarize only saved input', scope => { const input = scope.input; scope.statistics = { count: 1, mean: input.value }; }, 'summary')
      .addFunction('Keep coverage separate', scope => { scope.coverage = scope.gap; }, 'coverage').build();
    const native = await runWindowFlow({ operationId: 'native-history-proof', chart });
    const seed = await scopedWrite(stores.repository, 'record');
    const value = JSON.parse(JSON.stringify(native.record)) as JsonValue;
    const { externalArtifactRef: _external, ...inline } = seed.observations[0]!;
    const write: CommitWrite = { ...seed, observations: [{ ...inline, payload: value }] };
    const plain = createFilesystemRepository({ directory: join(f.directory, 'comparison'), packing: 'gzip' });
    // Comparison store is an unrelated test artifact, not governed by this retention policy.
    const { committedAt: _a, ...a } = await stores.repository.commit(write);
    const { committedAt: _b, ...b } = await plain.commit(write); expect(a).toEqual(b);
    stores.repository.close();
    const reopened = f.open(0), saved = await reopened.repository.read(write.observations[0]!.ref);
    expect(saved).toEqual(await plain.read(write.observations[0]!.ref));
    const record = saved!.kind === 'observation' ? saved!.payload as unknown as RuntimeSnapshot : undefined;
    async function cause(snapshot: RuntimeSnapshot) {
      const reads = keysReadFromExecutionTree(snapshot.executionTree);
      const graph = createRecordGraph([{ address: snapshot.logAddress!, commitLog: snapshot.commitLog, complete: true,
        reads: Object.fromEntries(snapshot.commitLog.map(commit => [commit.runtimeStageId, reads.lookup(commit.runtimeStageId)])) }]);
      const node = (key: string) => { const writer = findLastWriter(snapshot.commitLog, key)!; return graph.nodeId(snapshot.logAddress!,
        { key, commitIdx: snapshot.commitLog.indexOf(writer), runtimeStageId: writer.runtimeStageId }); };
      const parents = await graph.parents(node('statistics'), { signal: new AbortController().signal });
      expect(parents.edges).toContainEqual(expect.objectContaining({ nodeId: node('input'), kind: 'data', attribution: 'per-write' }));
      expect(parents.edges.some(edge => edge.nodeId === node('gap'))).toBe(false);
      return explain(graph, node('statistics'));
    }
    expect(record!.sharedState.statistics).toEqual({ count: 1, mean: 12 }); expect(record!.sharedState.coverage).toBe('not-collected');
    expect(await cause(record!)).toEqual(await cause(native.record));
    expect((record!.sharedState.input as { ref: unknown }).ref).toEqual(source.observations[0]!.ref);
    // Reopening alone credits no downtime. Advance a handle that observed zero.
    await reopened.repository.historyStatus(); f.at(8 * DAY); await reopened.repository.sweep({ limit: 100 });
    for (const ref of [source.observations[0]!.ref, write.observations[0]!.ref])
      await expect(reopened.repository.read(ref)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
  } finally { await f.close(); }
});
