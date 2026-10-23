import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, it } from 'vitest';
import { flowChart, type RuntimeSnapshot } from 'footprintjs';
import { findLastWriter, keysReadFromExecutionTree } from 'foottrace';
import { createRecordGraph, explain } from 'foottrace/cause';
import { runWindowFlow, type CommitWrite, type JsonValue } from '../src/index.js';
import { createFilesystemRepository, createRetainedFilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

it('clock-safe storage reopens identical native cause answers and exact source references, with gaps separate from numbers', async () => {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'clock-cause-')));
  let wall = 1_790_000_000_000;
  const repo = createRetainedFilesystemRepository({ enabled: true, directory: join(directory, 'guarded'),
    retentionMs: 1000, expiryClock: 'verified-uptime', clock: () => wall, monotonicClock: () => 0,
    requireSeparateFilesystem: false, onHealth() {} })!;
  try {
    const source = candidate('source'); await repo.commit(source);
    interface Scope { input: { ref: typeof source.observations[0]['ref']; value: number }; gap: string; statistics: { count: number; mean: number }; coverage: string }
    const chart = flowChart<Scope>('Read saved input', async scope => {
      const input = await repo.read(source.observations[0]!.ref);
      if (input?.kind !== 'observation') throw Error('Missing saved input');
      scope.input = { ref: input.ref, value: (input.payload as { latency_us: number }).latency_us };
    }, 'read-input').addFunction('Record absent poll', scope => { scope.gap = 'not-collected'; }, 'missing-poll')
      .addFunction('Summarize only saved values', scope => {
        const input = scope.input; scope.statistics = { count: 1, mean: input.value };
      }, 'summary').addFunction('Keep coverage separate', scope => { scope.coverage = scope.gap; }, 'coverage').build();
    const native = await runWindowFlow({ operationId: 'clock-cause-proof', chart });
    const value = JSON.parse(JSON.stringify(native.record)) as JsonValue;
    const seed = candidate('saved-native');
    const { externalArtifactRef: _external, ...inline } = seed.observations[0]!;
    const write: CommitWrite = { ...seed, observations: [{ ...inline, payload: value }] };
    const plain = createFilesystemRepository({ directory: join(directory, 'plain'), packing: 'gzip' });
    // Separate publications have different acknowledgement times; their logical
    // identity, references and every saved record must still be identical.
    const { committedAt: _guardedAt, ...guardedReceipt } = await repo.commit(write);
    const { committedAt: _plainAt, ...plainReceipt } = await plain.commit(write);
    expect(guardedReceipt).toEqual(plainReceipt);
    wall += 100 * 365 * 86400_000;
    const saved = await repo.read(write.observations[0]!.ref);
    expect(saved).toEqual(await plain.read(write.observations[0]!.ref));
    expect(saved?.kind).toBe('observation');
    const reopened = (saved!.kind === 'observation' ? saved!.payload : undefined) as unknown as RuntimeSnapshot;
    async function cause(record: RuntimeSnapshot) {
      const reads = keysReadFromExecutionTree(record.executionTree);
      const graph = createRecordGraph([{ address: record.logAddress!, commitLog: record.commitLog, complete: true,
        reads: Object.fromEntries(record.commitLog.map(commit => [commit.runtimeStageId, reads.lookup(commit.runtimeStageId)])) }]);
      const node = (key: string) => { const writer = findLastWriter(record.commitLog, key)!; return graph.nodeId(record.logAddress!,
        { key, commitIdx: record.commitLog.indexOf(writer), runtimeStageId: writer.runtimeStageId }); };
      const context = { signal: new AbortController().signal };
      const parents = await graph.parents(node('statistics'), context);
      expect(parents.edges).toContainEqual(expect.objectContaining({ nodeId: node('input'), kind: 'data', attribution: 'per-write' }));
      expect(parents.edges.some(edge => edge.nodeId === node('gap'))).toBe(false);
      return explain(graph, node('statistics'));
    }
    expect(reopened.sharedState.statistics).toEqual({ count: 1, mean: 12 });
    expect(reopened.sharedState.coverage).toBe('not-collected');
    expect(await cause(reopened)).toEqual(await cause(native.record));
    expect((reopened.sharedState.input as { ref: unknown }).ref).toEqual(source.observations[0]!.ref);
  } finally { repo.close(); await rm(directory, { recursive: true, force: true }); }
});
