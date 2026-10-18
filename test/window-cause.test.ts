import { describe, expect, it } from 'vitest';
import { decide, flowChart, FlowChartExecutor, type RuntimeSnapshot } from 'footprintjs';
import {
  commitValueAt,
  findLastWriter,
  flattenCausalDAG,
  keysReadFromExecutionTree,
  readRequirementResults,
  sliceForKey,
  type Attention,
  type RequirementResult,
} from 'foottrace';
import {
  attentionList,
  createRecordGraph,
  explain,
  type RecordCauseGraph,
  type RecordScope,
} from 'foottrace/cause';
import { runWindowFlow } from '../src/index.js';

// Synthetic acquisition fixture, not PowerScale data or a physical-cause oracle.
// Raw response bytes stay outside the record; the native writes retain exact row references.
const RAW_BODY_SENTINEL = 'RAW_RESPONSE_BODY_STAYS_IN_THE_AUTHORIZED_SOURCE_STORE';
const WINDOW_A = 'window:2026-10-18T10:00:00Z/2026-10-18T10:05:00Z';
const WINDOW_B = 'window:2026-10-18T10:05:00Z/2026-10-18T10:10:00Z';

interface Poll {
  windowRef: string;
  rawRef: string;
  sourceRef: string;
  sourceOperationWindow: 'unknown';
}

interface Samples {
  windowRef: string;
  pollRef: string;
  rowRefs: string[];
  valuesMs: number[];
  reportedMaximumMs: number;
  sourceNode: string;
}

interface Statistic {
  windowRef: string;
  count: number;
  meanMs: number;
  reportedMaximumMs: number;
  sourceNode: string;
  rowRefs: string[];
}

interface WindowState {
  pollA: Poll;
  pollB: Poll;
  samplesA: Samples;
  samplesB: Samples;
  statsA: Statistic;
  statsB: Statistic;
  meanA: number;
  mappingA: RequirementResult;
  mappingAttentionA: Attention;
  findingA: { windowRef: string; statisticKey: string; attribution: 'unverified'; requirementResultRef: string };
  findingAttentionA: Attention;
  quietA: boolean;
}

function chartFor(windowRef: string) {
  const raw = {
    a: { rawRef: `fixture:raw/a/${windowRef}`, valuesMs: [20, 60], maximumMs: 1500, node: '2', body: RAW_BODY_SENTINEL },
    b: { rawRef: `fixture:raw/b/${windowRef}`, valuesMs: [800, 1200], maximumMs: 9000, node: '9', body: RAW_BODY_SENTINEL },
  };
  return flowChart<WindowState>('Capture A poll receipt', scope => {
    scope.pollA = { windowRef, rawRef: raw.a.rawRef, sourceRef: 'fixture:resource/a', sourceOperationWindow: 'unknown' };
  }, 'poll-a')
    .addFunction('Capture B poll receipt', scope => {
      scope.pollB = { windowRef, rawRef: raw.b.rawRef, sourceRef: 'fixture:resource/b', sourceOperationWindow: 'unknown' };
    }, 'poll-b')
    .addFunction('Capture exact A sample references', scope => {
      const poll = scope.pollA;
      scope.samplesA = {
        windowRef: poll.windowRef, pollRef: poll.rawRef,
        rowRefs: [`${poll.rawRef}#/rows/0`, `${poll.rawRef}#/rows/1`],
        valuesMs: raw.a.valuesMs, reportedMaximumMs: raw.a.maximumMs, sourceNode: raw.a.node,
      };
    }, 'samples-a')
    .addFunction('Capture exact B sample references', scope => {
      const poll = scope.pollB;
      scope.samplesB = {
        windowRef: poll.windowRef, pollRef: poll.rawRef,
        rowRefs: [`${poll.rawRef}#/rows/0`, `${poll.rawRef}#/rows/1`],
        valuesMs: raw.b.valuesMs, reportedMaximumMs: raw.b.maximumMs, sourceNode: raw.b.node,
      };
    }, 'samples-b')
    .addFunction('Summarize A retained samples', scope => {
      const samples = scope.samplesA;
      const meanMs = samples.valuesMs.reduce((sum, value) => sum + value, 0) / samples.valuesMs.length;
      scope.statsA = {
        windowRef: samples.windowRef, count: samples.valuesMs.length, meanMs,
        reportedMaximumMs: samples.reportedMaximumMs, sourceNode: samples.sourceNode, rowRefs: samples.rowRefs,
      };
      scope.meanA = meanMs;
    }, 'stats-a')
    .addFunction('Summarize B retained samples independently', scope => {
      const samples = scope.samplesB;
      scope.statsB = {
        windowRef: samples.windowRef, count: samples.valuesMs.length,
        meanMs: samples.valuesMs.reduce((sum, value) => sum + value, 0) / samples.valuesMs.length,
        reportedMaximumMs: samples.reportedMaximumMs, sourceNode: samples.sourceNode, rowRefs: samples.rowRefs,
      };
    }, 'stats-b')
    .addFunction('Record uncollected external mapping', scope => {
      const statistic = scope.statsA;
      const result: RequirementResult = {
        id: `requirement-result:vm-a/${statistic.windowRef}`,
        requirementRef: 'requirement:client-node-vm-inventory', findingRef: `finding:a/${statistic.windowRef}`,
        windowRef: statistic.windowRef, affects: [`claim:vm-attribution/a/${statistic.windowRef}`],
        state: 'unresolved', reason: 'not-collected', detail: 'fixture:client-node-vm-mapping',
        evidenceRefs: statistic.rowRefs, checks: [], actionRefs: [],
      };
      scope.mappingA = result;
      scope.mappingAttentionA = {
        kind: 'not-collected', reason: 'EVIDENCE_NOT_COLLECTED', evidence: [result.id, result.requirementRef],
      };
    }, 'mapping-a')
    .addDeciderFunction('Apply A sample-mean rule', scope => decide(scope, [
      { when: { meanA: { gt: 30 } }, then: 'attention', label: 'Retained sample mean exceeds 30 ms' },
    ], { branch: 'quiet', label: 'Mean does not exceed the declared rule' }), 'rule-a')
      .addFunctionBranch('attention', 'Publish A finding', scope => {
        const statistic = scope.statsA;
        const requirement = scope.mappingA;
        scope.findingA = {
          windowRef: statistic.windowRef, statisticKey: 'statsA',
          attribution: 'unverified', requirementResultRef: requirement.id,
        };
        scope.findingAttentionA = {
          kind: 'outlier', reason: 'BUSINESS_RULE_MATCHED', evidence: [requirement.findingRef, 'rule:sample-mean-over-30ms'],
        };
      })
      .addFunctionBranch('quiet', 'Publish no A alert', scope => { scope.quietA = true; })
      .setDefault('quiet')
      .end()
    .build();
}

function writer(record: RuntimeSnapshot, key: string) {
  const found = findLastWriter(record.commitLog, key);
  if (!found) throw new Error(`Missing native writer for ${key}`);
  return found;
}

function controlCapture(record: RuntimeSnapshot) {
  const bundle = record.recorders?.find(item => item.name === 'window-control-dependencies');
  const data = bundle?.data;
  if (data === null || typeof data !== 'object' || !('controlDeps' in data) || !('decisions' in data)) {
    throw new Error('Native control-dependency capture is required');
  }
  if (!Array.isArray(data.decisions) || data.controlDeps === null || typeof data.controlDeps !== 'object') {
    throw new Error('Malformed native control-dependency capture');
  }
  const controlDeps: Record<string, { deciderId: string }> = {};
  for (const [stage, dependency] of Object.entries(data.controlDeps)) {
    if (dependency === null || typeof dependency !== 'object' || !('deciderId' in dependency) || typeof dependency.deciderId !== 'string') {
      throw new Error('Malformed native control parent');
    }
    controlDeps[stage] = { deciderId: dependency.deciderId };
  }
  return { controlDeps, decisions: data.decisions };
}

function addressOf(record: RuntimeSnapshot) {
  if (!record.logAddress) throw new Error('Native record address is required');
  return record.logAddress;
}

// This is an explicit, authorized fixture-side projection of declarations made
// by the chart, not inferred domain truth or a second dependency traversal.
function scopeFor(record: RuntimeSnapshot): RecordScope {
  const reads = keysReadFromExecutionTree(record.executionTree);
  const marks: Record<string, readonly Attention[]> = {};
  for (const [valueKey, markKey] of [['mappingA', 'mappingAttentionA'], ['findingA', 'findingAttentionA']]) {
    const valueWriter = writer(record, valueKey!);
    const mark = record.sharedState[markKey!];
    if (mark === null || typeof mark !== 'object' || !('kind' in mark) || !('reason' in mark) || !('evidence' in mark)) {
      throw new Error('Missing declared domain attention');
    }
    if (mark.kind !== 'not-collected' && mark.kind !== 'outlier') throw new Error('Unexpected fixture attention');
    if (mark.reason !== 'EVIDENCE_NOT_COLLECTED' && mark.reason !== 'BUSINESS_RULE_MATCHED') throw new Error('Unexpected fixture reason');
    if (!Array.isArray(mark.evidence) || !mark.evidence.every(ref => typeof ref === 'string')) throw new Error('Malformed attention references');
    marks[valueWriter.runtimeStageId] = [{ kind: mark.kind, reason: mark.reason, evidence: mark.evidence }];
  }
  return {
    address: addressOf(record), commitLog: record.commitLog, complete: true,
    reads: Object.fromEntries(record.commitLog.map(commit => [commit.runtimeStageId, reads.lookup(commit.runtimeStageId)])),
    controlDeps: controlCapture(record).controlDeps, attention: marks,
  };
}

function idFor(graph: RecordCauseGraph, record: RuntimeSnapshot, key: string) {
  const found = writer(record, key);
  return graph.nodeId(addressOf(record), {
    runtimeStageId: found.runtimeStageId, commitIdx: record.commitLog.indexOf(found), key,
  });
}

const context = () => ({ signal: new AbortController().signal });

describe('one native collection-window record with public cause readers', () => {
  it('traces a finding through its own statistic, exact samples and poll without the unrelated resource', async () => {
    const { record, trace } = await runWindowFlow({ operationId: 'window-acceptance:a', chart: chartFor(WINDOW_A) });
    expect(trace.status).toBe('disabled');
    expect(record.writeProvenance).toBe('reads-prefix');
    expect(record.runId).toBeTruthy();
    expect(record.sharedState.statsA).toEqual({
      windowRef: WINDOW_A, count: 2, meanMs: 40, reportedMaximumMs: 1500, sourceNode: '2',
      rowRefs: [`fixture:raw/a/${WINDOW_A}#/rows/0`, `fixture:raw/a/${WINDOW_A}#/rows/1`],
    });
    expect(record.sharedState.pollA).toMatchObject({ sourceOperationWindow: 'unknown' });
    expect(JSON.stringify(record)).not.toContain(RAW_BODY_SENTINEL);

    const slice = sliceForKey(record.commitLog, 'findingA', keysReadFromExecutionTree(record.executionTree), {
      edgeAttribution: 'per-write', controlDeps: id => controlCapture(record).controlDeps[id],
    });
    expect(slice.root).toBeDefined();
    const stages = flattenCausalDAG(slice.root!).map(node => node.stageId);
    expect(stages).toEqual(expect.arrayContaining(['stats-a', 'samples-a', 'poll-a', 'mapping-a', 'rule-a']));
    expect(stages).not.toEqual(expect.arrayContaining(['stats-b']));
    expect(stages).not.toEqual(expect.arrayContaining(['samples-b']));
    expect(stages).not.toEqual(expect.arrayContaining(['poll-b']));
    expect(writer(record, 'statsA').trace.find(row => row.path === 'statsA')?.readKeys).toEqual(['samplesA']);

    const graph = createRecordGraph([scopeFor(record)]);
    const statistic = await graph.parents(idFor(graph, record, 'statsA'), context());
    expect(statistic.edges).toContainEqual(expect.objectContaining({
      nodeId: idFor(graph, record, 'samplesA'), kind: 'data', attribution: 'per-write',
    }));
    const samples = await graph.parents(idFor(graph, record, 'samplesA'), context());
    expect(samples.edges).toContainEqual(expect.objectContaining({ nodeId: idFor(graph, record, 'pollA'), kind: 'data' }));
    expect(samples.edges.some(edge => edge.nodeId === idFor(graph, record, 'pollB'))).toBe(false);
  });

  it('retains a real decide rule, recorded control edge and explicit requirement gap without a definite physical root', async () => {
    const { record } = await runWindowFlow({ operationId: 'window-acceptance:gap', chart: chartFor(WINDOW_A) });
    const captured = controlCapture(record);
    expect(captured.decisions).toContainEqual(expect.objectContaining({
      evidence: expect.objectContaining({ chosen: 'attention', rules: [expect.objectContaining({
        type: 'filter', matched: true, conditions: [expect.objectContaining({ key: 'meanA', op: 'gt', threshold: 30, result: true })],
      })] }),
    }));
    const [requirement] = readRequirementResults([record.sharedState.mappingA]);
    expect(requirement).toMatchObject({
      state: 'unresolved', reason: 'not-collected', requirementRef: 'requirement:client-node-vm-inventory',
      windowRef: WINDOW_A, checks: [], actionRefs: [],
    });
    const graph = createRecordGraph([scopeFor(record)]);
    const findingId = idFor(graph, record, 'findingA');
    const mappingId = idFor(graph, record, 'mappingA');
    const governing = writer(record, 'meanA');
    const decision = captured.decisions.find(item => typeof item === 'object' && item !== null && 'evidence' in item);
    expect(decision).toMatchObject({ deciderRuntimeStageId: expect.any(String) });
    const parents = await graph.parents(findingId, context());
    expect(parents.edges).toContainEqual(expect.objectContaining({ kind: 'control', attribution: 'recorded-control' }));
    expect(governing.trace.some(row => row.path === 'meanA')).toBe(true);

    const result = await explain(graph, findingId);
    expect(result.confidence).toBe('incomplete');
    expect(result.rootCause).toBeUndefined();
    expect(result.gaps).toContainEqual(expect.objectContaining({ code: 'not-collected', nodeId: mappingId }));
    expect(await graph.attention(mappingId, context())).toEqual([{
      kind: 'not-collected', reason: 'EVIDENCE_NOT_COLLECTED', evidence: [requirement!.id, requirement!.requirementRef],
    }]);
    const marked = await attentionList(graph, { nodeIds: [mappingId, findingId] });
    expect(marked.nodes).toContainEqual(expect.objectContaining({
      nodeId: findingId, attention: [expect.objectContaining({ kind: 'outlier', reason: 'BUSINESS_RULE_MATCHED' })],
    }));
    expect(record.sharedState.statsA).toMatchObject({ meanMs: 40, reportedMaximumMs: 1500, sourceNode: '2' });
    const statsWriter = writer(record, 'statsA');
    expect(commitValueAt(record.commitLog, record.commitLog.indexOf(statsWriter), 'statsA')).toEqual(record.sharedState.statsA);
  });

  it('keeps equal resource keys in two windows as independent addressed records with no invented cross-window edge', async () => {
    const first = await runWindowFlow({ operationId: 'window-acceptance:first', chart: chartFor(WINDOW_A) });
    const second = await runWindowFlow({ operationId: 'window-acceptance:second', chart: chartFor(WINDOW_B) });
    expect(addressOf(first.record)).not.toEqual(addressOf(second.record));
    const graph = createRecordGraph([scopeFor(first.record), scopeFor(second.record)]);
    const firstSamples = idFor(graph, first.record, 'samplesA');
    const secondParents = await graph.parents(idFor(graph, second.record, 'statsA'), context());
    expect(secondParents.edges).toContainEqual(expect.objectContaining({ nodeId: idFor(graph, second.record, 'samplesA') }));
    expect(secondParents.edges.some(edge => edge.nodeId === firstSamples)).toBe(false);
    const firstPollChildren = await graph.children(idFor(graph, first.record, 'pollA'), context());
    expect(firstPollChildren.edges.some(edge => edge.nodeId === idFor(graph, second.record, 'samplesA'))).toBe(false);
    const result = await explain(graph, idFor(graph, second.record, 'findingA'));
    expect(result.rootCause).toBeUndefined();
    expect(result.gaps).toContainEqual(expect.objectContaining({ code: 'not-collected' }));
    expect(readRequirementResults([first.record.sharedState.mappingA])[0]?.windowRef).toBe(WINDOW_A);
    expect(readRequirementResults([second.record.sharedState.mappingA])[0]?.windowRef).toBe(WINDOW_B);
  });

  it('does not turn an older native run without captured reads, control or attention into a complete explanation', async () => {
    const executor = new FlowChartExecutor(chartFor(WINDOW_A), { readTracking: 'off', writeProvenance: 'off' });
    await executor.run();
    const record = executor.getSnapshot();
    const graph = createRecordGraph([{ address: addressOf(record), commitLog: record.commitLog }]);
    const result = await explain(graph, idFor(graph, record, 'findingA'));
    expect(result.confidence).toBe('incomplete');
    expect(result.rootCause).toBeUndefined();
    expect(result.gaps).toContainEqual(expect.objectContaining({ code: 'not-collected' }));
    expect(record.writeProvenance).toBe('off');
    expect(writer(record, 'statsA').trace.find(row => row.path === 'statsA')?.readKeys).toBeUndefined();
  });
});
