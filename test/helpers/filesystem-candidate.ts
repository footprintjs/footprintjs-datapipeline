import type { BatchOutcome, Evidence, Observation } from '../../src/contracts.js';
import type { CommitWrite } from '../../src/ports.js';

export function candidate(operationId = 'collect:one', fingerprint = 'request-one', value = 12, outcome: BatchOutcome = 'complete'): CommitWrite {
  const batchRef = { kind: 'batch' as const, id: `${operationId}:batch` };
  const evidenceRef = { kind: 'evidence' as const, id: `${operationId}:evidence` };
  const observationRef = { kind: 'observation' as const, id: `${operationId}:sample` };
  const sourceRef = { id: 'array-a', version: 'config-1' };
  const timestamp = '2026-10-16T13:00:00.000Z';
  const time = { kind: 'point' as const, basis: 'source' as const, at: timestamp };
  const observation: Observation = {
    kind: 'observation', ref: observationRef, key: 'sample', batchRef, evidenceRef,
    subject: { node: 1 }, payload: { latency_us: value }, schemaRef: { id: 'latency', version: '1' },
    time, support: { kind: 'source', sourceRef },
  };
  const evidence: Evidence = {
    kind: 'evidence', ref: evidenceRef, operationId, fingerprint,
    origin: { kind: 'collect', sourceRef, adapterRef: { id: 'vendor', version: '1' }, sourceConfigFingerprint: 'configuration-digest', request: {} },
    startedAt: timestamp, finishedAt: timestamp, outcome, outputRefs: [batchRef, observationRef],
  };
  return {
    operationId, fingerprint, observations: [observation], evidence,
    batch: { kind: 'batch', ref: batchRef, key: 'capture', operationId, observationRefs: [observationRef], evidenceRef,
      coverage: { status: outcome === 'partial' ? 'partial' : 'unknown', time }, outcome,
      startedAt: timestamp, finishedAt: timestamp, sourceRef },
  };
}
