import type { RecordRef, StoredRecord } from '../contracts.js';
import type { CommitWrite } from '../ports.js';
import { FilesystemRepositoryError } from './filesystemError.js';
export const sameRef = (a: RecordRef, b: RecordRef): boolean => a.kind === b.kind && a.id === b.id;
export const refKey = (ref: RecordRef): string => `${ref.kind}:${ref.id}`;
export function validRef(value: unknown): value is RecordRef {
  const ref = value as RecordRef | undefined;
  return !!ref && ['batch', 'observation', 'evidence'].includes(ref.kind) && typeof ref.id === 'string' && ref.id.length > 0;
}

export function checkedWrite(write: CommitWrite): StoredRecord[] {
  const fail = (message: string): never => { throw new FilesystemRepositoryError('invalid_commit', message); };
  if (!write.operationId || !write.fingerprint) fail('An operation ID and request fingerprint are required.');
  if (write.batch.operationId !== write.operationId || write.evidence.operationId !== write.operationId) {
    fail('Batch and evidence must belong to the committed operation.');
  }
  if (write.batch.kind !== 'batch' || write.evidence.kind !== 'evidence' ||
      write.observations.some(item => item.kind !== 'observation') || write.evidence.fingerprint !== write.fingerprint) {
    fail('Record kinds and evidence fingerprint must match the commit.');
  }
  const records: StoredRecord[] = [write.batch, ...write.observations, write.evidence];
  const keys = new Set<string>();
  for (const record of records) {
    if (!validRef(record.ref) || record.kind !== record.ref.kind) fail('A record must have its own matching reference.');
    if (keys.has(refKey(record.ref))) fail('A commit cannot contain duplicate record references.');
    keys.add(refKey(record.ref));
  }
  if (!sameRef(write.batch.evidenceRef, write.evidence.ref) || write.batch.outcome !== write.evidence.outcome) {
    fail('Batch and evidence must agree on evidence reference and outcome.');
  }
  const supplied = new Set(write.observations.map(item => refKey(item.ref)));
  if (write.batch.observationRefs.length !== supplied.size || new Set(write.batch.observationRefs.map(refKey)).size !== supplied.size ||
      write.batch.observationRefs.some(ref => !supplied.has(refKey(ref)))) fail('Batch membership must match the supplied observations.');
  for (const observation of write.observations) {
    if (observation.externalArtifactRef !== undefined) {
      throw new FilesystemRepositoryError('external_artifact_unsupported', 'This repository does not verify or store external artifact bytes; inline the payload or use an artifact-aware repository.');
    }
    if (!Object.prototype.hasOwnProperty.call(observation, 'payload') || observation.payload === undefined) {
      fail('An observation must contain an inline JSON payload.');
    }
    if (!sameRef(observation.batchRef, write.batch.ref) || !sameRef(observation.evidenceRef, write.evidence.ref)) {
      fail('Every observation must reference this batch and evidence.');
    }
  }
  const outputs = new Set(write.evidence.outputRefs.map(refKey));
  const expected = [write.batch.ref, ...write.observations.map(item => item.ref)];
  if (outputs.size !== expected.length || write.evidence.outputRefs.length !== expected.length ||
      expected.some(ref => !outputs.has(refKey(ref)))) fail('Evidence outputs must match this batch and observations.');
  return records;
}

