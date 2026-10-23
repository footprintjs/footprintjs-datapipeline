import { createHash } from 'node:crypto';
import type { CommitWrite } from '../../ports.js';
import { canonical, digest } from '../../validation.js';
import { checkedWrite } from '../filesystemRecords.js';
import { FilesystemRepositoryError } from '../filesystemError.js';
import { MAX_PACKED_OPERATION_BYTES } from '../packedOperation.js';
import { RepositoryRetentionError } from './contracts.js';
import type { RepositoryLedger, Row } from './ledger.js';
export const operationHash = (id: string): string => createHash('sha256').update(id).digest('hex');
export function snapshotWrite(input: CommitWrite): CommitWrite {
  const text = canonical(input);
  if (Buffer.byteLength(text) > MAX_PACKED_OPERATION_BYTES) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  const write = JSON.parse(text) as CommitWrite;
  checkedWrite(write);
  return write;
}
export function prepareIntent(write: CommitWrite, ledger: RepositoryLedger, fresh: (row: Row) => void, now: number, retentionMs: number): void {
  const period = ledger.validatePeriodWrite(write);
  const hash = operationHash(write.operationId), previous = ledger.row(hash);
  if (previous) {
    fresh(previous);
    if (previous.fingerprint !== digest(write.fingerprint)) throw new FilesystemRepositoryError('idempotency_conflict', 'The retained operation has a different request fingerprint.');
    if (previous.status === 'pending' && previous.writeHash !== digest(write)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    return;
  }
  const refs = checkedWrite(write).map(record => digest(record.ref));
  for (const ref of refs) {
    const owner = ledger.owner(ref);
    if (owner) { fresh(owner); throw new FilesystemRepositoryError('reference_conflict', 'A retained reference already belongs to another operation.'); }
  }
  ledger.intent(hash, digest(write.fingerprint), digest(write), refs, now, retentionMs, period);
}
