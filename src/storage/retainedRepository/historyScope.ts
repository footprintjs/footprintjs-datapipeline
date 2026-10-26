import type { RecordRef } from '../../contracts.js';
import type { Repository, RepositoryIdentity } from '../../ports.js';
import type { RepositoryHistoryPolicy } from './contracts.js';
import { operationId, reference } from './historyIdentity.js';
export function scopedIdentity(policy: RepositoryHistoryPolicy,
  assertOperation: RepositoryIdentity['assertOperation']): RepositoryIdentity {
  return Object.freeze({ operationId: (logicalId: string) => operationId(policy, logicalId), assertOperation,
    reference: <K extends RecordRef['kind']>(id: string, kind: K, key: string) => reference(policy, id, kind, key) });
}
/** A new immutable scope shares storage/serialization; an earlier pipeline keeps its IDs. */
export function scopedRepository(repository: Repository, identity: RepositoryIdentity): Repository {
  return Object.freeze({ identity,
    findOperation: (id: string) => repository.findOperation(id), commit: (write: Parameters<Repository['commit']>[0]) => repository.commit(write),
    read: (ref: RecordRef) => repository.read(ref), readMany: (refs: readonly RecordRef[]) => repository.readMany!(refs) });
}
