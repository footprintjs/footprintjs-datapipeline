import type { RecordRef } from '../../contracts.js';
import type { CommitWrite } from '../../ports.js';
import { digest } from '../../validation.js';
import { checkedWrite } from '../filesystemRecords.js';
import { RepositoryRetentionError, type RepositoryHistoryPolicy } from './contracts.js';
export const HISTORY_PERIOD_MS = 86400_000;
const hex = '[a-f0-9]{64}';
const pattern = new RegExp(`^fp3:([a-f0-9]{32}):(0|[1-9][0-9]{0,8}):(o|batch|observation|evidence):(${hex})(?::(${hex}))?$`);
const invalid = (): never => { throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS'); };
export interface PeriodAddress { period: number; owner: string; kind: string }
export function address(id: string, policy: RepositoryHistoryPolicy, kind: string): PeriodAddress {
  if (typeof id !== 'string') return invalid();
  const match = pattern.exec(id);
  if (!match || match[1] !== policy.namespaceId || match[3] !== kind ||
    (kind === 'o' ? match[5] !== undefined : match[5] === undefined)) return invalid();
  const period = Number(match[2]);
  if (!Number.isSafeInteger(period * HISTORY_PERIOD_MS)) return invalid();
  return { period, owner: match[4]!, kind };
}
export function operationId(policy: RepositoryHistoryPolicy, logicalId: string): string {
  if (!/^[a-f0-9]{64}$/.test(logicalId)) return invalid();
  return `fp3:${policy.namespaceId}:${policy.period}:o:${logicalId}`;
}
export function reference<K extends RecordRef['kind']>(policy: RepositoryHistoryPolicy, operation: string, kind: K, key: string): { kind: K; id: string } {
  const owner = address(operation, policy, 'o');
  return { kind, id: `fp3:${policy.namespaceId}:${owner.period}:${kind}:${owner.owner}:${digest({ kind, key })}` };
}
export function validateOwnership(write: CommitWrite, policy: RepositoryHistoryPolicy): number {
  const owner = address(write.operationId, policy, 'o');
  for (const record of checkedWrite(write)) {
    const ref = address(record.ref.id, policy, record.ref.kind);
    if (ref.period !== owner.period || ref.owner !== owner.owner) return invalid();
  }
  return owner.period;
}
