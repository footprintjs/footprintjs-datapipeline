import type { SealedHistoryOperation } from './contracts.js';
import { isHash } from './layout.js';

export function validRow(row: SealedHistoryOperation, retentionMs: number): boolean {
  return isHash(row.operationHash) && isHash(row.fingerprint) && isHash(row.writeHash) &&
    Number.isSafeInteger(row.acceptedAt) && row.acceptedAt >= 0 && Number.isSafeInteger(row.expiresAt) &&
    row.expiresAt - row.acceptedAt === retentionMs &&
    ['pending', 'stored', 'expired'].includes(row.status) && ['unconfirmed', 'committed'].includes(row.publication) &&
    (row.status !== 'stored' || row.publication === 'committed') && typeof row.dataDeleted === 'boolean' &&
    (!row.dataDeleted || row.status === 'expired');
}
export function isSealable(row: SealedHistoryOperation): boolean {
  return row.status === 'expired' && row.publication === 'committed' && row.dataDeleted;
}
