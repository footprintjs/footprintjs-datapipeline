import { operationHash } from '../../src/storage/retainedRepository/commit.js';
import { digest } from '../../src/validation.js';

export function sealedSnapshot() {
  const operation = { operationHash: operationHash('one'), fingerprint: digest('fingerprint'), writeHash: digest('write'),
    acceptedAt: 0, expiresAt: 100, status: 'expired' as const, publication: 'committed' as const, dataDeleted: true };
  return { operations: [operation], references: [{ refHash: digest({ kind: 'batch', id: 'one' }), operationHash: operation.operationHash }] };
}
