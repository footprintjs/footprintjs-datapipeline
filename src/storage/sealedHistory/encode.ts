import type { SealedHistoryOperation, SealedHistorySnapshot } from './contracts.js';
import { CHECKSUM_BYTES, HEADER_BYTES, MAGIC, OPERATION_BYTES, REFERENCE_BYTES, fail, hash, isHash, sizeOf, validRow, writeRow } from './layout.js';

/** Reject accessors/extra fields instead of invoking code or silently dropping metadata. */
function plain(value: unknown, keys: readonly string[]): boolean {
  if (!value || typeof value !== 'object' || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) return false;
  return Reflect.ownKeys(value).length === keys.length && keys.every(key => {
    const property = Object.getOwnPropertyDescriptor(value, key);
    return property && 'value' in property;
  });
}
const rowKeys = ['operationHash', 'fingerprint', 'writeHash', 'acceptedAt', 'expiresAt', 'status', 'publication', 'dataDeleted'];
function entries<T>(array: readonly T[]): T[] {
  if (Reflect.ownKeys(array).length !== array.length + 1 || Object.getOwnPropertySymbols(array).length) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
  const values: T[] = [];
  for (let i = 0; i < array.length; i++) {
    const property = Object.getOwnPropertyDescriptor(array, String(i));
    if (!property || !('value' in property)) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
    values.push(property.value as T);
  }
  return values;
}

export function encodeHistory(input: SealedHistorySnapshot, retentionMs: number, limit: number): Buffer {
  if (!plain(input, ['operations', 'references']) || !Array.isArray(input.operations) || !Array.isArray(input.references)) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
  const size = sizeOf(input.operations.length, input.references.length);
  if (!Number.isSafeInteger(size) || size > limit) fail('SEALED_HISTORY_LIMIT');
  const operations: SealedHistoryOperation[] = [];
  for (const row of entries(input.operations)) {
    if (!plain(row, rowKeys) || !validRow(row, retentionMs)) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
    operations.push({ ...row });
  }
  operations.sort((a, b) => a.operationHash < b.operationHash ? -1 : a.operationHash > b.operationHash ? 1 : 0);
  const owners = new Map<string, number>();
  operations.forEach((row, index) => {
    if (owners.has(row.operationHash)) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
    owners.set(row.operationHash, index);
  });
  const references = entries(input.references).map(ref => {
    if (!plain(ref, ['refHash', 'operationHash']) || !isHash(ref.refHash) || !owners.has(ref.operationHash)) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
    return { refHash: ref.refHash, owner: owners.get(ref.operationHash)! };
  }).sort((a, b) => a.refHash < b.refHash ? -1 : a.refHash > b.refHash ? 1 : 0);
  const bytes = Buffer.alloc(size);
  MAGIC.copy(bytes); bytes.writeUInt32BE(1, 8); bytes.writeUInt32BE(HEADER_BYTES, 12);
  bytes.writeBigUInt64BE(BigInt(retentionMs), 16);
  bytes.writeUInt32BE(operations.length, 24); bytes.writeUInt32BE(references.length, 28);
  const refOffset = HEADER_BYTES + operations.length * OPERATION_BYTES;
  bytes.writeUInt32BE(HEADER_BYTES, 32); bytes.writeUInt32BE(refOffset, 36); bytes.writeUInt32BE(size - CHECKSUM_BYTES, 40);
  operations.forEach((row, index) => writeRow(bytes, row, index));
  references.forEach((ref, index) => {
    if (index && references[index - 1]!.refHash === ref.refHash) fail('SEALED_HISTORY_INVALID_SNAPSHOT');
    const offset = refOffset + index * REFERENCE_BYTES;
    Buffer.from(ref.refHash, 'hex').copy(bytes, offset); bytes.writeUInt32BE(ref.owner, offset + 32);
  });
  Buffer.from(hash(bytes.subarray(0, size - CHECKSUM_BYTES)), 'hex').copy(bytes, size - CHECKSUM_BYTES);
  return bytes;
}
