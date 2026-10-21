import { createHash } from 'node:crypto';
import type { SealedHistoryOperation } from './contracts.js';

export const HEADER_BYTES = 64, OPERATION_BYTES = 115, REFERENCE_BYTES = 36, CHECKSUM_BYTES = 32;
export const MAGIC = Buffer.from('FPHIST01');
export const hash = (bytes: Uint8Array): string => createHash('sha256').update(bytes).digest('hex');
export const isHash = (value: unknown): value is string => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
export function sizeOf(operations: number, references: number): number {
  return HEADER_BYTES + operations * OPERATION_BYTES + references * REFERENCE_BYTES + CHECKSUM_BYTES;
}
export function readRow(bytes: Buffer, index: number): SealedHistoryOperation {
  const offset = HEADER_BYTES + index * OPERATION_BYTES;
  return Object.freeze({
    operationHash: bytes.toString('hex', offset, offset + 32),
    fingerprint: bytes.toString('hex', offset + 32, offset + 64),
    writeHash: bytes.toString('hex', offset + 64, offset + 96),
    acceptedAt: Number(bytes.readBigUInt64BE(offset + 96)),
    expiresAt: Number(bytes.readBigUInt64BE(offset + 104)),
    status: (['pending', 'stored', 'expired'] as const)[bytes[offset + 112]!]!,
    publication: (['unconfirmed', 'committed'] as const)[bytes[offset + 113]!]!,
    dataDeleted: bytes[offset + 114] === 1,
  });
}
export function writeRow(bytes: Buffer, row: SealedHistoryOperation, index: number): void {
  const offset = HEADER_BYTES + index * OPERATION_BYTES;
  Buffer.from(row.operationHash, 'hex').copy(bytes, offset);
  Buffer.from(row.fingerprint, 'hex').copy(bytes, offset + 32);
  Buffer.from(row.writeHash, 'hex').copy(bytes, offset + 64);
  bytes.writeBigUInt64BE(BigInt(row.acceptedAt), offset + 96);
  bytes.writeBigUInt64BE(BigInt(row.expiresAt), offset + 104);
  bytes[offset + 112] = ['pending', 'stored', 'expired'].indexOf(row.status);
  bytes[offset + 113] = row.publication === 'committed' ? 1 : 0;
  bytes[offset + 114] = row.dataDeleted ? 1 : 0;
}
