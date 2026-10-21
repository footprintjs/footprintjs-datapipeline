import { fail, type SealedHistoryOperation } from './contracts.js';
import { CHECKSUM_BYTES, HEADER_BYTES, MAGIC, OPERATION_BYTES, REFERENCE_BYTES, hash, readRow, sizeOf } from './layout.js';
import { isSealable, validRow } from './rules.js';

export interface DecodedHistory {
  readonly counts: { readonly operations: number; readonly references: number };
  readonly terminal: boolean;
  row(hash: string): SealedHistoryOperation | undefined;
  owner(hash: string): SealedHistoryOperation | undefined;
}
function search(bytes: Buffer, hash: string, count: number, start: number, stride: number): number {
  const wanted = Buffer.from(hash, 'hex');
  let low = 0, high = count - 1;
  while (low <= high) {
    const index = Math.floor((low + high) / 2), offset = start + index * stride;
    const compared = Buffer.compare(wanted, bytes.subarray(offset, offset + 32));
    if (compared === 0) return index;
    if (compared < 0) high = index - 1; else low = index + 1;
  }
  return -1;
}
function checkHeader(bytes: Buffer, retentionMs: number) {
  if (bytes.length < HEADER_BYTES + CHECKSUM_BYTES || !bytes.subarray(0, 8).equals(MAGIC)) fail('SEALED_HISTORY_CORRUPT');
  if (bytes.readUInt32BE(8) !== 1) fail('SEALED_HISTORY_UNSUPPORTED_FORMAT');
  const operations = bytes.readUInt32BE(24), references = bytes.readUInt32BE(28);
  const refOffset = HEADER_BYTES + operations * OPERATION_BYTES;
  const bodyEnd = sizeOf(operations, references) - CHECKSUM_BYTES;
  if (bodyEnd + CHECKSUM_BYTES !== bytes.length) fail('SEALED_HISTORY_CORRUPT');
  if (bytes.readUInt32BE(12) !== HEADER_BYTES || bytes.readUInt32BE(32) !== HEADER_BYTES ||
      bytes.readUInt32BE(36) !== refOffset || bytes.readUInt32BE(40) !== bodyEnd) fail('SEALED_HISTORY_CORRUPT');
  if (bytes.readBigUInt64BE(16) !== BigInt(retentionMs) || bytes.subarray(44, HEADER_BYTES).some(byte => byte !== 0)) fail('SEALED_HISTORY_CORRUPT');
  if (hash(bytes.subarray(0, bodyEnd)) !== bytes.toString('hex', bodyEnd)) fail('SEALED_HISTORY_CORRUPT');
  return { operations, references, refOffset };
}
function checkRows(bytes: Buffer, operations: number, retentionMs: number): boolean {
  let allTerminal = true;
  for (let index = 0; index < operations; index++) {
    const row = readRow(bytes, index), offset = HEADER_BYTES + index * OPERATION_BYTES;
    if (!validRow(row, retentionMs) || bytes[offset + 114]! > 1 ||
        (index && Buffer.compare(bytes.subarray(offset - OPERATION_BYTES, offset - OPERATION_BYTES + 32), bytes.subarray(offset, offset + 32)) >= 0)) fail('SEALED_HISTORY_CORRUPT');
    allTerminal &&= isSealable(row);
  }
  return allTerminal;
}
function checkReferences(bytes: Buffer, operations: number, references: number, refOffset: number): void {
  for (let index = 0; index < references; index++) {
    const offset = refOffset + index * REFERENCE_BYTES;
    if (bytes.readUInt32BE(offset + 32) >= operations ||
        (index && Buffer.compare(bytes.subarray(offset - REFERENCE_BYTES, offset - REFERENCE_BYTES + 32), bytes.subarray(offset, offset + 32)) >= 0)) fail('SEALED_HISTORY_CORRUPT');
  }
}
/** Detached, bounded metadata only. Not a source-data or cause-record reader. */
export function decodeHistory(input: Uint8Array, retentionMs: number, limit: number): DecodedHistory {
  if (input.byteLength > limit) fail('SEALED_HISTORY_LIMIT');
  const bytes = Buffer.from(input);
  const { operations, references, refOffset } = checkHeader(bytes, retentionMs);
  const allTerminal = checkRows(bytes, operations, retentionMs);
  checkReferences(bytes, operations, references, refOffset);
  return Object.freeze({
    counts: Object.freeze({ operations, references }), terminal: allTerminal,
    row(value: string) {
      const index = search(bytes, value, operations, HEADER_BYTES, OPERATION_BYTES);
      return index < 0 ? undefined : readRow(bytes, index);
    },
    owner(value: string) {
      const index = search(bytes, value, references, refOffset, REFERENCE_BYTES);
      return index < 0 ? undefined : readRow(bytes, bytes.readUInt32BE(refOffset + index * REFERENCE_BYTES + 32));
    },
  });
}
