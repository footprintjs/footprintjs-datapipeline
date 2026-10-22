import { createHash } from 'node:crypto';
import { expect, it } from 'vitest';
import { digest } from '../src/validation.js';
import { encodeHistory } from '../src/storage/sealedHistory/encode.js';
import { decodeHistory } from '../src/storage/sealedHistory/decode.js';
import type { SealedHistoryOperation, SealedHistorySnapshot } from '../src/storage/sealedHistory/contracts.js';

const limit = 16 * 1024 * 1024;
function rows(): SealedHistorySnapshot {
  const operations = ([
    { status: 'pending', publication: 'unconfirmed', dataDeleted: false },
    { status: 'stored', publication: 'committed', dataDeleted: false },
    { status: 'expired', publication: 'unconfirmed', dataDeleted: true },
    { status: 'expired', publication: 'committed', dataDeleted: true },
  ] as const).map((flags, i) => ({ ...flags, operationHash: digest('operation-' + i), fingerprint: digest('fingerprint-' + i),
    writeHash: digest('write-' + i), acceptedAt: i * 10, expiresAt: i * 10 + 100 }));
  return { operations, references: operations.map((row, i) => ({ refHash: digest('ref-' + i), operationHash: row.operationHash })) };
}
function checksum(bytes: Buffer): Buffer {
  createHash('sha256').update(bytes.subarray(0, -32)).digest().copy(bytes, bytes.length - 32);
  return bytes;
}
it('round-trips every valid row state, both hashes, exact times and kind-sensitive ownership', () => {
  const input = rows(), bytes = encodeHistory(input, 100, limit), decoded = decodeHistory(bytes, 100, limit);
  expect(bytes.length).toBe(96 + 4 * 115 + 4 * 36);
  expect(decoded.terminal).toBe(false);
  for (const row of input.operations) expect(decoded.row(row.operationHash)).toEqual(row);
  for (const ref of input.references) expect(decoded.owner(ref.refHash)).toEqual(input.operations.find(row => row.operationHash === ref.operationHash));
  expect(decoded.owner(digest('foreign-ref'))).toBeUndefined();
  bytes.fill(0);
  expect(decoded.row(input.operations[0]!.operationHash)).toEqual(input.operations[0]);
});
it('rejects bad counts, offsets, flags, unsafe times, ordering and owners even with a recomputed checksum', () => {
  const original = encodeHistory(rows(), 100, limit);
  const mutations = [
    (b: Buffer) => b.writeUInt32BE(0xffffffff, 24),
    (b: Buffer) => b.writeUInt32BE(65, 32),
    (b: Buffer) => b.writeUInt32BE(1, 36),
    (b: Buffer) => { b[44] = 1; },
    (b: Buffer) => { b[64 + 112] = 255; },
    (b: Buffer) => { b[64 + 113] = 2; },
    (b: Buffer) => { b[64 + 114] = 2; },
    (b: Buffer) => b.writeBigUInt64BE(BigInt(Number.MAX_SAFE_INTEGER) + 1n, 64 + 96),
    (b: Buffer) => b.copy(b, 64 + 115, 64, 64 + 32),
    (b: Buffer) => b.writeUInt32BE(4, 64 + 4 * 115 + 32),
    (b: Buffer) => b.copy(b, 64 + 4 * 115 + 36, 64 + 4 * 115, 64 + 4 * 115 + 32),
  ];
  for (const mutate of mutations) {
    const changed = Buffer.from(original); mutate(changed);
    expect(() => decodeHistory(checksum(changed), 100, limit)).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_CORRUPT' }));
  }
  const unsupported = Buffer.from(original); unsupported.writeUInt32BE(999, 8);
  expect(() => decodeHistory(checksum(unsupported), 100, limit)).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_UNSUPPORTED_FORMAT' }));
  expect(() => decodeHistory(original, 101, limit)).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_CORRUPT' }));
  expect(() => decodeHistory(original, 100, original.length - 1)).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_LIMIT' }));
  expect(() => decodeHistory(original.subarray(0, -1), 100, limit)).toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_CORRUPT' }));
});
it('rejects accessors, extra metadata and incompatible row states without invoking row accessors', () => {
  const input = rows(); let invoked = 0;
  const badRow = Object.defineProperty({ ...input.operations[0] }, 'fingerprint', { get() { invoked++; return digest('private'); } });
  const bad = [badRow, { ...input.operations[0], discarded: 'private' }, { ...input.operations[0], status: 'stored' },
    { ...input.operations[0], acceptedAt: -1 }, { ...input.operations[0], dataDeleted: true }];
  for (const row of bad) expect(() => encodeHistory({ ...input, operations: [row as SealedHistoryOperation] }, 100, limit))
    .toThrowError(expect.objectContaining({ code: 'SEALED_HISTORY_INVALID_SNAPSHOT' }));
  expect(invoked).toBe(0);
});
