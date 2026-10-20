import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { expect, it, vi } from 'vitest';
import * as codec from '../src/storage/packedOperation.js';
import { PackedReads } from '../src/storage/packedReads.js';
import { createFilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

it('real repository bulk reads share one overlapping inflate, recheck later, and isolate returned receipts', async () => {
  const path = await mkdtemp(join(tmpdir(), 'packed-read-integration-'));
  try {
    const write = candidate();
    await createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write);
    const reader = createFilesystemRepository({ directory: path });
    const originalLoad = codec.readPackedOperation, originalRead = PackedReads.prototype.read;
    let calls = 0, arrivals = 0, release!: () => void, allArrived!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const ready = new Promise<void>(resolve => { allArrived = resolve; });
    vi.spyOn(codec, 'readPackedOperation').mockImplementation(async (...args) => { calls++; await gate; return originalLoad(...args); });
    vi.spyOn(PackedReads.prototype, 'read').mockImplementation(function (this: PackedReads<unknown>, directory: string) {
      if (++arrivals === 10) allArrived();
      return originalRead.call(this, directory);
    });
    const pending = Array.from({ length: 10 }, () => reader.read(write.observations[0]!.ref));
    await ready;
    expect(calls).toBe(1);
    release();
    for (const row of await Promise.all(pending)) expect(row).toEqual(write.observations[0]);
    expect(calls).toBe(1);
    expect(await reader.read(write.observations[0]!.ref)).toEqual(write.observations[0]);
    expect(calls).toBe(2);
    const [left, right] = await Promise.all([reader.findOperation(write.operationId), reader.findOperation(write.operationId)]);
    expect(left).toEqual(right);
    expect(left!.receipt).not.toBe(right!.receipt);
  } finally {
    vi.restoreAllMocks();
    await rm(path, { recursive: true, force: true });
  }
});
