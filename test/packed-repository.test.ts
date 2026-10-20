import { mkdtemp, readdir, readFile, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { gzipSync, gunzipSync } from 'node:zlib';
import { afterEach, expect, it } from 'vitest';
import { createFilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';

const paths: string[] = [];
async function directory() { const path = await mkdtemp(join(tmpdir(), 'packed-repository-')); paths.push(path); return path; }
afterEach(async () => { for (const path of paths.splice(0)) await rm(path, { recursive: true, force: true }); });
async function operation(path: string) { return join(path, 'operations', (await readdir(join(path, 'operations'))).find(x => !x.startsWith('.'))!); }

it('packing is opt-in; a fresh default reader reads mixed formats without rewriting old files', async () => {
  const path = await directory(), plain = candidate('plain'), packed = candidate('packed');
  await createFilesystemRepository({ directory: path }).commit(plain);
  const oldDir = await operation(path), oldManifest = await readFile(join(oldDir, 'manifest.json'));
  const store = createFilesystemRepository({ directory: path, packing: 'gzip' });
  const receipt = await store.commit(packed);
  expect(receipt.status).toBe('committed');
  expect(await readFile(join(oldDir, 'manifest.json'))).toEqual(oldManifest);
  const directories = await readdir(join(path, 'operations'));
  const newDir = join(path, 'operations', directories.find(x => join(path, 'operations', x) !== oldDir)!);
  expect(await readdir(newDir)).toEqual(['operation.json.gz']);
  const reader = createFilesystemRepository({ directory: path });
  for (const write of [plain, packed]) for (const record of [write.batch, ...write.observations, write.evidence]) expect(await reader.read(record.ref)).toEqual(record);
  expect(await reader.findOperation('packed')).toEqual({ fingerprint: packed.fingerprint, receipt });
  expect(await store.commit(candidate('packed', packed.fingerprint, 999))).toEqual(receipt);
  await expect(store.commit(candidate('packed', 'changed'))).rejects.toMatchObject({ code: 'idempotency_conflict' });
});

it('refuses oversized packed content before publication and rejects oversized reads', async () => {
  const path = await directory();
  await expect(createFilesystemRepository({ directory: path, packing: 'gzip', maxPackedBytes: 100 }).commit(candidate())).rejects.toMatchObject({ code: 'packed_limit' });
  expect(await createFilesystemRepository({ directory: path }).findOperation('collect:one')).toBeUndefined();
  await createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(candidate());
  await expect(createFilesystemRepository({ directory: path, maxPackedBytes: 100 }).findOperation('collect:one')).rejects.toMatchObject({ code: 'storage_corrupt' });
});

it.each(['gzip', 'hash', 'missing', 'duplicate', 'extra'] as const)('refuses %s corruption rather than reconciling or returning a point', async mode => {
  const path = await directory(), write = candidate();
  await createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write);
  const file = join(await operation(path), 'operation.json.gz');
  const bytes = await readFile(file);
  if (mode === 'gzip') await writeFile(file, bytes.subarray(0, bytes.length - 8));
  else {
    const packed = JSON.parse(gunzipSync(bytes).toString());
    if (mode === 'hash') packed.records[0].bytes += ' ';
    if (mode === 'missing') packed.records.pop();
    if (mode === 'duplicate') packed.records.push(packed.records[0]);
    if (mode === 'extra') packed.records.push({ file: '../../outside', bytes: '{}' });
    await writeFile(file, gzipSync(JSON.stringify(packed)));
  }
  const reader = createFilesystemRepository({ directory: path });
  await expect(reader.read(write.observations[0]!.ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
  await expect(reader.findOperation(write.operationId)).rejects.toMatchObject({ code: 'storage_corrupt' });
});

it('rejects ambiguous plain plus packed publication and never falls back from corrupt packed data', async () => {
  const path = await directory(), write = candidate();
  await createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write);
  const dir = await operation(path);
  const packed = JSON.parse(gunzipSync(await readFile(join(dir, 'operation.json.gz'))).toString());
  await writeFile(join(dir, 'manifest.json'), JSON.stringify(packed.manifest));
  await expect(createFilesystemRepository({ directory: path }).read(write.batch.ref)).rejects.toMatchObject({ code: 'storage_corrupt' });
});

it('one immutable winner survives concurrent packed/plain commits', async () => {
  const path = await directory(), write = candidate('raced');
  const receipts = await Promise.all([createFilesystemRepository({ directory: path }).commit(write), createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write)]);
  expect(receipts[0]).toEqual(receipts[1]);
  expect(await createFilesystemRepository({ directory: path }).read(write.observations[0]!.ref)).toEqual(write.observations[0]);
});

it('snapshots mutable caller input before I/O and refuses cross-format reference conflicts', async () => {
  const path = await directory(), write = candidate('original');
  const expected = structuredClone(write);
  const pending = createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write);
  (write.observations[0]!.payload as { latency_us: number }).latency_us = 999;
  expect((await pending).status).toBe('committed');
  const reader = createFilesystemRepository({ directory: path });
  expect(await reader.read(expected.observations[0]!.ref)).toEqual(expected.observations[0]);
  const other = candidate('other');
  const ref = expected.observations[0]!.ref;
  const conflicting = { ...other, observations: [{ ...other.observations[0]!, ref }], batch: { ...other.batch, observationRefs: [ref] }, evidence: { ...other.evidence, outputRefs: [other.batch.ref, ref] } };
  await expect(reader.commit(conflicting)).rejects.toMatchObject({ code: 'reference_conflict' });
  expect(await reader.findOperation('other')).toBeUndefined();
});

it.each([0, -1, NaN, Infinity, 16 * 1024 * 1024 + 1])('refuses invalid packed ceiling %s', maxPackedBytes => {
  expect(() => createFilesystemRepository({ directory: 'unused', maxPackedBytes })).toThrow();
});

it.each(['inflate', 'utf8', 'json'] as const)('bounds or refuses malformed %s before returning records', async mode => {
  const path = await directory(), write = candidate();
  await createFilesystemRepository({ directory: path, packing: 'gzip' }).commit(write);
  const bytes = mode === 'inflate' ? Buffer.alloc(100_000, 120) : mode === 'utf8' ? Buffer.from([0xff]) : Buffer.from('not json');
  await writeFile(join(await operation(path), 'operation.json.gz'), gzipSync(bytes));
  await expect(createFilesystemRepository({ directory: path, maxPackedBytes: 4096 }).findOperation(write.operationId)).rejects.toMatchObject({ code: 'storage_corrupt' });
});
