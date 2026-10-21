import { afterEach, expect, it, vi } from 'vitest';
import { mkdtemp, realpath, readFile, rm, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync, gzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';
const calls = vi.hoisted(() => ({ canonical: [] as unknown[], compression: 0 }));
vi.mock('../src/validation.js', async original => {
  const actual = await original<typeof import('../src/validation.js')>();
  return { ...actual, canonical(value: unknown) { calls.canonical.push(value); return actual.canonical(value); } };
});
vi.mock('../src/storage/traceCompression.js', async original => {
  const actual = await original<typeof import('../src/storage/traceCompression.js')>();
  return { ...actual, async compressTrace(text: string) { calls.compression++; return actual.compressTrace(text); } };
});
import { createGuardedFilesystemTraceStore } from '../src/storage/guardedTrace/index.js';
import { canonical } from '../src/validation.js';
const folders: string[] = [];
afterEach(async () => { calls.canonical = []; calls.compression = 0; await Promise.all(folders.splice(0).map(path => rm(path, { recursive: true, force: true }))); });
async function host() {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'single-encoding-'))); folders.push(directory);
  const store = createGuardedFilesystemTraceStore({ enabled: true, directory, maxTraceBytesOnDisk: 16 * 1024 * 1024, warningAtBytes: 8 * 1024 * 1024, minFreeBytes: 1, receiptReserveBytes: 16 * 1024 * 1024, requireSeparateFilesystem: false, onHealth: () => {} })!;
  return { store, directory };
}
it('validates a full trace once and compresses once while preserving the exact canonical bytes and reference', async () => {
  const f = await host();
  const trace = { operationId: 'single-pass', recordedAt: '2026-10-21T00:00:00Z', value: { rows: Array.from({ length: 256 }, (_, i) => ({ index: i, value: 'LOCAL SEED '.repeat(50) })) } };
  const expected = canonical(trace); calls.canonical = []; calls.compression = 0;
  try {
    const ref = await f.store.store(trace);
    expect(calls.canonical.filter(value => (value as { operationId?: unknown })?.operationId === 'single-pass')).toHaveLength(1);
    expect(calls.compression).toBe(1);
    expect(ref.id).toBe(createHash('sha256').update(expected).digest('hex'));
    expect(gunzipSync(await readFile(join(f.directory, 'traces', `${ref.id}.json.gz`))).toString('utf8')).toBe(expected);
    expect(await f.store.read(ref)).toEqual(trace);
    expect((await f.store.receipt(ref))?.status).toBe('stored');
  } finally { f.store.close(); }
});
it('keeps the write-time snapshot when the caller changes its object after store starts', async () => {
  const f = await host(); const trace = { operationId: 'snapshot', recordedAt: '2026-10-21T00:00:00Z', value: { marker: 'before' } };
  try {
    const writing = f.store.store(trace); trace.value.marker = 'after';
    const ref = await writing;
    expect(await f.store.read(ref)).toEqual({ ...trace, value: { marker: 'before' } });
  } finally { f.store.close(); }
});

it('refuses a corrupt orphan file rather than replacing it during a new reservation', async () => {
  const f = await host();
  const trace = { operationId: 'orphan', recordedAt: '2026-10-21T00:00:00Z', value: { marker: 'expected' } };
  const id = createHash('sha256').update(canonical(trace)).digest('hex');
  try {
    await f.store.usage();
    await mkdir(join(f.directory, 'traces'), { recursive: true, mode: 0o700 });
    const path = join(f.directory, 'traces', `${id}.json.gz`);
    const original = gzipSync('corrupt orphan');
    await writeFile(path, original);
    await expect(f.store.store(trace)).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED' });
    expect(await readFile(path)).toEqual(original);
    expect((await f.store.receipt({ kind: 'trace', id }))?.status).not.toBe('stored');
  } finally { f.store.close(); }
});
