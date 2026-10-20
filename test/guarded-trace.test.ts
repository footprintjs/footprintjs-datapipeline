import { mkdtemp, mkdir, realpath, readdir, rm, readFile, writeFile, unlink, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { createGuardedFilesystemTraceStore } from '../src/storage/guardedTrace/index.js';
const disk = vi.hoisted(() => ({ free: 10_000_000_000n, failTrace: false, failDelete: false, block: undefined as Promise<void> | undefined, onStart: undefined as (() => void) | undefined }));
vi.mock('node:fs/promises', async original => {
 const actual = await original<typeof import('node:fs/promises')>();
 return { ...actual,
  statfs: async () => ({ bsize: 4096n, bavail: disk.free / 4096n }),
  open: async (...args: Parameters<typeof actual.open>) => {
   if (String(args[0]).endsWith('/trace.json')) {
    disk.onStart?.(); if (disk.block) await disk.block;
    if (disk.failTrace) throw Object.assign(Error('private source text'), { code: 'ENOSPC' });
   }
   return actual.open(...args);
  },
  unlink: async (...args: Parameters<typeof actual.unlink>) => {
   if (disk.failDelete) throw Error('private filesystem error');
   return actual.unlink(...args);
  },
 };
});
const folders: string[] = [], stores: { close(): void }[] = [];
afterEach(async () => { for (const s of stores.splice(0)) s.close(); for (const p of folders.splice(0)) await rm(p, { recursive: true, force: true }); disk.free = 10_000_000_000n; disk.failTrace = false; disk.failDelete = false; disk.block = undefined; disk.onStart = undefined; });
async function host(extra = {}) {
 const directory = await realpath(await mkdtemp(join(tmpdir(), 'guarded-trace-'))); folders.push(directory);
 let now = Date.parse('2026-10-20T12:00:00Z'); const health: unknown[] = [];
 const options = { enabled: true, directory, maxTraceBytesOnDisk: 8 * 1024 * 1024, warningAtBytes: 4 * 1024 * 1024, minFreeBytes: 1024 * 1024, receiptReserveBytes: 16 * 1024 * 1024, requireSeparateFilesystem: false, clock: () => now, onHealth: (event: unknown) => { health.push(event); }, ...extra };
 const store = createGuardedFilesystemTraceStore(options)!; stores.push(store);
 return { store, options, health, directory, advance: (ms: number) => { now += ms; } };
}
const trace = (id = 'run') => ({ operationId: id, recordedAt: '2000-01-01T00:00:00.000Z', value: { evidence: 'LOCAL SEED' } });
it('is absent by default and creates no store', () => { expect(createGuardedFilesystemTraceStore({ enabled: false } as never)).toBeUndefined(); });
it('keeps complete gzip traces and small receipts, reopening through a fresh instance', async () => {
 const f = await host(); const ref = await f.store.store(trace());
 expect(await f.store.read(ref)).toEqual(trace());
 expect((await f.store.receipt(ref))?.status).toBe('stored');
 expect((await readdir(join(f.directory, 'traces'))).every(name => name.endsWith('.json.gz'))).toBe(true);
 const second = createGuardedFilesystemTraceStore(f.options)!; stores.push(second);
 expect(await second.read(ref)).toEqual(trace());
});
it('refuses root-filesystem storage by default before creating evidence files', async () => {
 const f = await host({ requireSeparateFilesystem: true });
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_UNSAFE_FILESYSTEM' });
 expect(await readdir(f.directory)).toEqual([]);
});
it('records a fixed pressure gap and alerts without saving a full trace', async () => {
 const f = await host(); disk.free = 1n;
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_DISK_PRESSURE' });
 expect(f.health).toContainEqual(expect.objectContaining({ code: 'TRACE_DISK_PRESSURE', receiptSaved: true }));
 const ref = (f.health[0] as {traceRef: import('../src/contracts.js').TraceRef}).traceRef;
 expect(await f.store.receipt(ref)).toMatchObject({ status: 'not-saved', code: 'TRACE_DISK_PRESSURE' });
 await expect(f.store.read(ref)).rejects.toMatchObject({ code: 'TRACE_DISK_PRESSURE' });
 expect(await f.store.usage()).toMatchObject({ traceBytes: 0, gaps: 1 });
 expect(await readdir(join(f.directory, 'traces')).catch(() => [])).toEqual([]);
});
it('enforces its trace budget without deleting previously saved evidence', async () => {
 const f = await host({ maxTraceBytesOnDisk: 70 * 1024, warningAtBytes: 1 });
 const ref = await f.store.store(trace('first'));
 await expect(f.store.store(trace('second'))).rejects.toMatchObject({ code: 'TRACE_BUDGET_EXCEEDED' });
 expect(await f.store.read(ref)).toEqual(trace('first'));
 expect(f.health).toContainEqual(expect.objectContaining({ code: 'TRACE_BUDGET_WARNING' }));
});
it('expires at exactly thirty days from acceptance, never from an app-supplied timestamp', async () => {
 const f = await host(); const ref = await f.store.store(trace());
 f.advance(30 * 86400_000 - 1); expect(await f.store.read(ref)).toEqual(trace());
 f.advance(1); await expect(f.store.read(ref)).rejects.toMatchObject({ code: 'TRACE_EXPIRED' });
 expect(await f.store.sweep({ limit: 1 })).toMatchObject({ deleted: 1 });
 expect((await f.store.receipt(ref))?.status).toBe('expired');
 expect(await f.store.usage()).toMatchObject({ traceBytes: 0 });
 await expect(f.store.read(ref)).rejects.toMatchObject({ code: 'TRACE_EXPIRED' });
});
it('does not extend retention on duplicate writes or restart', async () => {
 const f = await host(); const ref = await f.store.store(trace());
 const original = await f.store.receipt(ref); f.advance(86400_000);
 expect(await f.store.store(trace())).toEqual(ref);
 expect(await f.store.receipt(ref)).toEqual(original);
 f.advance(29 * 86400_000);
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_EXPIRED' });
});
it('sweeps only expired files in bounded chunks, preserving small expiry receipts', async () => {
 const f = await host(); await f.store.store(trace('a')); await f.store.store(trace('b')); f.advance(30 * 86400_000);
 expect(await f.store.sweep({ limit: 1 })).toMatchObject({ deleted: 1 });
 expect((await readdir(join(f.directory, 'traces'))).length).toBe(1);
 expect(await f.store.sweep({ limit: 1 })).toMatchObject({ deleted: 1 });
 await expect(f.store.sweep({ limit: 101 })).rejects.toMatchObject({ code: 'TRACE_INVALID_OPTIONS' });
});
it('does not let a broken alert callback erase the recorded storage gap', async () => {
 const f = await host({ onHealth: () => { throw Error('private callback text'); } }); disk.free = 1n;
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_DISK_PRESSURE', message: 'Full trace storage was refused.' });
 expect(await f.store.usage()).toMatchObject({ gaps: 1 });
});
it('reserves in-flight bytes across two instances before either trace is acknowledged', async () => {
 const f = await host({ maxTraceBytesOnDisk: 70 * 1024, warningAtBytes: 1 });
 let release!: () => void, started!: () => void;
 disk.block = new Promise<void>(resolve => { release = resolve; });
 const ready = new Promise<void>(resolve => { started = resolve; }); disk.onStart = started;
 const first = f.store.store(trace('in-flight')); await ready;
 const other = createGuardedFilesystemTraceStore(f.options)!; stores.push(other);
 try { await expect(other.store(trace('concurrent'))).rejects.toMatchObject({ code: 'TRACE_BUDGET_EXCEEDED' }); }
 finally { release(); }
 expect(await f.store.read(await first)).toEqual(trace('in-flight'));
});
it('keeps failed-write receipts and reservations across restart, never silently retrying', async () => {
 const f = await host(); disk.failTrace = true;
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED' });
 const event = f.health.find(e => (e as {receiptSaved?: boolean}).receiptSaved) as {traceRef: import('../src/contracts.js').TraceRef};
 expect(event).toBeDefined(); expect((await f.store.receipt(event.traceRef))?.status).toBe('pending');
 expect((await f.store.usage()).traceBytes).toBeGreaterThan(0);
 disk.failTrace = false;
 const reopened = createGuardedFilesystemTraceStore(f.options)!; stores.push(reopened);
 await expect(reopened.store(trace())).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED' });
 await expect(reopened.read(event.traceRef)).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED' });
});
it('persists expiry before deletion and can recover an interrupted cleanup without double releasing bytes', async () => {
 const f = await host(); const ref = await f.store.store(trace()); f.advance(30 * 86400_000); disk.failDelete = true;
 await expect(f.store.sweep({ limit: 1 })).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED' });
 expect((await f.store.receipt(ref))?.status).toBe('expired');
 expect((await f.store.usage()).traceBytes).toBeGreaterThan(0);
 disk.failDelete = false; expect(await f.store.sweep({ limit: 1 })).toEqual({ deleted: 1 });
 expect(await f.store.sweep({ limit: 1 })).toEqual({ deleted: 0 });
 expect((await f.store.usage()).traceBytes).toBe(0);
});
it('refuses policy drift within one managed namespace', async () => {
 const f = await host(); await f.store.store(trace());
 const other = createGuardedFilesystemTraceStore({ ...f.options, maxTraceBytesOnDisk: 10 * 1024 * 1024 })!; stores.push(other);
 await expect(other.store(trace('other'))).rejects.toMatchObject({ code: 'TRACE_INVALID_OPTIONS' });
});
it('never follows a trace symlink during expiry cleanup', async () => {
 const f = await host(); const ref = await f.store.store(trace());
 const outside = join(f.directory, 'do-not-delete.txt'); await writeFile(outside, 'saved user data');
 const file = join(f.directory, 'traces', `${ref.id}.json.gz`); await unlink(file); await symlink(outside, file);
 f.advance(30 * 86400_000);
 await expect(f.store.sweep({ limit: 1 })).rejects.toMatchObject({ code: 'TRACE_UNSAFE_FILESYSTEM' });
 expect(await readFile(outside, 'utf8')).toBe('saved user data');
});
it('snapshots caller data before asynchronous directory work', async () => {
 const f = await host(); const value = trace(); const saving = f.store.store(value); value.value.evidence = 'changed';
 expect((await f.store.read(await saving))?.value).toEqual({ evidence: 'LOCAL SEED' });
});
it('reports receipt-store failure on an independent health channel without leaking error text', async () => {
 const f = await host(); await mkdir(join(f.directory, 'retention.sqlite'));
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED', message: 'Full trace storage was refused.' });
 expect(f.health).toContainEqual({ code: 'TRACE_STORAGE_UNCONFIRMED', receiptSaved: false });
});
it('a rejected full trace does not undo the pipeline data acknowledgement', async () => {
 const { createPipeline } = await import('../src/index.js');
 const { createFilesystemRepository } = await import('../src/storage/filesystem.js');
 const f = await host(); disk.free = 1n;
 const ref = { id: 'LOCAL-SEED', version: '1' };
 const pipeline = createPipeline({ repository: createFilesystemRepository({ directory: join(f.directory, 'data') }), traceStore: f.store,
  sources: [{ ref, adapterRef: ref }], adapters: [{ ref, async collect() { return { key: 'poll', outcome: 'complete', coverage: { status: 'unknown', time: { kind: 'source-time-unknown' } }, observations: [] }; } }],
 });
 const result = await pipeline.collect({ sourceRef: ref, idempotencyKey: 'poll', request: null });
 expect(result.commit.status).toBe('committed'); expect(result.trace.status).toBe('unavailable');
 expect(await pipeline.read(result.commit.batchRef!)).toBeDefined();
 expect((await f.store.usage()).gaps).toBe(1);
});
it('does not run root filesystem checks or load a store when disabled', () => {
 expect(createGuardedFilesystemTraceStore({ enabled: false, directory: '/' } as never)).toBeUndefined();
});
it('refuses adoption of unmanaged old trace files instead of inventing acceptance dates', async () => {
 const { createFilesystemTraceStore } = await import('../src/storage/filesystem.js');
 const f = await host(); await createFilesystemTraceStore({ directory: f.directory, compression: 'gzip' }).store(trace());
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_STORAGE_UNCONFIRMED' });
});
it('bounds maintenance even if no records are expired', async () => {
 const f = await host(); await f.store.store(trace());
 expect(await f.store.sweep({ limit: 1 })).toEqual({ deleted: 0 });
 await expect(f.store.sweep({ limit: 0 })).rejects.toMatchObject({ code: 'TRACE_INVALID_OPTIONS' });
});
it('handles asynchronous health failure without an unhandled rejection', async () => {
 const f = await host({ onHealth: async () => { throw Error('private asynchronous text'); } }); disk.free = 1n;
 await expect(f.store.store(trace())).rejects.toMatchObject({ code: 'TRACE_DISK_PRESSURE' });
 expect(await f.store.usage()).toMatchObject({ gaps: 1 });
});
