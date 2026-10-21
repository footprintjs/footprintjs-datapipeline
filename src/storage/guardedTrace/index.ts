import { lstat, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import type { TraceRecord, TraceRef } from '../../contracts.js';
import type { TraceStore } from '../../ports.js';
import { canonical } from '../../validation.js';
import { createHash } from 'node:crypto';
import { publishTraceBytes } from '../tracePublication.js';
import { createFilesystemTraceStore, type FilesystemTraceStore } from '../filesystem.js';
import { compressTrace, MAX_COMPRESSED_TRACE_CONTENT_BYTES } from '../traceCompression.js';
import { checked, traceId, TRACE_METADATA_ALLOWANCE, TRACE_RETENTION_MS, TraceStorageError, type GuardedTraceOptions, type TraceStorageHealth, type TraceStorageReceipt, type TraceStorageGapReceipt } from './contracts.js';
import { prepareDirectory, capacity } from './paths.js';
import { TraceLedger } from './ledger.js';
import { sweepExpired } from './cleanup.js';
export * from './contracts.js';
/** Explicit host integration; no timer, production activation or raw-response capture. */
export class GuardedFilesystemTraceStore implements TraceStore {
 private ledger?: TraceLedger;
 private base?: FilesystemTraceStore;
 private path?: string;
 private busy = false;
 constructor(private readonly options: GuardedTraceOptions) { checked(options); this.options = Object.freeze({ ...options }); }
 private alert(event: TraceStorageHealth): void { try { void Promise.resolve(this.options.onHealth(Object.freeze(event))).catch(() => {}); } catch { /* Independent health callbacks cannot change storage outcomes. */ } }
 private now(): number { const n = (this.options.clock ?? Date.now)(); if (!Number.isSafeInteger(n) || n < 0 || !Number.isSafeInteger(n + TRACE_RETENTION_MS)) throw new TraceStorageError('TRACE_INVALID_OPTIONS'); return n; }
 private async ready(): Promise<void> {
  if (this.ledger) return;
  const path = await prepareDirectory(this.options);
  try {
   await lstat(join(path, 'retention.sqlite'));
  } catch (error) {
   if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
   try { if ((await readdir(join(path, 'traces'))).length) throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED'); }
   catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; }
  }
  const ledger = new TraceLedger(path);
  try { ledger.bindPolicy(canonical({ version: 1, retentionMs: TRACE_RETENTION_MS, maxTraceBytesOnDisk: this.options.maxTraceBytesOnDisk, minFreeBytes: this.options.minFreeBytes, receiptReserveBytes: this.options.receiptReserveBytes, warningAtBytes: this.options.warningAtBytes })); }
  catch (error) { ledger.close(); throw error; }
  this.ledger = ledger; this.path = path;
  this.base = createFilesystemTraceStore({ directory: path, compression: 'gzip' });
 }
 private async exclusive<T>(fn: () => Promise<T>): Promise<T> {
  if (this.busy) throw new TraceStorageError('TRACE_STORE_BUSY');
  this.busy = true;
  try { await this.ready(); return await fn(); }
  catch (error) {
   if (error instanceof TraceStorageError) throw error;
   this.alert({ code: 'TRACE_STORAGE_UNCONFIRMED', receiptSaved: false });
   throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED');
  } finally { this.busy = false; }
 }
 async store(trace: TraceRecord): Promise<TraceRef> {
  if (this.busy) throw new TraceStorageError('TRACE_STORE_BUSY');
  let text: string;
  try {
   text = canonical(trace);
   if (Buffer.byteLength(text) > MAX_COMPRESSED_TRACE_CONTENT_BYTES) throw new Error();
  } catch { throw new TraceStorageError('TRACE_INVALID_OPTIONS'); }
  return this.exclusive(async () => {
   const ref: TraceRef = { kind: 'trace', id: createHash('sha256').update(text).digest('hex') }, now = this.now();
   const compressed = await compressTrace(text), disk = await capacity(this.path!);
   const reserved = Math.ceil(compressed.length / disk.block) * disk.block + TRACE_METADATA_ALLOWANCE;
   const result = this.ledger!.reserve(ref.id, reserved, now, disk.free, this.options);
   if (result.refusal) { this.alert({ code: result.refusal, receiptSaved: true, traceRef: ref }); throw new TraceStorageError(result.refusal); }
   if (result.existing) { this.available(result.existing); if (!await this.base!.read(ref)) throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED'); return ref; }
   try {
   // Keep the existing reader's corruption/orphan check, without re-encoding.
   if (await this.base!.read(ref) === undefined) await publishTraceBytes(join(this.path!, 'traces'), ref.id, compressed, true);
   const info = await lstat(join(this.path!, 'traces', `${ref.id}.json.gz`));
   if (!info.isFile() || info.isSymbolicLink()) throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED');
   this.ledger!.stored(ref.id, info.blocks * 512);
   if (this.ledger!.traceBytes() >= this.options.warningAtBytes) this.alert({ code: 'TRACE_BUDGET_WARNING', receiptSaved: true, traceRef: ref });
   return ref;
   } catch {
    let receiptSaved = false;
    try { this.ledger!.gap(ref.id, now); receiptSaved = true; } catch { /* An independent health channel must report receipt failure too. */ }
    this.alert({ code: 'TRACE_STORAGE_UNCONFIRMED', receiptSaved, traceRef: ref });
    throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED');
   }
  });
 }
 private available(row: TraceStorageReceipt): void {
  if (row.status === 'expired' || row.expiresAt <= this.now()) throw new TraceStorageError('TRACE_EXPIRED');
  if (row.status !== 'stored') throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED');
 }
 async read(ref: TraceRef): Promise<TraceRecord | undefined> {
  return this.exclusive(async () => { const row = this.ledger!.row(traceId(ref)); if (!row) { const gap = this.ledger!.lastGap(ref.id); if (gap) throw new TraceStorageError(gap.code); return undefined; } this.available(row); const value = await this.base!.read(ref); if (!value) throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED'); return value; });
 }
 async receipt(ref: TraceRef): Promise<TraceStorageReceipt | TraceStorageGapReceipt | undefined> { return this.exclusive(async () => this.ledger!.row(traceId(ref)) ?? this.ledger!.lastGap(ref.id)); }
 async usage(): Promise<{ traceBytes: number; gaps: number }> { return this.exclusive(async () => this.ledger!.usage()); }
 async sweep(options: { limit: number }): Promise<{ deleted: number }> {
  return this.exclusive(() => sweepExpired(this.path!, this.ledger!, this.now(), options.limit));
 }
 close(): void { if (this.busy) throw new TraceStorageError('TRACE_STORE_BUSY'); this.ledger?.close(); delete this.ledger; }
}
export function createGuardedFilesystemTraceStore(options: GuardedTraceOptions): GuardedFilesystemTraceStore | undefined {
 if (options.enabled !== true) return undefined;
 return new GuardedFilesystemTraceStore(options);
}
