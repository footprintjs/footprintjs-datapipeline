import { ensureDirectory, syncDirectory, writeSynced } from './filesystemIo.js';
import { publishTraceBytes } from './tracePublication.js';
import { checkedWrite, validRef, sameRef, refKey } from './filesystemRecords.js';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, readdir, rename, rm } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import type {
  CommitReceipt, RecordRef, StoredRecord, TraceRecord, TraceRef,
} from '../contracts.js';
import { MAX_REPOSITORY_READ_BATCH, type CommitWrite, type OperationCommit, type Repository, type TraceStore } from '../ports.js';
import { canonical } from '../validation.js';
import { compressTrace, readGzipTrace, MAX_COMPRESSED_TRACE_CONTENT_BYTES } from './traceCompression.js';
import { PackedReads } from './packedReads.js';
import { readOperationEncoding } from './operationEncoding.js';
import { FilesystemRepositoryError } from './filesystemError.js';
export { FilesystemRepositoryError } from './filesystemError.js';
import { encodePackedOperation, readPackedOperation, PACKED_OPERATION_FILE, MAX_PACKED_OPERATION_BYTES, type PackedOperation } from './packedOperation.js';

export interface FilesystemOptions {
  /** Explicit storage location; there is no implicit in-memory fallback. */
  readonly directory: string;
}

export interface FilesystemRepositoryOptions extends FilesystemOptions {
  /** New operations only; upgrade all readers before enabling. Default: none. */
  readonly packing?: 'none' | 'gzip';
  /** Packed content limit before encoding and during inflate; may lower 16 MiB. */
  readonly maxPackedBytes?: number;
}

export interface FilesystemTraceOptions extends FilesystemOptions {
  /** New writes only; existing plain traces are never rewritten. Default: none. */
  readonly compression?: 'none' | 'gzip';
  /** Gzip content ceiling, before compression and during decompression; may lower 16 MiB. */
  readonly maxTraceBytes?: number;
}

interface Entry {
  readonly ref: RecordRef;
  readonly file: string;
  readonly sha256: string;
}
interface Manifest {
  readonly version: 1;
  readonly operationId: string;
  readonly fingerprint: string;
  readonly receipt: CommitReceipt;
  readonly records: readonly Entry[];
}

const MAX_RECORD_READ_CONCURRENCY = 8;

const digest = (text: string | Uint8Array): string => createHash('sha256').update(text).digest('hex');
const recordFile = (ref: RecordRef): string => `${ref.kind}-${digest(refKey(ref))}.json`;
const isMissing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'ENOENT';
const corrupt = (message: string): never => { throw new FilesystemRepositoryError('storage_corrupt', message); };

/** Canonical JSON rejects values that could silently change at the storage boundary. */
function json(value: unknown): string {
  try { return canonical(value); }
  catch { throw new FilesystemRepositoryError('invalid_json', 'Records must contain finite, acyclic, plain JSON values.'); }
}




/**
 * File-backed reference backend for one host and a modest number of operations.
 * Publication renames a complete, fsynced directory into place. A concurrent
 * writer cannot replace an already published nonempty directory. Core-created
 * references are namespaced by operation; lookup scans published manifests.
 * Storage must provide atomic directory rename and honor fsync (local POSIX FS).
 * This is not a distributed transaction service or a tamper-evident archive.
 */
export class FilesystemRepository implements Repository {
  readonly directory: string;
  private readonly operations: string;
  private readonly packing: 'none' | 'gzip';
  private readonly maxPackedBytes: number;
  private readonly packedReads: PackedReads<PackedOperation<Manifest>>;

  constructor(options: FilesystemRepositoryOptions) {
    if (!options.directory) throw new FilesystemRepositoryError('invalid_directory', 'An explicit directory is required.');
    this.packing = options.packing ?? 'none';
    this.maxPackedBytes = options.maxPackedBytes ?? MAX_PACKED_OPERATION_BYTES;
    if (!['none', 'gzip'].includes(this.packing) || !Number.isSafeInteger(this.maxPackedBytes) || this.maxPackedBytes < 1 || this.maxPackedBytes > MAX_PACKED_OPERATION_BYTES) {
      throw new FilesystemRepositoryError('invalid_repository_options', 'Packed repository options are invalid.');
    }
    this.directory = resolve(options.directory);
    this.operations = join(this.directory, 'operations');
    this.packedReads = new PackedReads(path => readPackedOperation<Manifest>(path, this.maxPackedBytes));
  }

  protected async operation(operationDirectory: string): Promise<{ manifest: Manifest; packed?: ReadonlyMap<string, string> }> {
    const loaded = await readOperationEncoding(operationDirectory, this.packedReads);
    const value = loaded.manifest;
    if (value?.version !== 1 || typeof value.operationId !== 'string' || typeof value.fingerprint !== 'string' ||
        value.receipt?.operationId !== value.operationId || value.receipt.status !== 'committed' ||
        !Array.isArray(value.records) || value.records.length < 2 ||
        join(this.operations, digest(value.operationId)) !== operationDirectory) corrupt('A published operation manifest has invalid metadata.');
    const keys = new Set<string>();
    for (const entry of value.records) {
      if (!validRef(entry.ref) || entry.file !== recordFile(entry.ref) || !/^[a-f0-9]{64}$/.test(entry.sha256) || keys.has(refKey(entry.ref))) {
        corrupt('A published operation contains an invalid record index.');
      }
      keys.add(refKey(entry.ref));
    }
    return loaded;
  }

  protected async record(directory: string, entry: Entry, packed?: ReadonlyMap<string, string>): Promise<StoredRecord> {
    let bytes: string;
    try {
      const loaded = packed ? packed.get(entry.file) : await readFile(join(directory, entry.file), 'utf8');
      if (loaded === undefined) return corrupt('A packed operation is missing a record.');
      bytes = loaded;
    }
    catch { return corrupt('A published operation is missing a readable record.'); }
    if (digest(bytes) !== entry.sha256) corrupt('A published record failed its content hash check.');
    let record: StoredRecord;
    try { record = JSON.parse(bytes) as StoredRecord; } catch { return corrupt('A published record is invalid JSON.'); }
    if (!validRef(record?.ref) || record.kind !== entry.ref.kind || !sameRef(record.ref, entry.ref)) {
      corrupt('A published record does not match its indexed reference.');
    }
    return record;
  }

  async findOperation(operationId: string): Promise<OperationCommit | undefined> {
    const directory = join(this.operations, digest(operationId));
    let loaded: Awaited<ReturnType<FilesystemRepository['operation']>>;
    try { loaded = await this.operation(directory); } catch (error) { if (isMissing(error)) return undefined; throw error; }
    const { manifest, packed } = loaded;
    if (manifest.operationId !== operationId) corrupt('An operation directory does not match its identity.');
    const records: StoredRecord[] = [];
    for (const entry of manifest.records) records.push(await this.record(directory, entry, packed));
    const batches = records.filter(record => record.kind === 'batch');
    const evidence = records.filter(record => record.kind === 'evidence');
    if (batches.length !== 1 || evidence.length !== 1) corrupt('A committed operation must have one batch and one evidence record.');
    const batch = batches[0]!;
    const proof = evidence[0]!;
    const observations = records.filter(record => record.kind === 'observation');
    try { checkedWrite({ operationId, fingerprint: manifest.fingerprint, batch, evidence: proof, observations }); }
    catch { return corrupt('A committed operation has inconsistent record membership.'); }
    if (!manifest.receipt.batchRef || !sameRef(manifest.receipt.batchRef, batch.ref) ||
        !manifest.receipt.evidenceRef || !sameRef(manifest.receipt.evidenceRef, proof.ref) ||
        !Array.isArray(manifest.receipt.observationRefs) || json(manifest.receipt.observationRefs) !== json(batch.observationRefs)) corrupt('A receipt does not match its published records.');
    // A concurrent reader can arrive after rename but before the publishing
    // writer's fsync. Do not acknowledge that directory entry without syncing it.
    await syncDirectory(this.operations);
    return { fingerprint: manifest.fingerprint, receipt: JSON.parse(json(manifest.receipt)) as CommitReceipt };
  }

  async read(ref: RecordRef): Promise<StoredRecord | undefined> {
    if (!validRef(ref)) throw new FilesystemRepositoryError('invalid_reference', 'A valid record reference is required.');
    let names: string[];
    try { names = await readdir(this.operations); } catch (error) { if (isMissing(error)) return undefined; throw error; }
    let found: StoredRecord | undefined;
    let foundHash: string | undefined;
    for (const name of names.filter(name => /^[a-f0-9]{64}$/.test(name)).sort()) {
      const directory = join(this.operations, name);
      const { manifest, packed } = await this.operation(directory);
      const entry = manifest.records.find(item => sameRef(item.ref, ref));
      if (!entry) continue;
      if (foundHash !== undefined && foundHash !== entry.sha256) corrupt('A reference names conflicting records in different operations.');
      found = await this.record(directory, entry, packed);
      foundHash = entry.sha256;
    }
    return found;
  }

  /** One bounded request; manifests are not cached between calls. */
  async readMany(refs: readonly RecordRef[]): Promise<readonly (StoredRecord | undefined)[]> {
    if (!Array.isArray(refs) || refs.length > MAX_REPOSITORY_READ_BATCH || refs.some(ref => !validRef(ref))) {
      throw new FilesystemRepositoryError('invalid_reference', 'A bounded list of valid record references is required.');
    }
    if (!refs.length) return [];
    let names: string[];
    try { names = await readdir(this.operations); }
    catch (error) { if (isMissing(error)) return refs.map(() => undefined); throw error; }
    const wanted = new Set(refs.map(refKey));
    const found = new Map<string, { hash: string; record: StoredRecord }>();
    for (const name of names.filter(name => /^[a-f0-9]{64}$/.test(name)).sort()) {
      const directory = join(this.operations, name);
      const { manifest, packed } = await this.operation(directory);
      const entries = manifest.records.filter(entry => wanted.has(refKey(entry.ref)));
      // Bound file concurrency without skipping any duplicate-copy hash check.
      for (let i = 0; i < entries.length; i += MAX_RECORD_READ_CONCURRENCY) {
        const results = await Promise.all(entries.slice(i, i + MAX_RECORD_READ_CONCURRENCY).map(async entry => ({ entry, record: await this.record(directory, entry, packed) })));
        for (const { entry, record } of results) {
          const key = refKey(entry.ref), previous = found.get(key);
          if (previous && previous.hash !== entry.sha256) corrupt('A reference names conflicting records in different operations.');
          found.set(key, { hash: entry.sha256, record });
        }
      }
    }
    return refs.map(ref => {
      const value = found.get(refKey(ref))?.record;
      return value === undefined ? undefined : JSON.parse(json(value)) as StoredRecord;
    });
  }

  private existing(operation: OperationCommit, fingerprint: string): CommitReceipt {
    if (operation.fingerprint !== fingerprint) {
      throw new FilesystemRepositoryError('idempotency_conflict', 'This operation ID was already committed with a different request fingerprint.');
    }
    return operation.receipt;
  }

  async commit(write: CommitWrite): Promise<CommitReceipt> {
    const operationId = write.operationId, fingerprint = write.fingerprint;
    let snapshot: CommitWrite | undefined;
    let invalid: unknown;
    try { snapshot = JSON.parse(json(write)) as CommitWrite; } catch (error) { invalid = error; }
    const previous = await this.findOperation(operationId);
    if (previous) return this.existing(previous, fingerprint);
    if (!snapshot) throw invalid;
    write = snapshot;
    const records = checkedWrite(write);
    // Snapshot before awaiting I/O: callers cannot mutate the candidate mid-commit.
    const encoded = records.map(record => ({ ref: JSON.parse(json(record.ref)) as RecordRef, bytes: json(record) }));
    for (const item of encoded) {
      const present = await this.read(item.ref);
      if (present && json(present) !== item.bytes) {
        const winner = await this.findOperation(write.operationId);
        if (winner) return this.existing(winner, write.fingerprint);
        throw new FilesystemRepositoryError('reference_conflict', 'An immutable record reference already names different content.');
      }
    }
    await ensureDirectory(this.operations);
    const staging = await mkdtemp(join(this.operations, '.stage-'));
    const final = join(this.operations, digest(write.operationId));
    let published = false;
    try {
      const receipt: CommitReceipt = {
        operationId: write.operationId,
        status: 'committed',
        batchRef: write.batch.ref,
        evidenceRef: write.evidence.ref,
        observationRefs: [...write.batch.observationRefs],
        committedAt: new Date().toISOString(),
      };
      const entries: Entry[] = encoded.map(item => ({ ref: item.ref, file: recordFile(item.ref), sha256: digest(item.bytes) }));
      const manifest: Manifest = { version: 1, operationId: write.operationId, fingerprint: write.fingerprint, receipt, records: entries };
      if (this.packing === 'gzip') {
        const payload = encoded.map((item, index) => ({ file: entries[index]!.file, bytes: item.bytes }));
        const bytes = await encodePackedOperation(manifest, payload, this.maxPackedBytes);
        await writeSynced(join(staging, PACKED_OPERATION_FILE), bytes);
        // Reopen the synced inode and verify every original content hash before publication.
        const verified = await this.packedReads.read(staging);
        if (json(verified.manifest) !== json(manifest)) corrupt('Packed publication does not match its intended manifest.');
      } else {
        for (const [index, item] of encoded.entries()) await writeSynced(join(staging, entries[index]!.file), item.bytes);
        await writeSynced(join(staging, 'manifest.json'), json(manifest));
      }
      await syncDirectory(staging);
      try { await rename(staging, final); }
      catch (error) {
        const winner = await this.findOperation(write.operationId);
        if (winner) return this.existing(winner, write.fingerprint);
        throw error;
      }
      published = true;
      try { await syncDirectory(this.operations); }
      catch {
        return { operationId: write.operationId, status: 'unknown', error: { code: 'commit_sync_unknown', message: 'Publication occurred but directory synchronization failed; reconcile the operation before retrying.' } };
      }
      // Return the persisted copy, never mutable caller-owned references.
      return JSON.parse(json(receipt)) as CommitReceipt;
    } finally {
      if (!published) await rm(staging, { recursive: true, force: true });
    }
  }
}

export function createFilesystemRepository(options: FilesystemRepositoryOptions): FilesystemRepository {
  return new FilesystemRepository(options);
}

/** Optional, separate post-run trace store. A trace write never commits data. */
export class FilesystemTraceStore implements TraceStore {
  private readonly traces: string;
  private readonly compression: 'none' | 'gzip';
  private readonly maxTraceBytes: number;
  constructor(options: FilesystemTraceOptions) {
    if (!options.directory) throw new FilesystemRepositoryError('invalid_directory', 'An explicit directory is required.');
    this.compression = options.compression ?? 'none';
    this.maxTraceBytes = options.maxTraceBytes ?? MAX_COMPRESSED_TRACE_CONTENT_BYTES;
    if (!['none', 'gzip'].includes(this.compression) || !Number.isSafeInteger(this.maxTraceBytes) ||
        this.maxTraceBytes < 1 || this.maxTraceBytes > MAX_COMPRESSED_TRACE_CONTENT_BYTES) {
      throw new FilesystemRepositoryError('invalid_trace_options', 'Trace codec settings are invalid.');
    }
    this.traces = join(resolve(options.directory), 'traces');
  }

  async store(trace: TraceRecord): Promise<TraceRef> {
    const bytes = json(trace);
    const id = digest(bytes);
    if (this.compression === 'gzip' && Buffer.byteLength(bytes) > this.maxTraceBytes) {
      throw new FilesystemRepositoryError('trace_limit', 'Trace content exceeds the gzip limit.');
    }
    const previous = await this.read({ kind: 'trace', id });
    if (previous !== undefined) return { kind: 'trace', id };
    const encoded = this.compression === 'gzip' ? await compressTrace(bytes) : bytes;
    await publishTraceBytes(this.traces, id, encoded, this.compression === 'gzip');
    return { kind: 'trace', id };
  }

  async read(ref: TraceRef): Promise<TraceRecord | undefined> {
    if (ref.kind !== 'trace' || !/^[a-f0-9]{64}$/.test(ref.id)) throw new FilesystemRepositoryError('invalid_reference', 'A valid trace reference is required.');
    let bytes: string | Buffer;
    try { bytes = await readFile(join(this.traces, `${ref.id}.json`), 'utf8'); }
    catch (error) {
      if (!isMissing(error)) throw error;
      try { bytes = await readGzipTrace(join(this.traces, `${ref.id}.json.gz`), this.maxTraceBytes); }
      catch (compressedError) {
        if (isMissing(compressedError)) return undefined;
        return corrupt('A compressed trace is unreadable or exceeds its content limit.');
      }
    }
    if (digest(bytes) !== ref.id) corrupt('A stored trace failed its content hash check.');
    try { return JSON.parse(typeof bytes === 'string' ? bytes : bytes.toString('utf8')) as TraceRecord; }
    catch { return corrupt('A stored trace is invalid JSON.'); }
  }
}

export function createFilesystemTraceStore(options: FilesystemTraceOptions): FilesystemTraceStore {
  return new FilesystemTraceStore(options);
}

export { createGuardedFilesystemTraceStore, GuardedFilesystemTraceStore, TraceStorageError, TRACE_RETENTION_MS } from './guardedTrace/index.js';
export type { GuardedTraceOptions, TraceStorageHealth, TraceStorageReceipt, TraceStorageGapReceipt, TraceStorageCode } from './guardedTrace/index.js';

export { createRetainedFilesystemRepository, RetainedFilesystemRepository, RepositoryRetentionError } from './retainedRepository/index.js';
export type { RetainedRepositoryOptions, RepositoryRetentionReceipt, RepositoryRetentionCode } from './retainedRepository/index.js';
