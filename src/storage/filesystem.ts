import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, open, readFile, readdir, rename, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import type {
  CommitReceipt, RecordRef, StoredRecord, TraceRecord, TraceRef,
} from '../contracts.js';
import type { CommitWrite, OperationCommit, Repository, TraceStore } from '../ports.js';
import { canonical } from '../validation.js';
import { compressTrace, readGzipTrace, MAX_COMPRESSED_TRACE_CONTENT_BYTES } from './traceCompression.js';

export interface FilesystemOptions {
  /** Explicit storage location; there is no implicit in-memory fallback. */
  readonly directory: string;
}

export interface FilesystemTraceOptions extends FilesystemOptions {
  /** New writes only; existing plain traces are never rewritten. Default: none. */
  readonly compression?: 'none' | 'gzip';
  /** Gzip content ceiling, before compression and during decompression; may lower 16 MiB. */
  readonly maxTraceBytes?: number;
}

export class FilesystemRepositoryError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = 'FilesystemRepositoryError';
  }
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

const digest = (text: string | Uint8Array): string => createHash('sha256').update(text).digest('hex');
const sameRef = (a: RecordRef, b: RecordRef): boolean => a.kind === b.kind && a.id === b.id;
const refKey = (ref: RecordRef): string => `${ref.kind}:${ref.id}`;
const recordFile = (ref: RecordRef): string => `${ref.kind}-${digest(refKey(ref))}.json`;
const isMissing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'ENOENT';
const corrupt = (message: string): never => { throw new FilesystemRepositoryError('storage_corrupt', message); };

/** Canonical JSON rejects values that could silently change at the storage boundary. */
function json(value: unknown): string {
  try { return canonical(value); }
  catch { throw new FilesystemRepositoryError('invalid_json', 'Records must contain finite, acyclic, plain JSON values.'); }
}

function validRef(value: unknown): value is RecordRef {
  const ref = value as RecordRef | undefined;
  return !!ref && ['batch', 'observation', 'evidence'].includes(ref.kind) && typeof ref.id === 'string' && ref.id.length > 0;
}

async function syncDirectory(path: string): Promise<void> {
  const handle = await open(path, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

async function ensureDirectory(path: string): Promise<void> {
  const firstCreated = await mkdir(path, { recursive: true });
  if (!firstCreated) return;
  let cursor = path;
  for (;;) {
    await syncDirectory(cursor);
    if (cursor === firstCreated) break;
    cursor = dirname(cursor);
  }
  await syncDirectory(dirname(firstCreated));
}

async function writeSynced(path: string, text: string | Uint8Array): Promise<void> {
  const handle = await open(path, 'wx', 0o600);
  try {
    await handle.writeFile(text, 'utf8');
    await handle.sync();
  } finally { await handle.close(); }
}

function checkedWrite(write: CommitWrite): StoredRecord[] {
  const fail = (message: string): never => { throw new FilesystemRepositoryError('invalid_commit', message); };
  if (!write.operationId || !write.fingerprint) fail('An operation ID and request fingerprint are required.');
  if (write.batch.operationId !== write.operationId || write.evidence.operationId !== write.operationId) {
    fail('Batch and evidence must belong to the committed operation.');
  }
  if (write.batch.kind !== 'batch' || write.evidence.kind !== 'evidence' ||
      write.observations.some(item => item.kind !== 'observation') || write.evidence.fingerprint !== write.fingerprint) {
    fail('Record kinds and evidence fingerprint must match the commit.');
  }
  const records: StoredRecord[] = [write.batch, ...write.observations, write.evidence];
  const keys = new Set<string>();
  for (const record of records) {
    if (!validRef(record.ref) || record.kind !== record.ref.kind) fail('A record must have its own matching reference.');
    if (keys.has(refKey(record.ref))) fail('A commit cannot contain duplicate record references.');
    keys.add(refKey(record.ref));
  }
  if (!sameRef(write.batch.evidenceRef, write.evidence.ref) || write.batch.outcome !== write.evidence.outcome) {
    fail('Batch and evidence must agree on evidence reference and outcome.');
  }
  const supplied = new Set(write.observations.map(item => refKey(item.ref)));
  if (write.batch.observationRefs.length !== supplied.size || new Set(write.batch.observationRefs.map(refKey)).size !== supplied.size ||
      write.batch.observationRefs.some(ref => !supplied.has(refKey(ref)))) fail('Batch membership must match the supplied observations.');
  for (const observation of write.observations) {
    if (observation.externalArtifactRef !== undefined) {
      throw new FilesystemRepositoryError('external_artifact_unsupported', 'This repository does not verify or store external artifact bytes; inline the payload or use an artifact-aware repository.');
    }
    if (!Object.prototype.hasOwnProperty.call(observation, 'payload') || observation.payload === undefined) {
      fail('An observation must contain an inline JSON payload.');
    }
    if (!sameRef(observation.batchRef, write.batch.ref) || !sameRef(observation.evidenceRef, write.evidence.ref)) {
      fail('Every observation must reference this batch and evidence.');
    }
  }
  const outputs = new Set(write.evidence.outputRefs.map(refKey));
  const expected = [write.batch.ref, ...write.observations.map(item => item.ref)];
  if (outputs.size !== expected.length || write.evidence.outputRefs.length !== expected.length ||
      expected.some(ref => !outputs.has(refKey(ref)))) fail('Evidence outputs must match this batch and observations.');
  return records;
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

  constructor(options: FilesystemOptions) {
    if (!options.directory) throw new FilesystemRepositoryError('invalid_directory', 'An explicit directory is required.');
    this.directory = resolve(options.directory);
    this.operations = join(this.directory, 'operations');
  }

  private async manifest(operationDirectory: string): Promise<Manifest> {
    let value: Manifest;
    try { value = JSON.parse(await readFile(join(operationDirectory, 'manifest.json'), 'utf8')) as Manifest; }
    catch (error) { if (isMissing(error)) throw error; return corrupt('A published operation manifest is unreadable.'); }
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
    return value;
  }

  private async record(directory: string, entry: Entry): Promise<StoredRecord> {
    let bytes: string;
    try { bytes = await readFile(join(directory, entry.file), 'utf8'); }
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
    let manifest: Manifest;
    try { manifest = await this.manifest(directory); } catch (error) { if (isMissing(error)) return undefined; throw error; }
    if (manifest.operationId !== operationId) corrupt('An operation directory does not match its identity.');
    const records: StoredRecord[] = [];
    for (const entry of manifest.records) records.push(await this.record(directory, entry));
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
    return { fingerprint: manifest.fingerprint, receipt: manifest.receipt };
  }

  async read(ref: RecordRef): Promise<StoredRecord | undefined> {
    if (!validRef(ref)) throw new FilesystemRepositoryError('invalid_reference', 'A valid record reference is required.');
    let names: string[];
    try { names = await readdir(this.operations); } catch (error) { if (isMissing(error)) return undefined; throw error; }
    let found: StoredRecord | undefined;
    let foundHash: string | undefined;
    for (const name of names.filter(name => /^[a-f0-9]{64}$/.test(name)).sort()) {
      const directory = join(this.operations, name);
      const manifest = await this.manifest(directory);
      const entry = manifest.records.find(item => sameRef(item.ref, ref));
      if (!entry) continue;
      if (foundHash !== undefined && foundHash !== entry.sha256) corrupt('A reference names conflicting records in different operations.');
      found = await this.record(directory, entry);
      foundHash = entry.sha256;
    }
    return found;
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
      const entries: Entry[] = [];
      for (const item of encoded) {
        const file = recordFile(item.ref);
        await writeSynced(join(staging, file), item.bytes);
        entries.push({ ref: item.ref, file, sha256: digest(item.bytes) });
      }
      const manifest: Manifest = { version: 1, operationId: write.operationId, fingerprint: write.fingerprint, receipt, records: entries };
      await writeSynced(join(staging, 'manifest.json'), json(manifest));
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

export function createFilesystemRepository(options: FilesystemOptions): FilesystemRepository {
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
    await ensureDirectory(this.traces);
    const final = join(this.traces, `${id}.json${this.compression === 'gzip' ? '.gz' : ''}`);
    const staging = await mkdtemp(join(this.traces, '.stage-'));
    try {
      const temporary = join(staging, 'trace.json');
      await writeSynced(temporary, encoded);
      await rename(temporary, final);
      await syncDirectory(this.traces);
      return { kind: 'trace', id };
    } finally { await rm(staging, { recursive: true, force: true }); }
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
