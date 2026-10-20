import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { canonical } from '../validation.js';
import { compressTrace, readGzipTrace } from './traceCompression.js';
import { FilesystemRepositoryError } from './filesystemError.js';

export const PACKED_OPERATION_FILE = 'operation.json.gz';
export const MAX_PACKED_OPERATION_BYTES = 16 * 1024 * 1024;
interface Indexed { readonly records: readonly { readonly file: string; readonly sha256: string }[] }
export interface PackedOperation<T> { readonly manifest: T; readonly records: ReadonlyMap<string, string> }
const hash = (text: string): string => createHash('sha256').update(text).digest('hex');
const corrupt = (): never => { throw new FilesystemRepositoryError('storage_corrupt', 'A packed operation is invalid, corrupt or exceeds its limit.'); };

/** Reuse the existing bounded gzip/UTF-8 codec; no unbounded inflate or pathname extraction. */
export async function readPackedOperation<T extends Indexed>(directory: string, maximum: number): Promise<PackedOperation<T>> {
  try {
    const decoded = await readGzipTrace(join(directory, PACKED_OPERATION_FILE), maximum);
    const packed = JSON.parse(decoded.toString('utf8')) as { version?: unknown; manifest?: T; records?: { file?: unknown; bytes?: unknown }[] };
    if (packed.version !== 1 || !Array.isArray(packed.manifest?.records) || !Array.isArray(packed.records) || packed.records.length !== packed.manifest.records.length) return corrupt();
    const indexed = new Map<string, string>();
    for (const entry of packed.manifest.records) {
      if (!/^(batch|observation|evidence)-[a-f0-9]{64}\.json$/.test(entry.file) || !/^[a-f0-9]{64}$/.test(entry.sha256) || indexed.has(entry.file)) return corrupt();
      indexed.set(entry.file, entry.sha256);
    }
    const records = new Map<string, string>();
    for (const entry of packed.records) {
      if (typeof entry.file !== 'string' || typeof entry.bytes !== 'string' || records.has(entry.file) || indexed.get(entry.file) !== hash(entry.bytes)) return corrupt();
      records.set(entry.file, entry.bytes);
    }
    return { manifest: packed.manifest, records };
  } catch (error) {
    if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') throw error;
    return corrupt();
  }
}

export async function encodePackedOperation(manifest: Indexed, records: readonly { readonly file: string; readonly bytes: string }[], maximum: number): Promise<Buffer> {
  const text = canonical({ version: 1, manifest, records });
  if (Buffer.byteLength(text) > maximum) throw new FilesystemRepositoryError('packed_limit', 'Packed operation content exceeds the configured limit.');
  return compressTrace(text);
}
