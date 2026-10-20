import { readFile, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { FilesystemRepositoryError } from './filesystemError.js';
import { PACKED_OPERATION_FILE, type PackedOperation } from './packedOperation.js';

const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'ENOENT';
const corrupt = (): never => { throw new FilesystemRepositoryError('storage_corrupt', 'A published operation has no unique readable encoding.'); };

/** Format selection has no fallback from malformed, unreadable or ambiguous publication. */
export async function readOperationEncoding<T>(directory: string, packed: { read(path: string): Promise<PackedOperation<T>> }): Promise<{ manifest: T; packed?: ReadonlyMap<string, string> }> {
  let plain: string | undefined;
  try { plain = await readFile(join(directory, 'manifest.json'), 'utf8'); }
  catch (error) { if (!missing(error)) return corrupt(); }
  if (plain !== undefined) {
    try { await stat(join(directory, PACKED_OPERATION_FILE)); return corrupt(); }
    catch (error) { if (!missing(error)) return corrupt(); }
    try { return { manifest: JSON.parse(plain) as T }; } catch { return corrupt(); }
  }
  try {
    const loaded = await packed.read(directory);
    return { manifest: loaded.manifest, packed: loaded.records };
  } catch (error) {
    if (!missing(error)) throw error;
    // Only a missing operation directory is absence. A published directory
    // with missing files, or a failed metadata probe, is corruption/unavailable.
    try { await stat(directory); }
    catch (probe) { if (missing(probe)) throw probe; return corrupt(); }
    return corrupt();
  }
}
