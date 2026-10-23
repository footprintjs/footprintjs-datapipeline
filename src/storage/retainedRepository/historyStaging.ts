import { lstat, mkdir, open, readdir, realpath, rmdir, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { RepositoryRetentionError } from './contracts.js';
import { PACKED_OPERATION_FILE } from '../packedOperation.js';
const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'ENOENT';
/** Only the managed writer (holding the control transaction) or confirmed-expiry
 * cleanup may call this. Readers never delete staging files. A stable per-intent
 * path prevents repeated killed retries from accumulating unbounded temp files. */
export async function removeHistoryStage(directory: string, hash: string): Promise<void> {
  const parent = join(directory, 'operations'), path = join(parent, '.stage-' + hash);
  try {
    const info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink() || await realpath(path) !== path) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    const names = await readdir(path);
    if (names.some(name => name !== PACKED_OPERATION_FILE)) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    if (names.length) {
      const file = join(path, PACKED_OPERATION_FILE), entry = await lstat(file);
      if (!entry.isFile() || entry.isSymbolicLink() || entry.nlink !== 1) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
      await unlink(file);
    }
    await rmdir(path);
  } catch (error) { if (!missing(error)) throw error; }
  // Concurrent expiry cleanup may already have removed a file. Never confirm
  // deletion while an empty/partly removed stage still exists, and synchronize
  // even when the other cleanup performed the unlink.
  try { await lstat(path); throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED'); }
  catch (error) { if (!missing(error)) throw error; }
  try { const handle = await open(parent, 'r'); try { await handle.sync(); } finally { await handle.close(); } }
  catch (error) { if (!missing(error)) throw error; }
}
export async function createHistoryStage(directory: string, hash: string): Promise<string> {
  await removeHistoryStage(directory, hash);
  const path = join(directory, 'operations', '.stage-' + hash);
  await mkdir(path, { mode: 0o700 });
  return path;
}
