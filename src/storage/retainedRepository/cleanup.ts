import { lstat, open, rmdir, unlink, realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { PACKED_OPERATION_FILE } from '../packedOperation.js';
import { RepositoryRetentionError } from './contracts.js';
/** Delete only the managed single packed file; no recursive traversal or trace deletion. */
export async function removeOperation(directory: string, hash: string): Promise<void> {
  const parent = join(directory, 'operations'), path = join(parent, hash);
  try {
    if (await realpath(parent) !== parent || await realpath(path) !== path) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    const info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink()) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    await unlink(join(path, PACKED_OPERATION_FILE)).catch(error => { if (error.code !== 'ENOENT') throw error; });
    await rmdir(path);
  } catch (error) { if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error; }
  try { const handle = await open(parent, 'r'); try { await handle.sync(); } finally { await handle.close(); } }
  catch (error) { if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error; }
}

