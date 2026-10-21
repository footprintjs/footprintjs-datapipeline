import { lstat, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { prepareDirectory } from '../guardedTrace/paths.js';
import { RepositoryRetentionError, type RetainedRepositoryOptions } from './contracts.js';
export function checkOptions(options: RetainedRepositoryOptions): void {
  if (!Number.isSafeInteger(options.retentionMs) || options.retentionMs < 1 || typeof options.onHealth !== 'function') throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
}
export function time(options: RetainedRepositoryOptions): number {
  const now = (options.clock ?? Date.now)();
  if (!Number.isSafeInteger(now) || now < 0 || !Number.isSafeInteger(now + options.retentionMs)) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  return now;
}
export async function readyDirectory(options: RetainedRepositoryOptions): Promise<string> {
  const path = await prepareDirectory(options);
  try { if ((await lstat(join(path, 'operations'))).isSymbolicLink()) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED'); }
  catch (error) { if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error; }
  try { await lstat(join(path, 'retention.sqlite')); }
  catch (error) {
    if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error;
    try { if ((await readdir(join(path, 'operations'))).length) throw new RepositoryRetentionError('REPOSITORY_UNMANAGED'); }
    catch (readError) { if ((readError as NodeJS.ErrnoException)?.code !== 'ENOENT') throw readError; }
  }
  return path;
}
