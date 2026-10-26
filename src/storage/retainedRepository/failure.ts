import { FilesystemRepositoryError } from '../filesystemError.js';
import { RepositoryRetentionError, type RetainedRepositoryOptions, type RepositoryRetentionCode } from './contracts.js';
/** Fixed diagnostics also cover filesystem work outside a database transaction. */
export function storageFailure(options: RetainedRepositoryOptions, error: unknown): never {
  const safe = error instanceof RepositoryRetentionError || error instanceof FilesystemRepositoryError
    ? error : new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
  let code: RepositoryRetentionCode | undefined;
  if (safe instanceof RepositoryRetentionError && ['REPOSITORY_BUSY', 'REPOSITORY_UNCONFIRMED'].includes(safe.code)) code = safe.code;
  if (safe instanceof FilesystemRepositoryError && safe.code === 'storage_corrupt') code = 'REPOSITORY_UNCONFIRMED';
  if (code) {
    try { void Promise.resolve(options.onHealth({ code })).catch(() => {}); }
    catch { /* Independent health cannot change the storage outcome. */ }
  }
  throw safe;
}
