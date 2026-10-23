import { lstat, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { prepareDirectory } from '../guardedTrace/paths.js';
import { RepositoryRetentionError, type RetainedRepositoryOptions } from './contracts.js';
import { checkedMetadataLimits } from './metadataBudget.js';
import { HISTORY_PERIOD_MS } from './historyIdentity.js';
import { MAX_PACKED_OPERATION_BYTES } from '../packedOperation.js';
export function checkOptions(options: RetainedRepositoryOptions): void {
  if (!Number.isSafeInteger(options.retentionMs) || options.retentionMs < 1 || typeof options.onHealth !== 'function') throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  if ((options.expiryClock !== undefined && options.expiryClock !== 'verified-uptime') ||
    (options.monotonicClock !== undefined && (options.expiryClock !== 'verified-uptime' || typeof options.monotonicClock !== 'function'))) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  if (options.monotonicClock && (options.monotonicClock === Date.now || options.monotonicClock === options.clock)) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  if (options.metadataLimits !== undefined) {
    if (options.expiryClock !== 'verified-uptime') throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
    checkedMetadataLimits(options.metadataLimits);
  }
  if (options.historyRetirement !== undefined) {
    const policy = options.historyRetirement;
    if (!policy || typeof policy !== 'object' || Array.isArray(policy) || options.expiryClock !== 'verified-uptime' || !options.metadataLimits ||
      Object.keys(policy).some(key => !['profile', 'keepDays', 'namespaceId', 'period', 'maxOperationBytes'].includes(key)) ||
      policy.profile !== 'daily-periods-v1' || !Number.isSafeInteger(policy.keepDays) || policy.keepDays < 1 || typeof policy.namespaceId !== 'string' || !/^[a-f0-9]{32}$/.test(policy.namespaceId) ||
      !Number.isSafeInteger(policy.period) || policy.period < 0 || !Number.isSafeInteger((policy.period + policy.keepDays + 1) * HISTORY_PERIOD_MS) ||
      !Number.isSafeInteger(policy.maxOperationBytes) || policy.maxOperationBytes < 1024 || policy.maxOperationBytes > MAX_PACKED_OPERATION_BYTES ||
      options.retentionMs > policy.keepDays * HISTORY_PERIOD_MS) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  }
}
export function time(options: RetainedRepositoryOptions): number {
  const now = (options.clock ?? Date.now)();
  if (!Number.isSafeInteger(now) || now < 0 || !Number.isSafeInteger(now + options.retentionMs)) throw new RepositoryRetentionError('REPOSITORY_INVALID_OPTIONS');
  return now;
}
export async function readyDirectory(options: RetainedRepositoryOptions): Promise<string> {
  const path = await prepareDirectory(options);
  for (const name of ['retention-v2.sqlite', 'retention-v3.sqlite']) try {
    const info = await lstat(join(path, name));
    if (!info.isFile() || info.isSymbolicLink()) throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED');
    try { await lstat(join(path, 'retention.sqlite')); }
    catch { throw new RepositoryRetentionError('REPOSITORY_UNCONFIRMED'); }
  } catch (error) { if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error; }
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
