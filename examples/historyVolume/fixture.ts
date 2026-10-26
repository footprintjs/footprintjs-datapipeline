import type { RetainedRepositoryOptions } from '../../src/storage/retainedRepository/contracts.js';
export const DAY = 86400_000, RETENTION_MS = 7 * DAY;
export function options(directory: string, daily: number, references: number, clock: () => number,
  onHealth: RetainedRepositoryOptions['onHealth']): RetainedRepositoryOptions {
  return { enabled: true, directory, retentionMs: RETENTION_MS, requireSeparateFilesystem: false,
    expiryClock: 'verified-uptime', monotonicClock: clock, clock: () => 1_790_000_000_000,
    metadataLimits: { maxBytes: 12 * 1024 ** 3, maxOperations: 10 * daily + 10000, maxReferences: 10 * daily * references + 100000 },
    historyRetirement: { profile: 'daily-periods-v1', keepDays: 7, namespaceId: 'abcdef0123456789abcdef0123456789', period: 0, maxOperationBytes: 65536 }, onHealth };
}
