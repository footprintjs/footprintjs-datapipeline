import { setTimeout as delay } from 'node:timers/promises';
import { RepositoryRetentionError } from './contracts.js';

// Retry only the supplied database boundary, never its transaction body or I/O.
// SQLite's synchronous busy handler stays disabled so the lock owner can resume.
export const REPOSITORY_ADMISSION_WAIT_MS = 1000;
const RETRY_INTERVAL_MS = 10;
export async function admitTransaction(start: () => void | Promise<void>): Promise<void> {
  const deadline = performance.now() + REPOSITORY_ADMISSION_WAIT_MS;
  for (;;) {
    try { await start(); return; }
    catch (error) {
      if (!(error instanceof RepositoryRetentionError) || error.code !== 'REPOSITORY_BUSY') throw error;
      const remaining = deadline - performance.now();
      if (remaining <= 0) throw error;
      await delay(Math.min(RETRY_INTERVAL_MS, remaining));
      if (performance.now() >= deadline) throw error;
    }
  }
}
