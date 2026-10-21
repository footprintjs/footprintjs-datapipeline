import { constants } from 'node:fs';
import { link, lstat, open, realpath, rm, stat } from 'node:fs/promises';
import { isAbsolute, join, parse, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import type { SealedHistoryOptions } from './contracts.js';
import { fail } from './layout.js';

export const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'ENOENT';
export const exists = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'EEXIST';
export async function syncDirectory(path: string): Promise<void> {
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try { await handle.sync(); } finally { await handle.close(); }
}
export async function privateDirectory(path: string): Promise<void> {
  const info = await lstat(path);
  if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || await realpath(path) !== path) fail('SEALED_HISTORY_UNMANAGED');
}
export async function rootDirectory(options: SealedHistoryOptions): Promise<string> {
  const path = resolve(options.directory);
  if (!isAbsolute(options.directory) || path === parse(path).root) fail('SEALED_HISTORY_INVALID_OPTIONS');
  await privateDirectory(path);
  if (options.requireSeparateFilesystem !== false && (await stat(path)).dev === (await stat(parse(path).root)).dev) fail('SEALED_HISTORY_INVALID_OPTIONS');
  return path;
}
/** Open the inode without following a link; cap all reads even if it grows. */
export async function readBounded(path: string, limit: number): Promise<Buffer> {
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const info = await handle.stat();
    if (!info.isFile() || (info.mode & 0o077) !== 0) fail('SEALED_HISTORY_CORRUPT');
    if (info.size > limit) fail('SEALED_HISTORY_LIMIT');
    const chunks: Buffer[] = [];
    let count = 0;
    while (true) {
      const chunk = Buffer.alloc(Math.min(64 * 1024, limit + 1 - count));
      const { bytesRead } = await handle.read(chunk);
      if (!bytesRead) break;
      count += bytesRead;
      if (count > limit) fail('SEALED_HISTORY_LIMIT');
      chunks.push(chunk.subarray(0, bytesRead));
    }
    if (count !== info.size) fail('SEALED_HISTORY_CORRUPT');
    return Buffer.concat(chunks, count);
  } finally { await handle.close(); }
}
export async function stagedFile(directory: string, bytes: Uint8Array): Promise<string> {
  const path = join(directory, '.stage-' + randomUUID());
  const handle = await open(path, 'wx', 0o600);
  try { await handle.writeFile(bytes); await handle.sync(); }
  catch (error) { await rm(path, { force: true }).catch(() => {}); throw error; }
  finally { await handle.close(); }
  return path;
}
/** Never replace a previously published inode. The caller verifies any winner. */
export async function linkOnce(staging: string, target: string): Promise<boolean> {
  try { await link(staging, target); return true; }
  catch (error) { if (exists(error)) return false; throw error; }
}
