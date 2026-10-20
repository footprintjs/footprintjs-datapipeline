import { lstat, mkdir, realpath, stat, statfs } from 'node:fs/promises';
import { dirname, isAbsolute, join, parse, resolve } from 'node:path';
import { TraceStorageError, type GuardedTraceOptions } from './contracts.js';
export async function prepareDirectory(options: GuardedTraceOptions): Promise<string> {
 const path = resolve(options.directory);
 if (!isAbsolute(options.directory) || path === parse(path).root) throw new TraceStorageError('TRACE_UNSAFE_FILESYSTEM');
 let ancestor = path;
 while (true) {
  try { await stat(ancestor); break; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; ancestor = dirname(ancestor); }
 }
 if (options.requireSeparateFilesystem !== false && (await stat(ancestor)).dev === (await stat(parse(path).root)).dev) throw new TraceStorageError('TRACE_UNSAFE_FILESYSTEM');
 if (await realpath(ancestor) !== ancestor) throw new TraceStorageError('TRACE_UNSAFE_FILESYSTEM');
 await mkdir(path, { recursive: true, mode: 0o700 });
 const info = await lstat(path);
 if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || await realpath(path) !== path) throw new TraceStorageError('TRACE_UNSAFE_FILESYSTEM');
 for (const name of ['traces', 'retention.sqlite']) {
  try { if ((await lstat(join(path, name))).isSymbolicLink()) throw new TraceStorageError('TRACE_UNSAFE_FILESYSTEM'); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
 }
 return path;
}
export async function capacity(path: string): Promise<{ free: bigint; block: number }> {
 const value = await statfs(path, { bigint: true });
 if (value.bsize <= 0n || value.bsize > BigInt(Number.MAX_SAFE_INTEGER)) throw new TraceStorageError('TRACE_STORAGE_UNCONFIRMED');
 return { free: value.bavail * value.bsize, block: Number(value.bsize) };
}
