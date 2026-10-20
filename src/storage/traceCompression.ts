import { open } from 'node:fs/promises';
import { isUtf8 } from 'node:buffer';
import { promisify } from 'node:util';
import { gzip, gunzip } from 'node:zlib';

export const MAX_COMPRESSED_TRACE_CONTENT_BYTES = 16 * 1024 * 1024;
const compress = promisify(gzip);
const decompress = promisify(gunzip);
const GZIP_OVERHEAD_ALLOWANCE = 64 * 1024;

/** Bounded read from one opened inode; changing files are rejected, never trusted by pathname. */
async function readBounded(path: string, maximum: number): Promise<Buffer> {
  const handle = await open(path, 'r');
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size > maximum) throw new RangeError('Trace bytes exceed the limit.');
    const buffer = Buffer.alloc(stat.size + 1);
    let used = 0;
    while (used < buffer.length) {
      const { bytesRead } = await handle.read(buffer, used, buffer.length - used, used);
      if (bytesRead === 0) break;
      used += bytesRead;
    }
    if (used !== stat.size) throw new RangeError('Trace bytes changed while reading.');
    return buffer.subarray(0, used);
  } finally { await handle.close(); }
}

export async function compressTrace(text: string): Promise<Buffer> {
  return compress(text);
}

export async function readGzipTrace(path: string, maxTraceBytes: number): Promise<Buffer> {
  const bytes = await readBounded(path, maxTraceBytes + GZIP_OVERHEAD_ALLOWANCE);
  // zlib enforces this while inflating, before an oversized output can be returned.
  const decoded = await decompress(bytes, { maxOutputLength: maxTraceBytes });
  if (!isUtf8(decoded)) throw new TypeError('Trace content must be UTF-8.');
  return decoded;
}
