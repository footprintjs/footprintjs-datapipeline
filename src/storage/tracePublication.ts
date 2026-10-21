import { mkdtemp, rename, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { ensureDirectory, syncDirectory, writeSynced } from './filesystemIo.js';

/** Internal publication of already-validated bytes; never a package entry point. */
export async function publishTraceBytes(directory: string, id: string, encoded: string | Uint8Array, gzip: boolean): Promise<void> {
  await ensureDirectory(directory);
  const final = join(directory, `${id}.json${gzip ? '.gz' : ''}`);
  const staging = await mkdtemp(join(directory, '.stage-'));
  try {
    const temporary = join(staging, 'trace.json');
    await writeSynced(temporary, encoded);
    await rename(temporary, final);
    await syncDirectory(directory);
  } finally { await rm(staging, { recursive: true, force: true }); }
}
