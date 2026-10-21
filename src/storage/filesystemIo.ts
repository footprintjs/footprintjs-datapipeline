import { mkdir, open } from 'node:fs/promises';
import { dirname } from 'node:path';

export async function syncDirectory(path: string): Promise<void> {
  const handle = await open(path, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

export async function ensureDirectory(path: string): Promise<void> {
  const firstCreated = await mkdir(path, { recursive: true });
  if (!firstCreated) return;
  let cursor = path;
  for (;;) {
    await syncDirectory(cursor);
    if (cursor === firstCreated) break;
    cursor = dirname(cursor);
  }
  await syncDirectory(dirname(firstCreated));
}

export async function writeSynced(path: string, text: string | Uint8Array): Promise<void> {
  const handle = await open(path, 'wx', 0o600);
  try {
    await handle.writeFile(text, 'utf8');
    await handle.sync();
  } finally { await handle.close(); }
}

