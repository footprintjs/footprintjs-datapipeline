import { chmodSync } from 'node:fs';
import { open, readdir, rm } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { linkOnce, missing, readBounded, syncDirectory } from './files.js';
import { fail } from './layout.js';
import { checkSchema, createSchema } from './schema.js';

const MARKER_LIMIT = 1024 * 1024;
export async function checkNamespace(directory: string, retentionMs: number): Promise<boolean> {
  const path = join(directory, 'retention.sqlite');
  try { await readBounded(path, MARKER_LIMIT); }
  catch (error) {
    if (!missing(error)) throw error;
    if ((await readdir(directory)).length) fail('SEALED_HISTORY_UNMANAGED');
    return false;
  }
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    checkSchema(db, retentionMs);
    return true;
  } finally { db.close(); }
}
/** New namespace only: the actual 0.9 retained reader rejects policy version 2. */
export async function initializeNamespace(directory: string, retentionMs: number): Promise<void> {
  if (await checkNamespace(directory, retentionMs)) {
    // A previous publication may have failed after linking its marker.
    await syncDirectory(join(directory, 'retention.sqlite'));
    await syncDirectory(directory);
    return;
  }
  const staging = join(directory, '.marker-' + randomUUID());
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  try {
    const db = new DatabaseSync(staging);
    try {
      chmodSync(staging, 0o600);
      createSchema(db, retentionMs);
    } finally { db.close(); }
    const handle = await open(staging, 'r');
    try { await handle.sync(); } finally { await handle.close(); }
    await linkOnce(staging, join(directory, 'retention.sqlite'));
    await syncDirectory(directory);
    if (!await checkNamespace(directory, retentionMs)) fail('SEALED_HISTORY_UNCONFIRMED');
  } finally { await rm(staging, { force: true }).catch(() => {}); }
}
