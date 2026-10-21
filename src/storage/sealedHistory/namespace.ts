import { open, readdir } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { accessDenied, linkOnce, missing, readBounded, syncDirectory } from './files.js';
import { fail, SealedHistoryError } from './contracts.js';
import { checkSchema, createSchema } from './schema.js';
import { clearUnmarkedStaging, createMarkerStage, removeMarkerStage } from './staging.js';

const MARKER_LIMIT = 1024 * 1024;
export async function checkNamespace(directory: string, retentionMs: number): Promise<boolean> {
  const path = join(directory, 'retention.sqlite');
  try { await readBounded(path, MARKER_LIMIT); }
  catch (error) {
    if (!missing(error)) throw error;
    const names = await readdir(directory);
    // A competing publisher may link its marker between the missing read and list.
    if (names.includes('retention.sqlite')) return checkNamespace(directory, retentionMs);
    if (names.some(name => name !== '.staging')) fail('SEALED_HISTORY_UNMANAGED');
    await clearUnmarkedStaging(directory);
    return false;
  }
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  try {
    const db = new DatabaseSync(path, { readOnly: true });
    try { checkSchema(db, retentionMs); return true; } finally { db.close(); }
  } catch (error) {
    if (error instanceof SealedHistoryError) throw error;
    const code = (error as { errcode?: number }).errcode;
    if (accessDenied(error) || (code !== undefined && [3, 23].includes(code & 0xff))) fail('SEALED_HISTORY_UNMANAGED');
    // SQLite CORRUPT / NOTADB diagnose bytes; BUSY, IOERR and access errors do not.
    fail(code !== undefined && [11, 26].includes(code & 0xff) ? 'SEALED_HISTORY_CORRUPT' : 'SEALED_HISTORY_UNAVAILABLE');
  }
}
/** New namespace only: the actual 0.9 retained reader rejects policy version 2. */
export async function initializeNamespace(directory: string, retentionMs: number): Promise<void> {
  if (await checkNamespace(directory, retentionMs)) {
    // A previous publication may have failed after linking its marker.
    await syncDirectory(join(directory, 'retention.sqlite'));
    await syncDirectory(directory);
    return;
  }
  const staging = await createMarkerStage(directory);
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  try {
    // Precreate exclusively with private mode, including the pre-SQLite kill window.
    const created = await open(staging.marker, 'wx', 0o600); await created.close();
    const db = new DatabaseSync(staging.marker);
    try {
      createSchema(db, retentionMs);
    } finally { db.close(); }
    const handle = await open(staging.marker, 'r');
    try { await handle.sync(); } finally { await handle.close(); }
    await linkOnce(staging.marker, join(directory, 'retention.sqlite'));
    await syncDirectory(directory);
    if (!await checkNamespace(directory, retentionMs)) fail('SEALED_HISTORY_UNCONFIRMED');
  } finally { await removeMarkerStage(staging.directory).catch(() => {}); }
}
