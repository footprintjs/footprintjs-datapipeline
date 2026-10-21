import { mkdir, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { canonical } from '../../validation.js';
import { fail, type SealedHistoryPublication } from './contracts.js';
import { decodeHistory } from './decode.js';
import { readCatalogue, type Catalogue } from './catalogue.js';
import { linkOnce, privateDirectory, readBounded, stagedFile, syncDirectory } from './files.js';
import { hash } from './layout.js';

export async function publishSnapshot(directory: string, bytes: Buffer, retentionMs: number, limit: number): Promise<SealedHistoryPublication> {
  const decoded = decodeHistory(bytes, retentionMs, limit), sha256 = hash(bytes);
  const snapshot = { sha256, bytes: bytes.length, ...decoded.counts };
  const previous = await readCatalogue(directory, retentionMs, limit);
  if (previous) {
    if (previous.catalogue.snapshot.sha256 !== sha256) fail('SEALED_HISTORY_CONFLICT');
    await syncDirectory(join(directory, 'snapshots'));
    await syncDirectory(directory);
    return Object.freeze({ status: 'stored', ...snapshot });
  }
  const snapshots = join(directory, 'snapshots');
  await mkdir(snapshots, { mode: 0o700, recursive: true });
  await privateDirectory(snapshots);
  await syncDirectory(directory);
  const staging = await stagedFile(snapshots, bytes);
  try {
    const verified = await readBounded(staging, limit);
    decodeHistory(verified, retentionMs, limit);
    if (hash(verified) !== sha256) fail('SEALED_HISTORY_CORRUPT');
    const final = join(snapshots, sha256 + '.bin');
    await linkOnce(staging, final);
    const published = await readBounded(final, limit);
    if (hash(published) !== sha256) fail('SEALED_HISTORY_CORRUPT');
    await syncDirectory(snapshots);
  } finally { await rm(staging, { force: true }).catch(() => {}); }
  const catalogue: Catalogue = { version: 1, retentionMs, snapshot };
  const stage = await stagedFile(directory, Buffer.from(canonical(catalogue)));
  try {
    await linkOnce(stage, join(directory, 'catalogue.json'));
    const winner = await readCatalogue(directory, retentionMs, limit);
    if (!winner || winner.catalogue.snapshot.sha256 !== sha256) fail('SEALED_HISTORY_CONFLICT');
    await syncDirectory(directory);
    return Object.freeze({ status: 'stored', ...snapshot });
  } finally { await rm(stage, { force: true }).catch(() => {}); }
}
