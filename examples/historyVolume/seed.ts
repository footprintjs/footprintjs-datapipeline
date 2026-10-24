import { createHash } from 'node:crypto';
import { statfsSync } from 'node:fs';
import type { DatabaseSync } from 'node:sqlite';
import { DAY, RETENTION_MS } from './fixture.js';
const digest = (value: string) => createHash('sha256').update(value).digest('hex');
export function seedDay(db: DatabaseSync, directory: string, day: number, daily: number, refs: number, signal?: AbortSignal) {
  // Bulk setup only, not timed as production publication. The normal schema's
  // foreign keys/counter triggers remain on. Setup uses 10,000-row transactions;
  // production retirement still has its original small work/retry bounds. No pretend device/payload capture.
  const insert = db.prepare("INSERT INTO operations VALUES(?,?,?,?,?,?,0,?,?,?,?)");
  const ref = db.prepare('INSERT INTO refs VALUES(?,?)');
  for (let base = 0; base < daily; base += 10000) {
    signal?.throwIfAborted();
    const free = statfsSync(directory, { bigint: true });
    if (free.bavail * free.bsize < 2n * 1024n ** 3n) throw Error('Measurement stopped at 2 GiB free-space floor');
    db.exec('BEGIN IMMEDIATE');
    try {
      for (let slot = base; slot < Math.min(daily, base + 10000); slot++) {
        const logical = digest(`day-${day}-slot-${slot}`), id = `fp3:abcdef0123456789abcdef0123456789:${day}:o:${logical}`;
        const hash = digest(id), age = day * DAY + Math.floor(slot * DAY / daily), wall = 1_790_000_000_000 + age;
        insert.run(hash, digest('fingerprint:' + id), digest('write:' + id), wall, wall + RETENTION_MS, slot % 1000 ? 'stored' : 'pending', slot % 1000 ? 1 : 0, age, age + RETENTION_MS, day);
        for (let i = 0; i < refs; i++) ref.run(digest(`reference-${i}:${id}`), hash);
      }
      db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
}
/** Represents independently confirmed file deletion in this metadata-only fixture. */
export function expiredFilesGone(db: DatabaseSync, period: number) {
  let cursor = '';
  for (;;) {
    const rows = db.prepare('SELECT operationHash FROM operations WHERE period=? AND operationHash>? ORDER BY operationHash LIMIT 1000').all(period, cursor);
    if (!rows.length) return;
    db.exec('BEGIN IMMEDIATE');
    try {
      const update = db.prepare("UPDATE operations SET status='expired',dataDeleted=1 WHERE operationHash=?");
      for (const row of rows) update.run(row.operationHash!);
      db.exec('COMMIT'); cursor = String(rows.at(-1)!.operationHash);
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
}
export function originalId(day: number, slot: number) {
  return `fp3:abcdef0123456789abcdef0123456789:${day}:o:${digest(`day-${day}-slot-${slot}`)}`;
}
