import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect, it, vi } from 'vitest';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { DAY, fixture, scopedWrite, drainHistory } from './helpers/history-repository.js';

afterEach(() => vi.restoreAllMocks());

type StartupStage = 'history-schema' | 'authority-table' | 'authority-token' | 'budget-activation' | 'authority-activation';

/** Real engine allocation failures, with no synthetic error codes or physical disk fill. */
function nativeFullOnce(stage: StartupStage) {
  const exec = DatabaseSync.prototype.exec, prepare = DatabaseSync.prototype.prepare;
  const connections = new Set<DatabaseSync>();
  let armed = true, nativeCode: number | undefined;
  const fail = (db: DatabaseSync, write?: string): never => {
    armed = false;
    const pages = prepare.call(db, 'PRAGMA page_count').get()!.page_count;
    exec.call(db, `PRAGMA max_page_count=${pages}`);
    try {
      // CREATE uses the actual startup statement. Small binding/activation writes
      // need not allocate a page, so force native FULL at their write boundary.
      exec.call(db, write ?? 'CREATE TABLE startup_full(payload)');
    } catch (error) { nativeCode = (error as { errcode?: number }).errcode; throw error; }
    throw new Error('Expected native SQLite FULL');
  };
  vi.spyOn(DatabaseSync.prototype, 'prepare').mockImplementation(function (this: DatabaseSync, sql: string) {
    if (armed) connections.add(this);
    const statement = prepare.call(this, sql);
    if (armed && ((stage === 'authority-token' && sql === 'INSERT INTO authority VALUES(1,?)') ||
      (stage === 'authority-activation' && sql.startsWith("UPDATE clock_authority SET state='active'")))) {
      vi.spyOn(statement, 'run').mockImplementationOnce(() => fail(this));
    }
    return statement;
  });
  vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: DatabaseSync, sql: string) {
    if (armed) {
      connections.add(this);
      if ((stage === 'history-schema' && sql.startsWith('CREATE TABLE IF NOT EXISTS retired_history')) ||
        (stage === 'authority-table' && sql.startsWith('CREATE TABLE authority('))) return fail(this, sql);
      if (stage === 'budget-activation' && sql === 'PRAGMA cache_spill=OFF') return fail(this);
    }
    return exec.call(this, sql);
  });
  return () => {
    expect(nativeCode).toBe(13);
    expect(connections.size).toBe(2);
    for (const connection of connections) expect(() => connection.prepare('SELECT 1')).toThrow();
  };
}

function authority(directory: string) {
  const db = new DatabaseSync(join(directory, 'retention.sqlite'));
  try {
    expect(db.prepare('SELECT version FROM policy WHERE id=1').get()).toMatchObject({ version: 6 });
    return db.prepare('SELECT state,token FROM clock_authority WHERE id=1').get()!;
  } finally { db.close(); }
}

it.each<StartupStage>(['history-schema', 'authority-table', 'authority-token', 'budget-activation', 'authority-activation'])(
  'maps history startup FULL at %s and preserves initialization recovery, cleanup and retirement', async stage => {
    // scopedWrite opens its namespace to validate identity. Prepare the candidate
    // in a separate fixture with the same policy, leaving the target unopened.
    const preparer = await fixture();
    const write = await (async () => {
      try { return await scopedWrite(preparer.open(0).repository, 'history-startup-full'); }
      finally { await preparer.close(); }
    })();
    const f = await fixture(), first = f.open(0);
    const assertFailure = nativeFullOnce(stage);
    const publication = vi.spyOn(FilesystemRepository.prototype, 'commit');
    try {
      await expect(first.repository.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
      assertFailure();
      expect(f.alerts).toEqual([{ code: 'REPOSITORY_METADATA_LIMIT' }]);
      expect(publication).not.toHaveBeenCalled();
      const initializing = authority(f.directory);
      expect(initializing).toMatchObject({ state: 'initializing' });
      const ledger = new DatabaseSync(join(f.directory, 'retention-v4.sqlite'));
      try {
        if (stage === 'authority-activation') {
          // The complete empty ledger committed before fence activation failed.
          expect(ledger.prepare('SELECT token FROM authority WHERE id=1').get()).toMatchObject({ token: initializing.token });
          expect(ledger.prepare('SELECT operations,refs FROM history_counts WHERE id=1').get()).toMatchObject({ operations: 0, refs: 0 });
          expect(ledger.prepare('SELECT ageMs,revision FROM elapsed_clock WHERE id=1').get()).toMatchObject({ ageMs: 0, revision: 0 });
          expect(ledger.prepare('SELECT 1 FROM operations').all()).toEqual([]);
          expect(ledger.prepare('SELECT 1 FROM refs').all()).toEqual([]);
        } else {
          // SQLite FULL rolls back the entire first ledger transaction.
          expect(ledger.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all()).toEqual([]);
        }
      } finally { ledger.close(); }

      expect(await first.repository.receipt(write.operationId)).toBeUndefined();
      expect(await first.repository.metadataCapacity()).toMatchObject({ operations: 0, references: 0 });
      expect(await first.repository.historyStatus()).toMatchObject({ currentPeriod: 0, retireBefore: 0, generation: 0,
        retiredOperations: 0, retiredReferences: 0, retiredUnconfirmedOperations: 0 });
      expect(authority(f.directory)).toEqual({ state: 'active', token: initializing.token });
      expect(publication).not.toHaveBeenCalled();
      expect(await first.repository.commit(write)).toMatchObject({ status: 'committed', operationId: write.operationId });
      expect(publication).toHaveBeenCalledTimes(1);
      first.repository.close();

      const reopened = f.open(0);
      expect(await reopened.repository.read(write.batch.ref)).toEqual(write.batch);
      expect(await reopened.repository.metadataCapacity()).toMatchObject({ operations: 1, references: 3 });
      f.at(DAY);
      expect(await reopened.repository.sweep({ limit: 10 })).toEqual({ deleted: 1 });
      expect(await reopened.repository.receipt(write.operationId)).toMatchObject({ status: 'expired', publication: 'committed', dataDeleted: true });
      f.at(8 * DAY);
      expect(await drainHistory(reopened.repository, 10)).toMatchObject({ retireBefore: 1, pending: false, blocked: false });
      expect(await reopened.repository.historyStatus()).toMatchObject({ retireBefore: 1, retiredOperations: 1, retiredReferences: 3,
        retiredUnconfirmedOperations: 0 });
      expect(await reopened.repository.metadataCapacity()).toMatchObject({ operations: 0, references: 0 });
      await expect(reopened.repository.receipt(write.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
      expect(publication).toHaveBeenCalledTimes(1);
    } finally { await f.close(); }
  });
