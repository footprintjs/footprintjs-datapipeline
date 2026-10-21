import type { DatabaseSync } from 'node:sqlite';
import { fail } from './layout.js';

/** Empty legacy guards keep an old constructor probe from changing the schema. */
const tables = {
  policy: 'CREATE TABLE policy(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,retentionMs INTEGER NOT NULL)',
  sealed_history: 'CREATE TABLE sealed_history(id INTEGER PRIMARY KEY CHECK(id=1),kind TEXT NOT NULL,version INTEGER NOT NULL)',
  operations: 'CREATE TABLE operations(operationHash TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,writeHash TEXT NOT NULL,acceptedAt INTEGER NOT NULL,expiresAt INTEGER NOT NULL,status TEXT NOT NULL,dataDeleted INTEGER NOT NULL DEFAULT 0,confirmed INTEGER NOT NULL DEFAULT 0)',
  refs: 'CREATE TABLE refs(refHash TEXT PRIMARY KEY,operationHash TEXT NOT NULL)',
};
const expiry = 'CREATE INDEX expiry ON operations(expiresAt) WHERE dataDeleted=0';
const expected = [
  ...Object.entries(tables).map(([name, sql]) => ({ type: 'table', name, tbl_name: name, sql })),
  { type: 'index', name: 'expiry', tbl_name: 'operations', sql: expiry },
  { type: 'index', name: 'sqlite_autoindex_operations_1', tbl_name: 'operations', sql: null },
  { type: 'index', name: 'sqlite_autoindex_refs_1', tbl_name: 'refs', sql: null },
].sort((a, b) => a.name < b.name ? -1 : 1);
export function createSchema(db: DatabaseSync, retentionMs: number): void {
  db.exec('PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;');
  for (const sql of Object.values(tables)) db.exec(sql);
  db.exec(expiry);
  db.prepare('INSERT INTO policy VALUES(1,2,?)').run(retentionMs);
  db.exec("INSERT INTO sealed_history VALUES(1,'exact-metadata',1)");
}
export function checkSchema(db: DatabaseSync, retentionMs: number): void {
  const actual = db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name').all();
  const policy = actual.find(row => row.name === 'policy');
  if (policy?.type !== 'table' || policy.sql !== tables.policy) fail('SEALED_HISTORY_CORRUPT');
  const policies = db.prepare('SELECT * FROM policy').all();
  if (policies.length !== 1 || policies[0]?.id !== 1 || policies[0]?.version !== 2) fail('SEALED_HISTORY_UNMANAGED');
  if (policies[0].retentionMs !== retentionMs) fail('SEALED_HISTORY_INVALID_OPTIONS');
  if (actual.length !== expected.length || actual.some((row, i) => {
    const wanted = expected[i]!;
    return row.type !== wanted.type || row.name !== wanted.name || row.tbl_name !== wanted.tbl_name || row.sql !== wanted.sql;
  })) fail('SEALED_HISTORY_CORRUPT');
  const descriptors = db.prepare('SELECT * FROM sealed_history').all();
  if (descriptors.length !== 1 || descriptors[0]?.id !== 1 || descriptors[0]?.kind !== 'exact-metadata') fail('SEALED_HISTORY_CORRUPT');
  if (descriptors[0].version !== 1) fail('SEALED_HISTORY_UNSUPPORTED_FORMAT');
  for (const name of ['operations', 'refs']) {
    if (db.prepare(`SELECT 1 FROM ${name} LIMIT 1`).get()) fail('SEALED_HISTORY_CORRUPT');
  }
}
