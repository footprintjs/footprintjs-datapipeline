import { mkdtemp, realpath, rm, unlink } from 'node:fs/promises';
import { lstatSync, readdirSync, statfsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createRequire } from 'node:module';
import { expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository } from '../src/storage/retainedRepository/index.js';
import { RepositoryLedger } from '../src/storage/retainedRepository/ledger.js';
import { MetadataBudget } from '../src/storage/retainedRepository/metadataBudget.js';
import * as metadataModule from '../src/storage/retainedRepository/metadataBudget.js';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { candidate } from './helpers/filesystem-candidate.js';
import type { CommitWrite } from '../src/ports.js';

async function fixture(maxOperations = 2, maxReferences = 100) {
  const directory = await realpath(await mkdtemp(join(tmpdir(), 'metadata-limits-')));
  let elapsed = 0; const alerts: unknown[] = [];
  const options = { enabled: true, directory, retentionMs: 100, expiryClock: 'verified-uptime' as const,
    clock: () => 1_790_000_000_000, monotonicClock: () => elapsed, requireSeparateFilesystem: false,
    metadataLimits: { maxBytes: 2 * 1024 * 1024, maxOperations, maxReferences },
    onHealth: (event: unknown) => { alerts.push(event); } };
  const repo = createRetainedFilesystemRepository(options)!;
  return { directory, options, repo, alerts, elapsed: (value: number) => { elapsed = value; },
    async close() { repo.close(); vi.restoreAllMocks(); await rm(directory, { recursive: true, force: true }); } };
}
function controlBytes(directory: string): number {
  return readdirSync(directory).filter(name => /^retention(?:-v2)?\.sqlite(?:-(?:journal|wal|shm))?$/.test(name))
    .reduce((sum, name) => { const info = lstatSync(join(directory, name)); return sum + Math.max(info.size, info.blocks * 512); }, 0);
}
function manyRecords(count: number): CommitWrite {
  const seed = candidate('many-records');
  const observations = Array.from({ length: count }, (_, i) => ({ ...seed.observations[0]!,
    ref: { kind: 'observation' as const, id: `many-records:sample-${i}` }, key: `sample-${i}` }));
  return { ...seed, observations, batch: { ...seed.batch, observationRefs: observations.map(item => item.ref) },
    evidence: { ...seed.evidence, outputRefs: [seed.batch.ref, ...observations.map(item => item.ref)] } };
}

function failSqliteWriteOnce(sql: string, armed: () => boolean = () => true) {
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let injected = false;
  const fail = () => { injected = true; throw Object.assign(new Error('private SQLite diagnostic'), { errcode: 13 }); };
  const prepare = DatabaseSync.prototype.prepare;
  vi.spyOn(DatabaseSync.prototype, 'prepare').mockImplementation(function (this: InstanceType<typeof DatabaseSync>, query: string) {
    const statement = prepare.call(this, query);
    if (query === sql && armed() && !injected) vi.spyOn(statement, 'run').mockImplementationOnce(fail);
    return statement;
  });
  const exec = DatabaseSync.prototype.exec;
  vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: InstanceType<typeof DatabaseSync>, query: string) {
    if (query === sql && armed() && !injected) return fail();
    return exec.call(this, query);
  });
  return () => expect(injected).toBe(true);
}

it.each([
  "UPDATE operations SET status='stored',confirmed=1 WHERE operationHash=?",
  'UPDATE elapsed_clock SET ageMs=?,revision=? WHERE id=1',
  'COMMIT',
])('reports metadata exhaustion after native publication at %s without replaying it', async sql => {
  const f = await fixture(10);
  try {
    await f.repo.commit(candidate('seed'));
    let published = false;
    const native = FilesystemRepository.prototype.commit;
    const publication = vi.spyOn(FilesystemRepository.prototype, 'commit').mockImplementation(async function (this: FilesystemRepository, write) {
      const result = await native.call(this, write); published = true; return result;
    });
    const assertInjected = failSqliteWriteOnce(sql, () => published);
    await expect(f.repo.commit(candidate('post-publication-full'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    assertInjected();
    expect(publication).toHaveBeenCalledTimes(1);
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_METADATA_LIMIT' }]);
    expect(await f.repo.receipt('post-publication-full')).toMatchObject({ status: 'pending', publication: 'unconfirmed' });
    expect(await f.repo.findOperation('post-publication-full')).toBeDefined();
    expect(await f.repo.receipt('post-publication-full')).toMatchObject({ status: 'stored', publication: 'committed' });
    expect(publication).toHaveBeenCalledTimes(1);
  } finally { await f.close(); }
});

it.each([
  ["UPDATE operations SET status='expired' WHERE operationHash=?", false],
  ['UPDATE operations SET dataDeleted=1 WHERE operationHash=?', true],
] as const)('reports metadata exhaustion during cleanup at %s without false deletion proof', async (sql, wasUnlinked) => {
  const f = await fixture();
  try {
    await f.repo.commit(candidate('cleanup-full')); f.elapsed(100);
    const assertInjected = failSqliteWriteOnce(sql);
    await expect(f.repo.sweep({ limit: 10 })).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    assertInjected();
    expect(f.alerts).toEqual([{ code: 'REPOSITORY_METADATA_LIMIT' }]);
    expect(readdirSync(join(f.directory, 'operations'))).toHaveLength(wasUnlinked ? 0 : 1);
    expect(await f.repo.receipt('cleanup-full')).toMatchObject({ status: 'expired', publication: 'committed', dataDeleted: false });
    expect(await f.repo.sweep({ limit: 10 })).toEqual({ deleted: 1 });
    expect(await f.repo.receipt('cleanup-full')).toMatchObject({ status: 'expired', dataDeleted: true });
  } finally { await f.close(); }
});

it('checks transient control files only while holding the writer transaction', async () => {
  const f = await fixture();
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let transaction = false, checkCount = 0;
  try {
    await f.repo.commit(candidate('seed'));
    const exec = DatabaseSync.prototype.exec;
    vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: InstanceType<typeof DatabaseSync>, sql: string) {
      const result = exec.call(this, sql); transaction = this.isTransaction; return result;
    });
    const check = MetadataBudget.prototype.checkFiles;
    vi.spyOn(MetadataBudget.prototype, 'checkFiles').mockImplementation(function (this: MetadataBudget) {
      checkCount++; expect(transaction).toBe(true); return check.call(this);
    });
    await f.repo.commit(candidate('checked'));
    await f.repo.metadataCapacity();
    expect(checkCount).toBeGreaterThan(0);
  } finally { await f.close(); }
});

it.each(['throw', 'reject'])('a health callback that %ss cannot replace a metadata refusal', async failure => {
  const f = await fixture(1);
  const onHealth = vi.fn(() => {
    if (failure === 'throw') throw new Error('private health failure');
    return Promise.reject(new Error('private health failure'));
  });
  const repo = createRetainedFilesystemRepository({ ...f.options, onHealth })!;
  try {
    await repo.commit(candidate('accepted'));
    await expect(repo.commit(candidate('refused'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(onHealth).toHaveBeenCalledExactlyOnceWith({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(await repo.receipt('refused')).toBeUndefined();
    expect(await repo.read(candidate('accepted').batch.ref)).toEqual(candidate('accepted').batch);
  } finally { repo.close(); await f.close(); }
});

it('retries an actual busy final COMMIT with metadata limits while a second publisher waits', async () => {
  const f = await fixture(10);
  const other = createRetainedFilesystemRepository(f.options)!;
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let reader: InstanceType<typeof DatabaseSync> | undefined;
  let blocked = 0, armed = true;
  try {
    await f.repo.commit(candidate('seed')); await other.receipt('seed');
    reader = new DatabaseSync(join(f.directory, 'retention-v2.sqlite'));
    const exec = DatabaseSync.prototype.exec;
    vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: InstanceType<typeof DatabaseSync>, sql: string) {
      try { return exec.call(this, sql); }
      catch (error) {
        if (sql === 'COMMIT' && (error as { errcode?: number }).errcode === 5) {
          blocked++; queueMicrotask(() => { if (reader!.isTransaction) reader!.exec('ROLLBACK'); });
        }
        throw error;
      }
    });
    const native = FilesystemRepository.prototype.commit;
    const publication = vi.spyOn(FilesystemRepository.prototype, 'commit').mockImplementation(async function (this: FilesystemRepository, write) {
      const result = await native.call(this, write);
      if (armed) { armed = false; reader!.exec('BEGIN'); reader!.prepare('SELECT * FROM elapsed_clock').all(); }
      return result;
    });
    const results = await Promise.all([f.repo.commit(candidate('one')), other.commit(candidate('two'))]);
    expect(results.map(result => result.status)).toEqual(['committed', 'committed']);
    expect(blocked).toBeGreaterThan(0); expect(publication).toHaveBeenCalledTimes(2);
    expect(await f.repo.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 3, references: 9 });
    expect(await other.read(candidate('one').batch.ref)).toEqual(candidate('one').batch);
    expect(f.alerts).toEqual([]);
  } finally {
    if (reader?.isTransaction) reader.exec('ROLLBACK'); reader?.close(); other.close(); await f.close();
  }
});

it('refuses a new identity at the operation limit, emits health, and keeps exact old identities after expiry', async () => {
  const f = await fixture();
  try {
    await f.repo.commit(candidate('a')); await f.repo.commit(candidate('b'));
    await expect(f.repo.commit(candidate('c'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(await f.repo.receipt('c')).toBeUndefined();
    expect(await f.repo.read(candidate('a').batch.ref)).toEqual(candidate('a').batch);
    f.elapsed(100); expect(await f.repo.sweep({ limit: 10 })).toEqual({ deleted: 2 });
    await expect(f.repo.commit(candidate('a'))).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
    await expect(f.repo.commit(candidate('c'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
  } finally { await f.close(); }
});

it('counts every reference membership, and a rejected intent rolls back without a native publication', async () => {
  const f = await fixture(100, 5);
  try {
    await f.repo.commit(candidate('a'));
    await expect(f.repo.commit(candidate('b'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(await f.repo.findOperation('b')).toBeUndefined();
    expect(await f.repo.metadataCapacity({ operations: 1, references: 3 })).toMatchObject({
      available: false, operations: 1, references: 3, remainingReferences: 2,
    });
  } finally { await f.close(); }
});

it('persists immutable limits, snapshots caller settings and refuses readers without the same limits', async () => {
  const f = await fixture(1);
  try {
    f.options.metadataLimits.maxOperations = 100;
    await f.repo.commit(candidate('a'));
    await expect(f.repo.commit(candidate('b'))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    const changed = createRetainedFilesystemRepository(f.options)!;
    try { await expect(changed.read(candidate('a').batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); }
    finally { changed.close(); }
    const { metadataLimits: _limits, ...withoutLimits } = f.options;
    const unsupported = createRetainedFilesystemRepository(withoutLimits)!;
    try { await expect(unsupported.read(candidate('a').batch.ref)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); }
    finally { unsupported.close(); }
  } finally { await f.close(); }
});

it('bounds native SQLite growth even when a single new intent exhausts its page budget', async () => {
  const f = await fixture(10000, 100000);
  let nativeError: unknown;
  const original = metadataModule.metadataWriteFailure;
  vi.spyOn(metadataModule, 'metadataWriteFailure').mockImplementation(error => {
    nativeError = error; return original(error);
  });
  try {
    await expect(f.repo.commit(manyRecords(5000))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(nativeError).toMatchObject({ errcode: 13 }); // SQLITE_FULL, not an unrelated packing/input refusal.
    expect(await f.repo.receipt('many-records')).toBeUndefined();
    expect(controlBytes(f.directory)).toBeLessThanOrEqual(f.options.metadataLimits.maxBytes);
    expect(await f.repo.findOperation('many-records')).toBeUndefined();
    expect((await f.repo.commit(candidate('small-after-full'))).status).toBe('committed');
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_METADATA_LIMIT' });
  } finally { await f.close(); }
});

it('charges the actual live rollback journal along with the main database', async () => {
  const f = await fixture(); let budget: MetadataBudget | undefined;
  const samples: { journalSize: number; actual: number; reported: number; journalBytes: number }[] = [];
  const check = MetadataBudget.prototype.checkFiles;
  vi.spyOn(MetadataBudget.prototype, 'checkFiles').mockImplementation(function (this: MetadataBudget) {
    budget = this; return check.call(this);
  });
  const intent = RepositoryLedger.prototype.intent;
  vi.spyOn(RepositoryLedger.prototype, 'intent').mockImplementation(function (this: RepositoryLedger, ...args) {
    intent.apply(this, args);
    const journal = lstatSync(join(f.directory, 'retention-v2.sqlite-journal'));
    samples.push({ journalSize: journal.size, actual: controlBytes(f.directory), reported: budget!.checkFiles(),
      journalBytes: Math.max(journal.size, journal.blocks * 512) });
  });
  try {
    await f.repo.commit(candidate('journal')); expect(samples).toHaveLength(1);
    for (const sample of samples) {
      expect(sample.journalSize).toBeGreaterThan(0);
      expect(sample.reported).toBe(sample.actual);
      expect(sample.reported).toBeGreaterThan(sample.actual - sample.journalBytes);
    }
  }
  finally { await f.close(); }
});

it('reserves room for the maximum main file plus a complete rollback image and fixed control files', async () => {
  const f = await fixture();
  try {
    await f.repo.commit(candidate('reserve'));
    const capacity = await f.repo.metadataCapacity();
    const block = statfsSync(f.directory).bsize, page = 4096;
    const mainMaximum = Math.ceil(capacity.databasePageLimit * page / block) * block;
    // SQLite rollback page records have a 4-byte page number and checksum.
    const rollbackMaximum = Math.ceil((capacity.databasePageLimit * (page + 8) + 65536) / block) * block;
    const main = lstatSync(join(f.directory, 'retention-v2.sqlite'));
    const fixedFiles = controlBytes(f.directory) - Math.max(main.size, main.blocks * 512);
    expect(mainMaximum + rollbackMaximum + fixedFiles).toBeLessThanOrEqual(capacity.limits.maxBytes);
  } finally { await f.close(); }
});

it('disables cache spilling so repeated rollback headers cannot bypass the journal reservation', async () => {
  const f = await fixture();
  const { DatabaseSync } = createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let db: InstanceType<typeof DatabaseSync> | undefined;
  try {
    await f.repo.commit(candidate('spill')); f.repo.close();
    db = new DatabaseSync(join(f.directory, 'retention-v2.sqlite'));
    const budget = new MetadataBudget(db, f.directory, f.options.metadataLimits);
    expect(db.prepare('PRAGMA cache_spill').get()!.cache_spill).toBe(0);
    db.exec('PRAGMA cache_spill=ON');
    expect(() => budget.checkFiles()).toThrow(expect.objectContaining({ code: 'REPOSITORY_UNCONFIRMED' }));
  } finally { db?.close(); await f.close(); }
});

it('does not mistake a publication-side error carrying a SQLite-like number for a metadata limit', async () => {
  const f = await fixture();
  const native = vi.spyOn(FilesystemRepository.prototype, 'commit').mockRejectedValue(Object.assign(Error('private'), { errcode: 13 }));
  try {
    await expect(f.repo.commit(candidate('publication-error'))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(native).toHaveBeenCalledTimes(1);
    expect(f.alerts).not.toContainEqual({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(await f.repo.receipt('publication-error')).toMatchObject({ status: 'pending', publication: 'unconfirmed' });
  } finally { await f.close(); }
});

it.each(['retention.sqlite', 'retention-v2.sqlite'])('refuses a missing required control file %s on an already open handle', async name => {
  const f = await fixture();
  try {
    await f.repo.commit(candidate('a'));
    await unlink(join(f.directory, name));
    await expect(f.repo.commit(candidate('b'))).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});

it('exports the capability through the existing filesystem entry and validates capacity requests', async () => {
  const module = await import('../src/storage/filesystem.js');
  expect(module).toHaveProperty('RETAINED_REPOSITORY_SAFETY_VERSION', 1);
  const f = await fixture();
  try {
    for (const request of [{ operations: -1, references: 1 }, { operations: 1, references: 0.5 }]) {
      await expect(f.repo.metadataCapacity(request)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
    }
    expect(await f.repo.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0, available: true });
  } finally { await f.close(); }
});

it('keeps control files bounded during dirty transactions and repeated refusals without forgetting history', async () => {
  const f = await fixture(20, 1000); let peak = 0;
  const original = RepositoryLedger.prototype.intent;
  vi.spyOn(RepositoryLedger.prototype, 'intent').mockImplementation(function (this: RepositoryLedger, ...args) {
    original.apply(this, args); peak = Math.max(peak, controlBytes(f.directory));
  });
  try {
    for (let i = 0; i < 20; i++) await f.repo.commit(candidate('run-' + i));
    const before = controlBytes(f.directory);
    for (let i = 0; i < 100; i++) await expect(f.repo.commit(candidate('refused-' + i))).rejects.toMatchObject({ code: 'REPOSITORY_METADATA_LIMIT' });
    expect(controlBytes(f.directory)).toBe(before);
    expect(peak).toBeGreaterThan(0); expect(peak).toBeLessThanOrEqual(f.options.metadataLimits.maxBytes);
    expect(await f.repo.metadataCapacity()).toMatchObject({ available: false, operations: 20, references: 60 });
    f.repo.close(); const restarted = createRetainedFilesystemRepository(f.options)!;
    try { expect(await restarted.metadataCapacity()).toMatchObject({ available: false, operations: 20, references: 60 }); }
    finally { restarted.close(); }
    console.info(JSON.stringify({ fixture: '20 accepted /100 refused, three refs per run', settledControlBytes: before, observedDirtyPeakBytes: peak,
      maxMetadataBytes: f.options.metadataLimits.maxBytes, retainedOperationIdentities: 20, retainedReferenceMemberships: 60 }));
  } finally { await f.close(); }
});

it.each([{ maxBytes: 1 }, { maxBytes: Infinity }, { maxOperations: 0 }, { maxReferences: -1 }, { maxReferences: 1.5 }])('refuses invalid metadata limits %j', async part => {
  const f = await fixture();
  try { expect(() => createRetainedFilesystemRepository({ ...f.options, metadataLimits: { ...f.options.metadataLimits, ...part } })).toThrow(); }
  finally { await f.close(); }
});
