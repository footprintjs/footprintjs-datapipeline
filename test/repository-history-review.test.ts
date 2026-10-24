import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect, it, vi } from 'vitest';
import { FilesystemRepository } from '../src/storage/filesystem.js';
import { MetadataBudget } from '../src/storage/retainedRepository/metadataBudget.js';
import { createPipeline } from '../src/createPipeline.js';
import { DAY, fixture, request, scopedWrite } from './helpers/history-repository.js';
afterEach(() => vi.restoreAllMocks());

it('retires uncertain-but-deleted saves separately, so the next day is not stuck', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const failed = await scopedWrite(first.repository, 'failed-publication');
    const native = vi.spyOn(FilesystemRepository.prototype, 'commit').mockRejectedValue(Error('not public'));
    await expect(first.repository.commit(failed)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' }); native.mockRestore();
    f.at(DAY); await first.repository.sweep({ limit: 100 });
    expect(await first.repository.receipt(failed.operationId)).toMatchObject({ publication: 'unconfirmed', status: 'expired', dataDeleted: true });
    const next = f.open(1), saved = await scopedWrite(next.repository, 'following-day'); await next.repository.commit(saved);
    f.at(9 * DAY); await next.repository.sweep({ limit: 100 });
    for (let i = 0; i < 20; i++) await next.repository.retireHistory({ limit: 100 });
    expect(await next.repository.historyStatus()).toMatchObject({ retireBefore: 2, retiredOperations: 2, retiredUnconfirmedOperations: 1 });
    for (const id of [failed.operationId, saved.operationId]) await expect(next.repository.findOperation(id)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    next.repository.close();
    expect(await f.open(9).repository.historyStatus()).toMatchObject({ retiredUnconfirmedOperations: 1, retireBefore: 2 });
  } finally { await f.close(); }
});

it('verifies a day in bounded resumable chunks and advances the floor only after the final input', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    for (let i = 0; i < 12; i++) await first.repository.commit(await scopedWrite(first.repository, 'chunk-' + i));
    f.at(DAY); await first.repository.sweep({ limit: 100 }); f.at(8 * DAY);
    const result = await first.repository.retireHistory({ limit: 2 });
    expect(result).toMatchObject({ retireBefore: 0, verifiedOperations: 2, reclaimedOperations: 0, pending: true, blocked: false });
    expect(await first.repository.historyStatus()).toMatchObject({ generation: 0, retiredOperations: 0 });
    first.repository.close(); const resumed = f.open(8);
    let complete = false;
    for (let i = 0; i < 100; i++) {
      const batch = await resumed.repository.retireHistory({ limit: 2 }) as typeof result & { verifiedItems: number };
      expect(batch.verifiedItems).toBeLessThanOrEqual(34);
      expect(batch.verifiedOperations).toBeLessThanOrEqual(2);
      expect(batch.verifiedReferences).toBeLessThanOrEqual(32);
      expect(batch.reclaimedOperations).toBeLessThanOrEqual(2);
      if (batch.retireBefore === 1) { complete = true; break; }
    }
    expect(complete).toBe(true);
    expect(await resumed.repository.historyStatus()).toMatchObject({ retiredOperations: 12, retiredReferences: 36, retiredUnconfirmedOperations: 0 });
  } finally { await f.close(); }
});

it('checks control files only while owning the write transaction', async () => {
  const f = await fixture(), first = f.open(0), checks: boolean[] = [];
  const original = MetadataBudget.prototype.checkFiles;
  vi.spyOn(MetadataBudget.prototype, 'checkFiles').mockImplementation(function (this: MetadataBudget) {
    checks.push((this as unknown as { db: DatabaseSync }).db.isTransaction);
    return original.call(this);
  });
  try {
    await first.repository.commit(await scopedWrite(first.repository, 'locked-snapshot'));
    await first.repository.sweep({ limit: 100 });
    expect(checks.length).toBeGreaterThan(2); expect(checks.every(Boolean)).toBe(true);
  } finally { await f.close(); }
});

it('uses transactional live counters without scanning the ownership tables on each save', async () => {
  const f = await fixture(), first = f.open(0), other = f.open(0), scans: string[] = [];
  const prepare = DatabaseSync.prototype.prepare;
  vi.spyOn(DatabaseSync.prototype, 'prepare').mockImplementation(function (this: DatabaseSync, sql: string) {
    if (/count\(\*\).*from (?:operations|refs)\b/i.test(sql)) scans.push(sql);
    return prepare.call(this, sql);
  });
  try {
    for (const [i, repo] of [first.repository, other.repository].entries()) await repo.commit(await scopedWrite(repo, 'counter-' + i));
    expect(await first.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 2, references: 6 });
    expect(scans).toEqual([]);
    f.at(8 * DAY); await other.repository.sweep({ limit: 100 });
    for (let i = 0; i < 20; i++) await first.repository.retireHistory({ limit: 100 });
    expect(await other.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
  } finally { await f.close(); }
});

it('binds new work to the next day without reopening the store or reminting an old retry', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const saved = await first.pipeline.collect(request('stable-old'));
    f.at(DAY);
    const bind = first.repository.bindPeriod.bind(first.repository);
    const scoped = await bind(1);
    const logicalId = saved.commit.operationId.slice(-64);
    expect(scoped.identity!.operationId(logicalId)).not.toBe(saved.commit.operationId);
    const oldRetry = await first.pipeline.collect(request('stable-old'));
    expect(oldRetry.commit.operationId).toBe(saved.commit.operationId);
    const pipeline = createPipeline({ repository: scoped,
      sources: [{ ref: request('').sourceRef, adapterRef: { id: 'same-store', version: '1' } }],
      adapters: [{ ref: { id: 'same-store', version: '1' }, async collect() { return { key: 'today', observations: [], outcome: 'partial' as const,
        coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } } }; } }] });
    const today = await pipeline.collect(request('new-slot'));
    expect(today.commit.status).toBe('committed'); expect(today.commit.operationId).toContain(':1:o:');
    await expect(bind(0)).rejects.toMatchObject({ code: 'REPOSITORY_PERIOD_CLOSED' });
    await expect(bind(2)).rejects.toMatchObject({ code: 'REPOSITORY_PERIOD_CLOSED' });
  } finally { await f.close(); }
});

it('limits reference verification and reclamation even when a single operation is wide', async () => {
  const f = await fixture(), first = f.open(0, { maxOperationBytes: 262144 });
  try {
    const seed = await scopedWrite(first.repository, 'wide');
    const observations = Array.from({ length: 60 }, (_, i) => ({ ...seed.observations[0]!, key: 'wide-' + i,
      ref: first.repository.identity!.reference(seed.operationId, 'observation', 'wide-' + i) }));
    const write = { ...seed, observations, batch: { ...seed.batch, observationRefs: observations.map(row => row.ref) },
      evidence: { ...seed.evidence, outputRefs: [seed.batch.ref, ...observations.map(row => row.ref)] } };
    await first.repository.commit(write); f.at(DAY); await first.repository.sweep({ limit: 100 }); f.at(8 * DAY);
    let passes = 0;
    for (;;) {
      const result = await first.repository.retireHistory({ limit: 1 }); passes++;
      expect(result.verifiedOperations + result.reclaimedOperations).toBeLessThanOrEqual(1);
      expect(result.verifiedReferences + result.reclaimedReferences).toBeLessThanOrEqual(16);
      if (passes === 1) expect(result.retireBefore).toBe(0);
      if (!result.pending) break;
      expect(passes).toBeLessThan(30);
    }
    expect(passes).toBeGreaterThan(4);
    expect(await first.repository.historyStatus()).toMatchObject({ retiredOperations: 1, retiredReferences: 62 });
    expect(await first.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
  } finally { await f.close(); }
});

it('does not turn partial verification into a floor when one later file is still present', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) { const write = await scopedWrite(first.repository, 'incomplete-' + i); ids.push(write.operationId); await first.repository.commit(write); }
    f.at(DAY); await first.repository.sweep({ limit: 9 }); f.at(8 * DAY);
    let blocked = false;
    for (let i = 0; i < 20; i++) { const result = await first.repository.retireHistory({ limit: 2 }); if (result.blocked) { blocked = true; break; } }
    expect(blocked).toBe(true); expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 0, retiredOperations: 0 });
    const receipts = await Promise.all(ids.map(id => first.repository.receipt(id)));
    expect(receipts.filter(receipt => !receipt?.dataDeleted)).toHaveLength(1);
    await first.repository.sweep({ limit: 100 });
    for (let i = 0; i < 20; i++) await first.repository.retireHistory({ limit: 2 });
    expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 1, retiredOperations: 10, retiredReferences: 30 });
  } finally { await f.close(); }
});

it.each(['history_scan', 'history_counts'])('refuses a lost %s authority instead of rebuilding it with empty counters', async table => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.pipeline.collect(request('saved')); first.repository.close();
    const db = new DatabaseSync(join(f.directory, 'retention-v4.sqlite'));
    try { db.exec(`DROP TABLE ${table}`); } finally { db.close(); }
    await expect(f.open(0).repository.historyStatus()).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    const inspect = new DatabaseSync(join(f.directory, 'retention-v4.sqlite'));
    try { expect(inspect.prepare("SELECT 1 FROM sqlite_master WHERE name=?").get(table)).toBeUndefined(); } finally { inspect.close(); }
  } finally { await f.close(); }
});

it('refuses a lost counter trigger, and database foreign keys forbid orphan memberships', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.pipeline.collect(request('saved'));
    const original = DatabaseSync.prototype.prepare;
    const probe = vi.spyOn(DatabaseSync.prototype, 'prepare').mockImplementation(function (this: DatabaseSync, sql: string) {
      if (sql === 'SELECT operations,refs FROM history_counts WHERE id=1') {
        expect(() => original.call(this, 'INSERT INTO refs VALUES(?,?)').run('f'.repeat(64), 'f'.repeat(64))).toThrow();
      }
      return original.call(this, sql);
    });
    expect(await first.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 1, references: 3 }); probe.mockRestore();
    first.repository.close(); const db = new DatabaseSync(join(f.directory, 'retention-v4.sqlite'));
    try { db.exec('DROP TRIGGER history_ref_delete'); } finally { db.close(); }
    await expect(f.open(0).repository.historyStatus()).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});

it('uses covering owner/period indexes rather than scanning all reference memberships', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.pipeline.collect(request('query-plan'));
    const db = new DatabaseSync(join(f.directory, 'retention-v4.sqlite'), { readOnly: true });
    try {
      const refs = db.prepare('EXPLAIN QUERY PLAN SELECT * FROM refs WHERE operationHash=? AND refHash>? ORDER BY refHash LIMIT ?').all('a'.repeat(64), '', 16);
      expect(refs.map(row => row.detail).join(' ')).toMatch(/SEARCH refs USING COVERING INDEX reference_owners/);
      const periods = db.prepare('EXPLAIN QUERY PLAN SELECT operationHash FROM operations WHERE period=? AND operationHash>? ORDER BY operationHash LIMIT 1').all(0, '');
      expect(periods.map(row => row.detail).join(' ')).toMatch(/SEARCH operations USING COVERING INDEX period_members/);
    } finally { db.close(); }
  } finally { await f.close(); }
});

it('refuses the earlier unpublished v3 format without creating a replacement ledger', async () => {
  const f = await fixture();
  const { readFile, access } = await import('node:fs/promises');
  try {
    const fence = new DatabaseSync(join(f.directory, 'retention.sqlite'));
    fence.exec('CREATE TABLE policy(id INTEGER PRIMARY KEY,version INTEGER,retentionMs INTEGER)');
    fence.prepare('INSERT INTO policy VALUES(1,3,?)').run(DAY); fence.close();
    const old = new DatabaseSync(join(f.directory, 'retention-v3.sqlite'));
    old.exec('CREATE TABLE original(value TEXT); INSERT INTO original VALUES(\'kept\')'); old.close();
    const before = await readFile(join(f.directory, 'retention-v3.sqlite'));
    await expect(f.open(0).repository.historyStatus()).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
    expect(await readFile(join(f.directory, 'retention-v3.sqlite'))).toEqual(before);
    await expect(access(join(f.directory, 'retention-v4.sqlite'))).rejects.toMatchObject({ code: 'ENOENT' });
  } finally { await f.close(); }
});

it('lets an already admitted intent finish after rollover while keeping its original ID', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.repository.historyStatus(); f.at(DAY - 1);
    const write = await scopedWrite(first.repository, 'admitted-before-boundary');
    const native = vi.spyOn(FilesystemRepository.prototype, 'commit').mockRejectedValue(Error('uncertain publication'));
    await expect(first.repository.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' }); native.mockRestore();
    f.at(DAY);
    expect(await first.repository.commit(write)).toMatchObject({ status: 'committed', operationId: write.operationId });
    expect(await first.repository.receipt(write.operationId)).toMatchObject({ publication: 'committed', status: 'stored' });
    const scope = await first.repository.bindPeriod(1);
    expect(scope.identity!.operationId(write.operationId.slice(-64))).not.toBe(write.operationId);
  } finally { await f.close(); }
});
