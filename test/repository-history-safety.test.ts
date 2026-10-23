import { access, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { createRetainedFilesystemRepository, FilesystemRepository } from '../src/storage/filesystem.js';
import { RepositoryLedger } from '../src/storage/retainedRepository/ledger.js';
import { RepositoryLedger as LegacyLedger } from './fixtures/retained-reader-0.9/ledger.js';
import { DatabaseSync } from 'node:sqlite';
import { DAY, namespaceId, fixture, request, scopedWrite } from './helpers/history-repository.js';

afterEach(() => { vi.restoreAllMocks(); });
it.each(['retired_history', 'operations', 'elapsed_clock', 'policy'])('refuses missing active authority table %s rather than recreating empty facts', async table => {
  const f = await fixture(), first = f.open(0);
  try {
    const saved = await first.pipeline.collect(request('saved'));
    f.at(8 * DAY); await first.repository.sweep({ limit: 100 }); first.repository.close();
    const db = new DatabaseSync(join(f.directory, 'retention-v3.sqlite'));
    try { db.exec(`DROP TABLE ${table}`); } finally { db.close(); }
    const reopened = f.open(8);
    await expect(reopened.repository.findOperation(saved.commit.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    const inspect = new DatabaseSync(join(f.directory, 'retention-v3.sqlite'));
    try { expect(inspect.prepare('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?').get(table)).toBeUndefined(); }
    finally { inspect.close(); }
  } finally { await f.close(); }
});
it('blocks known and invented retired IDs, direct commits, and relabelled old references', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const old = await scopedWrite(first.repository, 'old'); await first.repository.commit(old);
    f.at(8 * DAY); await first.repository.sweep({ limit: 100 });
    const next = f.open(8), invented = old.operationId.slice(0, -64) + 'f'.repeat(64);
    for (const id of [old.operationId, invented]) {
      await expect(next.repository.findOperation(id)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
      await expect(next.repository.receipt(id)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    }
    await expect(next.repository.read({ ...old.observations[0]!.ref, id: old.observations[0]!.ref.id.slice(0, -64) + 'f'.repeat(64) }))
      .rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    await expect(next.repository.commit(old)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    const fresh = await scopedWrite(next.repository, 'fresh'), oldRef = old.observations[0]!.ref;
    const forged = { ...fresh, batch: { ...fresh.batch, observationRefs: [oldRef] },
      observations: [{ ...fresh.observations[0]!, ref: oldRef }],
      evidence: { ...fresh.evidence, outputRefs: [fresh.batch.ref, oldRef] } };
    await expect(next.repository.commit(forged)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' });
    expect(await next.repository.receipt(fresh.operationId)).toBeUndefined();
    expect(await next.repository.commit(fresh)).toMatchObject({ status: 'committed' });
  } finally { await f.close(); }
});

it('keeps the permanent floor while reclaiming exact rows in bounded batches and reopening handles', async () => {
  const f = await fixture(), first = f.open(0); const ids: string[] = [];
  try {
    for (let i = 0; i < 6; i++) { const write = await scopedWrite(first.repository, 'row-' + i); await first.repository.commit(write); ids.push(write.operationId); }
    f.at(DAY); await first.repository.sweep({ limit: 100 });
    f.at(8 * DAY);
    expect(await first.repository.retireHistory({ limit: 1 })).toEqual({ retireBefore: 1, reclaimedOperations: 1, blocked: false });
    expect(await first.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 5, references: 15 });
    first.repository.close();
    const reopened = f.open(8);
    for (const id of ids) await expect(reopened.repository.findOperation(id)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    for (let i = 0; i < 5; i++) expect((await reopened.repository.retireHistory({ limit: 1 })).reclaimedOperations).toBe(1);
    expect(await reopened.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
    expect(await reopened.repository.historyStatus()).toMatchObject({ retireBefore: 1, generation: 1, retiredOperations: 6, retiredReferences: 18 });
  } finally { await f.close(); }
});

it('requires physical deletion proof before retiring a period', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const write = await scopedWrite(first.repository, 'saved'); await first.repository.commit(write);
    f.at(8 * DAY);
    await expect(first.repository.readMany([write.batch.ref])).rejects.toMatchObject({ code: 'REPOSITORY_EXPIRED' });
    expect(await first.repository.retireHistory({ limit: 100 })).toMatchObject({ retireBefore: 0, blocked: true });
    expect(await first.repository.receipt(write.operationId)).toMatchObject({ status: 'expired', publication: 'committed', dataDeleted: false });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_HISTORY_BLOCKED' });
    await first.repository.sweep({ limit: 100 });
    expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 1 });
  } finally { await f.close(); }
});

it('never converts uncertain publication into deletion proof to meet the history deadline', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const write = await scopedWrite(first.repository, 'uncertain');
    const native = vi.spyOn(FilesystemRepository.prototype, 'commit').mockRejectedValue(Error('private failure'));
    await expect(first.repository.commit(write)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' }); native.mockRestore();
    f.at(8 * DAY); await first.repository.sweep({ limit: 100 });
    expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 0 });
    expect(await first.repository.receipt(write.operationId)).toMatchObject({ status: 'expired', publication: 'unconfirmed', dataDeleted: true });
    expect(f.alerts).toContainEqual({ code: 'REPOSITORY_HISTORY_BLOCKED' });
  } finally { await f.close(); }
});

it('a wall-clock jump cannot retire a period and an elapsed-clock reversal is refused', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const saved = await first.pipeline.collect(request('time'));
    f.wall(1_790_000_000_000 + 1000 * DAY); f.at(DAY - 1);
    await first.repository.sweep({ limit: 100 });
    expect(await first.repository.read(saved.commit.observationRefs![0]!)).toMatchObject({ payload: { latency: 0 } });
    expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 0 });
    f.at(0);
    await expect(first.repository.retireHistory({ limit: 100 })).rejects.toMatchObject({ code: 'REPOSITORY_CLOCK_UNAVAILABLE' });
    f.at(DAY - 1); expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 0 });
  } finally { await f.close(); }
});

it('fences legacy and non-retiring readers, and refuses a lost active ledger or fence', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const saved = await first.pipeline.collect(request('format')); first.repository.close();
    expect(() => new LegacyLedger(f.directory, DAY)).toThrow();
    expect(() => new RepositoryLedger(f.directory, DAY, f.options)).toThrow(expect.objectContaining({ code: 'REPOSITORY_INVALID_OPTIONS' }));
    const foreign = createRetainedFilesystemRepository({ ...f.options, historyRetirement: { profile: 'daily-periods-v1', keepDays: 7, namespaceId: 'f'.repeat(32), period: 0, maxOperationBytes: 65536 } })!;
    try { await expect(foreign.findOperation(saved.commit.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); } finally { foreign.close(); }
    await unlink(join(f.directory, 'retention-v3.sqlite'));
    const reopened = f.open(0);
    await expect(reopened.repository.findOperation(saved.commit.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    await expect(access(join(f.directory, 'retention-v3.sqlite'))).rejects.toMatchObject({ code: 'ENOENT' });
  } finally { await f.close(); }
});

it.each([
  { profile: 'forever' }, { namespaceId: 'not-an-instance' }, { period: -1 }, { period: 0.5 },
  { maxOperationBytes: 0 }, { maxOperationBytes: 17 * 1024 * 1024 }, { extra: true },
  { keepDays: 0 }, { keepDays: 1.5 }, { keepDays: Infinity }, { keepDays: undefined },
])('refuses an unsafe history policy %j', async change => {
  const f = await fixture();
  try {
    expect(() => createRetainedFilesystemRepository({ ...f.options, historyRetirement: {
      profile: 'daily-periods-v1', keepDays: 7, namespaceId, period: 0, maxOperationBytes: 65536, ...change,
    } } as never)).toThrow(expect.objectContaining({ code: 'REPOSITORY_INVALID_OPTIONS' }));
  } finally { await f.close(); }
});

it('does not shorten a namespace history or change its data budget by reopening with different settings', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.pipeline.collect(request('immutable-policy'));
    for (const change of [{ keepDays: 6 }, { maxOperationBytes: 32768 }]) {
      const altered = createRetainedFilesystemRepository({ ...f.options, historyRetirement: {
        profile: 'daily-periods-v1', keepDays: 7, namespaceId, period: 0, maxOperationBytes: 65536, ...change,
      } })!;
      try { await expect(altered.historyStatus()).rejects.toMatchObject({ code: 'REPOSITORY_INVALID_OPTIONS' }); }
      finally { altered.close(); }
    }
    expect(await first.repository.historyStatus()).toMatchObject({ retireBefore: 0 });
  } finally { await f.close(); }
});

it('requires metadata caps, verified time, and payload retention no longer than kept history', async () => {
  const f = await fixture();
  try {
    const policy = { profile: 'daily-periods-v1' as const, keepDays: 7, namespaceId, period: 0, maxOperationBytes: 65536 };
    for (const change of [{ metadataLimits: undefined }, { expiryClock: undefined }, { retentionMs: 7 * DAY + 1 }]) {
      expect(() => createRetainedFilesystemRepository({ ...f.options, historyRetirement: policy, ...change } as never))
        .toThrow(expect.objectContaining({ code: 'REPOSITORY_INVALID_OPTIONS' }));
    }
    expect(createRetainedFilesystemRepository({ enabled: false, historyRetirement: policy } as never)).toBeUndefined();
  } finally { await f.close(); }
});

it('uses the required host duration rather than a hidden seven-day library default', async () => {
  const f = await fixture(), first = f.open(0, { keepDays: 2 });
  try {
    const saved = await first.pipeline.collect(request('host-duration'));
    f.at(3 * DAY - 1); await first.repository.sweep({ limit: 100 });
    expect(await first.repository.receipt(saved.commit.operationId)).toMatchObject({ dataDeleted: true });
    f.at(3 * DAY); await first.repository.sweep({ limit: 100 });
    await expect(first.repository.findOperation(saved.commit.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
  } finally { await f.close(); }
});
it('snapshots a mutable host history policy before any asynchronous filesystem work', async () => {
  const f = await fixture();
  const policy = { profile: 'daily-periods-v1' as const, keepDays: 7, namespaceId, period: 0, maxOperationBytes: 65536 };
  const repo = createRetainedFilesystemRepository({ ...f.options, historyRetirement: policy })!;
  try {
    policy.keepDays = 1;
    await repo.historyStatus(); f.at(3 * DAY); await repo.retireHistory({ limit: 1 });
    expect(await repo.historyStatus()).toMatchObject({ retireBefore: 0 });
  } finally { repo.close(); await f.close(); }
});
it('enforces the operation-content ceiling while leaving refused publication uncertain', async () => {
  const f = await fixture(), first = f.open(0, { maxOperationBytes: 1024 });
  try {
    const saved = await first.pipeline.collect(request('small-content-budget'));
    expect(saved.commit.status).not.toBe('committed');
    expect(await first.repository.receipt(saved.commit.operationId)).toMatchObject({ status: 'pending', publication: 'unconfirmed' });
  } finally { await f.close(); }
});

it.each(['policy', 'elapsed_clock', 'retired_history'])('refuses a missing active authority row in %s', async table => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.pipeline.collect(request('saved')); f.at(8 * DAY); await first.repository.sweep({ limit: 100 }); first.repository.close();
    const db = new DatabaseSync(join(f.directory, 'retention-v3.sqlite'));
    try { db.exec(`DELETE FROM ${table} WHERE id=1`); } finally { db.close(); }
    const reopened = f.open(8);
    await expect(reopened.repository.historyStatus()).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    const inspect = new DatabaseSync(join(f.directory, 'retention-v3.sqlite'));
    try { expect(inspect.prepare(`SELECT 1 FROM ${table} WHERE id=1`).get()).toBeUndefined(); } finally { inspect.close(); }
  } finally { await f.close(); }
});
it.each(['DROP TABLE history_namespace', 'DELETE FROM history_namespace WHERE id=1'])('does not recreate a missing namespace witness: %s', async command => {
  const f = await fixture(), first = f.open(0);
  try {
    await first.pipeline.collect(request('saved')); first.repository.close();
    const db = new DatabaseSync(join(f.directory, 'retention.sqlite'));
    try { db.exec(command); } finally { db.close(); }
    await expect(f.open(0).repository.historyStatus()).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
  } finally { await f.close(); }
});
