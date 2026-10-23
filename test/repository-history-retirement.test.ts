import { expect, it } from 'vitest';
import { DAY, fixture, request, bytes, scopedWrite } from './helpers/history-repository.js';

it('keeps original IDs refused after exact membership is retired and the store restarts', async () => {
  const f = await fixture(), original = f.open(0);
  try {
    const result = await original.pipeline.collect(request('one'));
    expect(result.commit.status).toBe('committed');
    const id = result.commit.operationId, ref = result.commit.observationRefs![0]!;
    expect(id).toMatch(/^fp3:/);
    expect(await original.repository.read(ref)).toMatchObject({ payload: { latency: 0 } });
    f.at(DAY); await original.repository.sweep({ limit: 100 });
    expect(await original.repository.receipt(id)).toMatchObject({ dataDeleted: true, publication: 'committed' });
    f.at(8 * DAY - 1); await original.repository.sweep({ limit: 100 });
    expect(await original.repository.receipt(id)).toMatchObject({ dataDeleted: true });
    f.at(8 * DAY); await original.repository.sweep({ limit: 100 });
    for (const store of [original, f.open(8)]) {
      await expect(store.repository.findOperation(id)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
      await expect(store.repository.read(ref)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
      await expect(store.repository.receipt(id)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    }
    const calls = f.calls();
    const refused = await original.pipeline.collect(request('one'));
    expect(refused.commit.status).toBe('failed'); expect(refused.commit.operationId).toBe(id);
    expect(f.calls()).toBe(calls);
    // Reusing a logical key in an explicitly new period is new work, with a new ID.
    const fresh = await f.open(8).pipeline.collect(request('one'));
    expect(fresh.commit.status).toBe('committed'); expect(fresh.commit.operationId).not.toBe(id);
  } finally { await f.close(); }
});

it('reuses capacity and reaches an allocated-byte plateau over four complete history cycles', async () => {
  const f = await fixture(10); const measured: number[] = [];
  const maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus();
    for (let day = 0; day < 32; day++) {
      f.at(day * DAY);
      // One long-lived host/cleanup clock observes the interval. Reopening a
      // process alone must not invent elapsed age for unknown downtime.
      await maintenance.repository.sweep({ limit: 100 });
      const current = f.open(day);
      for (let i = 0; i < 1; i++) {
        // This small test isolates repository/page reuse. The separate automated
        // measurement runs the full Footprint collection flow 256 times.
        const write = await scopedWrite(current.repository, `day-${day}-row-${i}`);
        const result = await current.repository.commit(write);
        expect(result.status, `day ${day}, row ${i}`).toBe('committed');
      }
      measured.push(await bytes(f.directory)); current.repository.close();
    }
    const late = measured.slice(16);
    expect(Math.max(...late) - Math.min(...late)).toBeLessThanOrEqual(65536);
    expect(Math.max(...late)).toBeLessThan(2 * 1024 * 1024);
    expect(f.alerts).toEqual([]);
  } finally { await f.close(); }
});

it('does not dispatch an old unopened period as new work or silently remint it', async () => {
  const f = await fixture(), previous = f.open(0);
  try {
    await previous.pipeline.collect(request('initialize'));
    f.at(DAY);
    const before = f.calls();
    const result = await previous.pipeline.collect(request('late-first-attempt'));
    expect(result.commit.status).toBe('failed'); expect(f.calls()).toBe(before);
    expect((await previous.pipeline.reconcileCollect(request('never-saved')))).toBeUndefined();
  } finally { await f.close(); }
});

it('keeps a late-period payload for its full seven-day lifetime before retiring its whole period', async () => {
  const f = await fixture(); f.options.retentionMs = 7 * DAY;
  const original = f.open(0);
  try {
    await original.repository.historyStatus(); f.at(DAY - 1);
    const result = await original.pipeline.collect(request('late-in-day'));
    f.at(8 * DAY - 2); await original.repository.sweep({ limit: 100 });
    expect(await original.repository.read(result.commit.observationRefs![0]!)).toMatchObject({ payload: { latency: 0 } });
    f.at(8 * DAY - 1); await original.repository.sweep({ limit: 100 });
    expect(await original.repository.receipt(result.commit.operationId)).toMatchObject({ dataDeleted: true });
    f.at(8 * DAY); await original.repository.sweep({ limit: 100 });
    await expect(original.repository.read(result.commit.observationRefs![0]!)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
  } finally { await f.close(); }
});
it('retires empty time ranges with one fixed checkpoint rather than a receipt for every day', async () => {
  const f = await fixture(), maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus(); f.at(200 * DAY);
    await maintenance.repository.retireHistory({ limit: 1 });
    expect(await maintenance.repository.historyStatus()).toMatchObject({ currentPeriod: 200, retireBefore: 193, generation: 1, retiredOperations: 0 });
    expect(await maintenance.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
    const result = await f.open(200).pipeline.collect(request('current')); expect(result.commit.status).toBe('committed');
  } finally { await f.close(); }
});
it('a retired input in a mixed read does not suppress an unrelated current value', async () => {
  const f = await fixture(), original = f.open(0);
  try {
    const old = await original.pipeline.collect(request('old')); f.at(8 * DAY); await original.repository.sweep({ limit: 100 });
    const current = f.open(8), fresh = await current.pipeline.collect(request('current'));
    const result = await Promise.allSettled([current.repository.read(old.commit.observationRefs![0]!), current.repository.read(fresh.commit.observationRefs![0]!)]);
    expect(result[0]).toMatchObject({ status: 'rejected', reason: { code: 'REPOSITORY_HISTORY_UNAVAILABLE' } });
    expect(result[1]).toMatchObject({ status: 'fulfilled', value: { payload: { latency: 0 } } });
  } finally { await f.close(); }
});
