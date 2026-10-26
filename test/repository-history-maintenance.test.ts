import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect, it, vi } from 'vitest';
import { createPipeline } from '../src/createPipeline.js';
import type { Repository } from '../src/ports.js';
import { DAY, fixture, request, scopedWrite, drainHistory } from './helpers/history-repository.js';

afterEach(() => vi.restoreAllMocks());

function collecting(repository: Repository, acquire: () => void) {
  return createPipeline({ repository,
    sources: [{ ref: request('').sourceRef, adapterRef: { id: 'prepared-period', version: '1' } }],
    adapters: [{ ref: { id: 'prepared-period', version: '1' }, async collect() {
      acquire();
      return { key: 'period-input', observations: [], outcome: 'partial' as const,
        coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } } };
    } }], clock: () => '2026-10-26T12:00:00.000Z' });
}

const pendingAlerts = (alerts: unknown[]) => alerts.filter(event =>
  (event as { code?: string }).code === 'REPOSITORY_HISTORY_PENDING');
const historyAlerts = (alerts: unknown[]) => alerts.filter(event =>
  (event as { code?: string }).code?.startsWith('REPOSITORY_HISTORY_'));

async function saveThree(f: Awaited<ReturnType<typeof fixture>>, period: number, prefix: string) {
  const scope = f.open(period);
  for (let i = 0; i < 3; i++) await scope.repository.commit(await scopedWrite(scope.repository, `${prefix}-${i}`));
}

it('prepares the next verified period without dispatching early, reminting retries, or accepting late new work', async () => {
  const f = await fixture(); f.options.retentionMs = 3 * DAY;
  const original = f.open(0); let calls = 0;
  try {
    const saved = await original.pipeline.collect(request('old-retry'));
    const prepared = await original.repository.bindPeriod(1);
    const next = collecting(prepared, () => { calls++; });
    const oldId = original.repository.identity!.operationId('a'.repeat(64));
    expect(prepared.identity!.operationId('a'.repeat(64))).not.toBe(oldId);
    await expect(original.repository.bindPeriod(2)).rejects.toMatchObject({ code: 'REPOSITORY_PERIOD_CLOSED' });

    // Advancing calendar time never opens an elapsed-age period.
    f.wall(1_790_000_000_000 + 400 * DAY); f.at(DAY - 1);
    const early = await next.collect(request('prepared-slot'));
    expect(early.commit.status).toBe('failed');
    await expect(prepared.identity!.assertOperation(early.commit.operationId, 'admit')).rejects.toMatchObject({ code: 'REPOSITORY_PERIOD_CLOSED' });
    expect(calls).toBe(0);

    f.at(DAY);
    const accepted = await next.collect(request('prepared-slot'));
    expect(accepted.commit.status).toBe('committed');
    expect(accepted.commit.operationId).toBe(early.commit.operationId);
    expect(calls).toBe(1);
    const oldRetry = await original.pipeline.collect(request('old-retry'));
    expect(oldRetry.commit).toMatchObject({ status: 'committed', operationId: saved.commit.operationId });
    expect(f.calls()).toBe(1);
    expect(original.repository.identity!.operationId('a'.repeat(64))).toBe(oldId);

    f.at(2 * DAY);
    const late = await next.collect(request('late-first-attempt'));
    expect(late.commit.status).toBe('failed');
    await expect(prepared.identity!.assertOperation(late.commit.operationId, 'admit')).rejects.toMatchObject({ code: 'REPOSITORY_PERIOD_CLOSED' });
    const retry = await next.collect(request('prepared-slot'));
    expect(retry.commit).toMatchObject({ status: 'committed', operationId: accepted.commit.operationId });
    expect(calls).toBe(1);
    await expect(original.repository.bindPeriod(1)).rejects.toMatchObject({ code: 'REPOSITORY_PERIOD_CLOSED' });
  } finally { await f.close(); }
});

it('does not treat a prepared period as a reservation when acquisition crosses its closing boundary', async () => {
  const f = await fixture(), original = f.open(0); let calls = 0;
  try {
    await original.repository.historyStatus();
    const next = collecting(await original.repository.bindPeriod(1), () => { calls++; f.at(2 * DAY); });
    f.at(DAY);
    const result = await next.collect(request('boundary-crossing'));
    expect(calls).toBe(1);
    expect(result.commit.status).not.toBe('committed');
    expect(result.commit.operationId).toContain(':1:o:');
    expect(await original.repository.receipt(result.commit.operationId)).toBeUndefined();
    expect((await original.repository.metadataCapacity()).operations).toBe(0);
  } finally { await f.close(); }
});

it('reports pending work once per episode, resets after draining, and reports a later backlog', async () => {
  const f = await fixture(), maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus();
    await saveThree(f, 0, 'first'); f.at(DAY);
    await maintenance.repository.sweep({ limit: 100 });
    expect(historyAlerts(f.alerts)).toEqual([]);
    f.at(8 * DAY);
    for (let pass = 0; pass < 2; pass++) {
      expect(await maintenance.repository.retireHistory({ limit: 1 })).toMatchObject({ pending: true, blocked: false });
      expect(pendingAlerts(f.alerts)).toEqual([{ code: 'REPOSITORY_HISTORY_PENDING' }]);
    }
    expect(await drainHistory(maintenance.repository, 1)).toMatchObject({ pending: false, blocked: false });
    await maintenance.repository.retireHistory({ limit: 1 });
    expect(pendingAlerts(f.alerts)).toHaveLength(1);

    await saveThree(f, 8, 'second'); f.at(9 * DAY);
    await maintenance.repository.sweep({ limit: 100 }); f.at(16 * DAY);
    expect(await maintenance.repository.retireHistory({ limit: 1 })).toMatchObject({ pending: true, blocked: false });
    expect(pendingAlerts(f.alerts)).toHaveLength(2);
    expect(await drainHistory(maintenance.repository, 1)).toMatchObject({ pending: false, blocked: false });
    expect(await maintenance.repository.historyStatus()).toMatchObject({ retiredOperations: 6 });
  } finally { await f.close(); }
});

it('surfaces pending history through sweep even though its return value only reports deleted payloads', async () => {
  const f = await fixture(), maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus();
    await saveThree(f, 0, 'sweep'); f.at(DAY);
    await maintenance.repository.sweep({ limit: 100 }); f.at(8 * DAY);
    expect(await maintenance.repository.sweep({ limit: 1 })).toEqual({ deleted: 0 });
    expect(pendingAlerts(f.alerts)).toEqual([{ code: 'REPOSITORY_HISTORY_PENDING' }]);
  } finally { await f.close(); }
});

it.each(['sync', 'async'] as const)('keeps maintenance successful when its pending health callback throws %s', async mode => {
  const f = await fixture();
  f.options.onHealth = event => {
    f.alerts.push(event);
    if (mode === 'async') return Promise.reject(Error('private callback failure'));
    throw Error('private callback failure');
  };
  const maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus();
    await saveThree(f, 0, 'callback'); f.at(DAY);
    await maintenance.repository.sweep({ limit: 100 }); f.at(8 * DAY);
    expect(await maintenance.repository.retireHistory({ limit: 1 })).toMatchObject({ pending: true, blocked: false });
    expect(await drainHistory(maintenance.repository, 1)).toMatchObject({ pending: false, blocked: false });
    expect(pendingAlerts(f.alerts)).toEqual([{ code: 'REPOSITORY_HISTORY_PENDING' }]);
    expect(await maintenance.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
  } finally { await f.close(); }
});

it('reports newly unblocked pending work without treating the earlier blocked state as its notification', async () => {
  const f = await fixture(), maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus();
    await saveThree(f, 0, 'not-deleted'); f.at(8 * DAY);
    expect(await maintenance.repository.retireHistory({ limit: 1 })).toMatchObject({ pending: true, blocked: true });
    expect(historyAlerts(f.alerts)).toEqual([{ code: 'REPOSITORY_HISTORY_BLOCKED' }]);
    // Delete all three payloads, then spend the pass's three-operation budget
    // on verification. Reclamation still remains, so this has not drained.
    expect(await maintenance.repository.sweep({ limit: 3 })).toEqual({ deleted: 3 });
    expect(pendingAlerts(f.alerts)).toEqual([{ code: 'REPOSITORY_HISTORY_PENDING' }]);
    expect(await maintenance.repository.retireHistory({ limit: 1 })).toMatchObject({ pending: true, blocked: false });
    expect(pendingAlerts(f.alerts)).toHaveLength(1);
    expect(await drainHistory(maintenance.repository, 1)).toMatchObject({ pending: false, blocked: false });
  } finally { await f.close(); }
});

it.each(['pending', 'blocked'] as const)('does not announce %s retirement when final COMMIT fails', async outcome => {
  const f = await fixture(), maintenance = f.open(0);
  try {
    await maintenance.repository.historyStatus();
    await saveThree(f, 0, 'failed-commit');
    if (outcome === 'pending') { f.at(DAY); await maintenance.repository.sweep({ limit: 100 }); }
    f.at(8 * DAY);
    const exec = DatabaseSync.prototype.exec; let commits = 0;
    const failCommit = vi.spyOn(DatabaseSync.prototype, 'exec').mockImplementation(function (this: DatabaseSync, sql: string) {
      if (sql === 'COMMIT') { commits++; throw Object.assign(Error('synthetic commit I/O failure'), { errcode: 10 }); }
      return exec.call(this, sql);
    });
    await expect(maintenance.repository.retireHistory({ limit: 1 })).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(commits).toBe(1); expect(historyAlerts(f.alerts)).toEqual([]);
    failCommit.mockRestore();

    // No notification flag or cursor progress escapes the failed transaction.
    const retried = await maintenance.repository.retireHistory({ limit: 1 });
    expect(retried).toMatchObject({ pending: true, blocked: outcome === 'blocked', retireBefore: 0 });
    if (outcome === 'pending') expect(retried.verifiedOperations).toBe(1);
    expect(historyAlerts(f.alerts)).toEqual([{ code: outcome === 'pending' ? 'REPOSITORY_HISTORY_PENDING' : 'REPOSITORY_HISTORY_BLOCKED' }]);
  } finally { await f.close(); }
});
