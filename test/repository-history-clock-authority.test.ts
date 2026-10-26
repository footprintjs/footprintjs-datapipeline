import { expect, it } from 'vitest';
import { createPipeline } from '../src/createPipeline.js';
import type { Repository } from '../src/ports.js';
import { DAY, fixture, request, scopedWrite } from './helpers/history-repository.js';

function collecting(repository: Repository, acquire: () => void) {
  return createPipeline({ repository,
    sources: [{ ref: request('').sourceRef, adapterRef: { id: 'clock-boundary', version: '1' } }],
    adapters: [{ ref: { id: 'clock-boundary', version: '1' }, async collect() {
      acquire();
      return { key: 'boundary-input', observations: [], outcome: 'partial' as const,
        coverage: { status: 'unknown' as const, time: { kind: 'source-time-unknown' as const } } };
    } }], clock: () => '2026-10-26T12:00:00.000Z' });
}

it('cannot charge an old handle\'s idle interval to newly accepted payloads or their permanent history floor', async () => {
  const f = await fixture(12), older = f.open(0, { keepDays: 1 });
  try {
    const initial = await older.repository.historyStatus();
    expect(initial).toMatchObject({ currentPeriod: 0, retireBefore: 0, retiredOperations: 0 });
    const acceptedAt = 2 * DAY + 1;
    f.at(acceptedAt);
    // Both clocks measure the same ideal elapsed time, with independent origins.
    const sharedClock = f.options.monotonicClock;
    f.options.monotonicClock = () => sharedClock() + 4 * DAY;
    const newer = f.open(0, { keepDays: 1 });
    expect(await newer.repository.historyStatus()).toEqual(initial);
    const write = await scopedWrite(newer.repository, 'new-after-old-idle');
    const committed = await newer.repository.commit(write);
    expect(committed.status).toBe('committed');
    const receipt = await newer.repository.receipt(write.operationId);
    expect(receipt).toMatchObject({ status: 'stored', publication: 'committed', dataDeleted: false,
      acceptedAgeMs: 0, expiresAgeMs: DAY });
    const operation = await newer.repository.findOperation(write.operationId);

    // No time passes after acceptance. Neither payload deletion nor irreversible
    // retirement may use the older handle's previously unpublished two days.
    expect(await older.repository.sweep({ limit: 10 })).toEqual({ deleted: 0 });
    expect(await older.repository.historyStatus()).toEqual(initial);
    expect(await newer.repository.receipt(write.operationId)).toEqual(receipt);
    expect(await newer.repository.findOperation(write.operationId)).toEqual(operation);
    expect(await older.repository.read(write.batch.ref)).toEqual(write.batch);

    f.at(acceptedAt + DAY - 1);
    expect(await older.repository.sweep({ limit: 10 })).toEqual({ deleted: 0 });
    expect(await newer.repository.receipt(write.operationId)).toEqual(receipt);
    f.at(acceptedAt + DAY);
    expect(await older.repository.sweep({ limit: 10 })).toEqual({ deleted: 1 });
    expect(await newer.repository.receipt(write.operationId)).toEqual({ ...receipt, status: 'expired', dataDeleted: true });
    expect(await older.repository.historyStatus()).toMatchObject({ currentPeriod: 1, retireBefore: 0, retiredOperations: 0 });

    f.at(acceptedAt + 2 * DAY - 1);
    expect(await older.repository.retireHistory({ limit: 10 })).toMatchObject({ retireBefore: 0 });
    expect(await newer.repository.receipt(write.operationId)).toMatchObject({ dataDeleted: true });
    f.at(acceptedAt + 2 * DAY);
    expect(await older.repository.sweep({ limit: 10 })).toEqual({ deleted: 0 });
    expect(await older.repository.historyStatus()).toMatchObject({ currentPeriod: 2, retireBefore: 1, retiredOperations: 1 });
    await expect(newer.repository.receipt(write.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
    expect(await older.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
  } finally { await f.close(); }
});

it('keeps verified age progressing when one publisher alternates admissions with another handle\'s maintenance', async () => {
  const f = await fixture(12), publisher = f.open(0, { keepDays: 1 }), worker = f.open(0, { keepDays: 1 });
  try {
    const original = await scopedWrite(publisher.repository, 'start');
    expect(await publisher.repository.commit(original)).toMatchObject({ status: 'committed' });
    await worker.repository.historyStatus();
    for (let quarter = 1; quarter <= 3; quarter++) {
      f.at(quarter * DAY / 4);
      expect(await publisher.repository.historyStatus()).toMatchObject({ currentPeriod: 0 });
      expect(await worker.repository.sweep({ limit: 10 })).toEqual({ deleted: 0 });
      const write = await scopedWrite(publisher.repository, `quarter-${quarter}`);
      expect(await publisher.repository.commit(write)).toMatchObject({ status: 'committed' });
      expect(await publisher.repository.receipt(write.operationId)).toMatchObject({ acceptedAgeMs: quarter * DAY / 4 });
    }
    f.at(DAY);
    expect(await publisher.repository.historyStatus()).toMatchObject({ currentPeriod: 1 });
    expect(await worker.repository.sweep({ limit: 10 })).toEqual({ deleted: 1 });
    expect(await publisher.repository.receipt(original.operationId)).toMatchObject({ dataDeleted: true });
    f.at(2 * DAY);
    expect(await publisher.repository.historyStatus()).toMatchObject({ currentPeriod: 2 });
    expect(await worker.repository.sweep({ limit: 10 })).toEqual({ deleted: 3 });
    expect(await worker.repository.historyStatus()).toMatchObject({ retireBefore: 1, retiredOperations: 4 });
    await expect(publisher.repository.receipt(original.operationId)).rejects.toMatchObject({ code: 'REPOSITORY_HISTORY_UNAVAILABLE' });
  } finally { await f.close(); }
});

it('refuses the rounded closing fraction without a partial intent or early admission through a prepared next binding', async () => {
  const f = await fixture(12); f.options.retentionMs = 100;
  const keeper = f.open(0, { keepDays: 1 }), publisher = f.open(0, { keepDays: 1 });
  let calls = 0;
  try {
    // This independent observer preserves the real boundary across refused
    // transactions, which are allowed to discard the publisher's local interval.
    await keeper.repository.historyStatus(); await publisher.repository.historyStatus();
    f.at(DAY - 0.1);
    const current = collecting(await publisher.repository.bindPeriod(0), () => { calls++; });
    const next = collecting(await publisher.repository.bindPeriod(1), () => { calls++; });
    const closing = await current.collect(request('closing-fraction'));
    expect(closing.commit.status).toBe('failed');
    expect(calls).toBe(1);
    expect(await publisher.repository.receipt(closing.commit.operationId)).toBeUndefined();
    expect(await publisher.repository.metadataCapacity({ operations: 0, references: 0 })).toMatchObject({ operations: 0, references: 0 });
    const early = await next.collect(request('next-boundary'));
    expect(early.commit.status).toBe('failed'); expect(calls).toBe(1);
    expect(await keeper.repository.historyStatus()).toMatchObject({ currentPeriod: 0, retireBefore: 0 });

    f.at(DAY);
    expect(await keeper.repository.historyStatus()).toMatchObject({ currentPeriod: 1 });
    const accepted = await next.collect(request('next-boundary'));
    expect(accepted.commit).toMatchObject({ status: 'committed', operationId: early.commit.operationId });
    expect(calls).toBe(2);
    expect(await publisher.repository.receipt(accepted.commit.operationId)).toMatchObject({ acceptedAgeMs: DAY, expiresAgeMs: DAY + 100 });
    f.at(DAY + 99.9);
    expect(await publisher.repository.sweep({ limit: 10 })).toEqual({ deleted: 0 });
    f.at(DAY + 100);
    expect(await publisher.repository.sweep({ limit: 10 })).toEqual({ deleted: 1 });
  } finally { await f.close(); }
});
