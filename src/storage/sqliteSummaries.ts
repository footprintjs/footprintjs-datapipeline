import { closeSync, mkdirSync, openSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type { CollectionSchedule, PreparedPoll, PreparedQueryResult, PreparedSummary, SummaryStore } from '../prepared/contracts.js';
import {
  PreparedContractError, PREPARED_MAX_QUERY_POLLS, PREPARED_MAX_SUMMARIES,
  preparedDigest, scheduleFingerprint, slotMilliseconds,
  validatePoll, validatePollMetadata, validatePreparedRange, validatePreparedSummary, validateSchedule,
} from '../prepared/validation.js';
import { canonical } from '../validation.js';

export interface SqliteSummaryOptions {
  /** Dedicated local SQLite file. The backend creates its parent directories. */
  readonly filename: string;
}
export class SummaryStoreError extends Error {
  constructor(readonly code: 'schedule_conflict' | 'poll_conflict' | 'storage_corrupt' | 'storage_failure' | 'store_closed', message: string) {
    super(message); this.name = 'SummaryStoreError';
  }
}

interface StoredRow { body: string; sha256: string }
interface PollRow extends StoredRow { slot_ms: number }
interface SummaryRow extends StoredRow { id: string; slot_ms: number; collected_ms: number }
interface StoredPoll { poll: Omit<PreparedPoll, 'summaries'>; summaryIds: string[] }
const MAX_QUERY_BYTES = 8 * 1024 * 1024;
const corrupt = (): never => { throw new SummaryStoreError('storage_corrupt', 'Stored prepared data failed verification.'); };
const conflict = (): never => { throw new SummaryStoreError('schedule_conflict', 'The registered collection schedule differs from this request.'); };
const scope = (schedule: CollectionSchedule): string => preparedDigest([schedule.namespace, schedule.ref]);
const clone = <T>(value: T): T => JSON.parse(canonical(value)) as T;

/**
 * Indexed, local prepared data. It deliberately has no Repository/read port:
 * answering a time-range query cannot load source response bodies.
 */
export function createSqliteSummaryStore(options: SqliteSummaryOptions): SummaryStore {
  let database: DatabaseSync;
  let opened: DatabaseSync | undefined;
  try {
    if (!options || typeof options.filename !== 'string' || !options.filename.trim() || options.filename === ':memory:') throw new PreparedContractError();
    const filename = resolve(options.filename);
    mkdirSync(dirname(filename), { recursive: true, mode: 0o700 });
    closeSync(openSync(filename, 'a', 0o600));
    database = opened = new DatabaseSync(filename, { enableForeignKeyConstraints: true, enableDoubleQuotedStringLiterals: false });
    database.exec(`
      PRAGMA busy_timeout = 5000;
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      PRAGMA trusted_schema = OFF;
      CREATE TABLE IF NOT EXISTS prepared_schedules (
        scope_key TEXT PRIMARY KEY, body TEXT NOT NULL, sha256 TEXT NOT NULL
      ) STRICT;
      CREATE TABLE IF NOT EXISTS prepared_polls (
        scope_key TEXT NOT NULL, slot_ms INTEGER NOT NULL,
        body TEXT NOT NULL, sha256 TEXT NOT NULL,
        PRIMARY KEY (scope_key, slot_ms),
        FOREIGN KEY (scope_key) REFERENCES prepared_schedules(scope_key)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS prepared_summaries (
        scope_key TEXT NOT NULL, id TEXT NOT NULL, slot_ms INTEGER NOT NULL,
        collected_ms INTEGER NOT NULL, body TEXT NOT NULL, sha256 TEXT NOT NULL,
        PRIMARY KEY (scope_key, id),
        FOREIGN KEY (scope_key, slot_ms) REFERENCES prepared_polls(scope_key, slot_ms)
      ) STRICT;
      CREATE INDEX IF NOT EXISTS prepared_summaries_time ON prepared_summaries (scope_key, collected_ms, id);
      CREATE INDEX IF NOT EXISTS prepared_summaries_slot ON prepared_summaries (scope_key, slot_ms, id);
    `);
  } catch (error) {
    try { opened?.close(); } catch { /* Opening errors remain public and bounded. */ }
    if (error instanceof PreparedContractError) throw error;
    throw new SummaryStoreError('storage_failure', 'Prepared storage could not be opened.');
  }
  let closed = false;
  function guard<T>(operation: () => T): T {
    if (closed) throw new SummaryStoreError('store_closed', 'Prepared storage is closed.');
    try { return operation(); }
    catch (error) {
      if (error instanceof SummaryStoreError || error instanceof PreparedContractError) throw error;
      throw new SummaryStoreError('storage_failure', 'Prepared storage operation failed.');
    }
  }
  function transaction<T>(write: boolean, operation: () => T): T {
    return guard(() => {
      database.exec(write ? 'BEGIN IMMEDIATE' : 'BEGIN');
      try { const result = operation(); database.exec('COMMIT'); return result; }
      catch (error) { try { database.exec('ROLLBACK'); } catch { /* Preserve the original safe error. */ } throw error; }
    });
  }
  function decoded<T>(row: StoredRow): T {
    try {
      const value: unknown = JSON.parse(row.body);
      if (preparedDigest(value) !== row.sha256) return corrupt();
      return value as T;
    } catch { return corrupt(); }
  }
  function registered(schedule: CollectionSchedule): string {
    const key = scope(schedule);
    const row = database.prepare('SELECT body, sha256 FROM prepared_schedules WHERE scope_key = ?').get(key) as StoredRow | undefined;
    if (!row) return conflict();
    try {
      const stored = validateSchedule(decoded(row));
      if (scope(stored) !== key) return corrupt();
      if (scheduleFingerprint(stored) !== scheduleFingerprint(schedule)) return conflict();
    } catch (error) {
      if (error instanceof SummaryStoreError) throw error;
      return corrupt();
    }
    return key;
  }
  function pollHead(schedule: CollectionSchedule, row: PollRow): StoredPoll {
    try {
      const stored = decoded<StoredPoll>(row);
      if (!stored || Object.keys(stored).sort().join(',') !== 'poll,summaryIds' || !Array.isArray(stored.summaryIds) || stored.summaryIds.length > PREPARED_MAX_SUMMARIES ||
          !stored.summaryIds.every(id => typeof id === 'string' && /^[a-f0-9]{64}$/.test(id)) || new Set(stored.summaryIds).size !== stored.summaryIds.length) return corrupt();
      const poll = validatePollMetadata(schedule, stored.poll);
      if (Date.parse(poll.slotAt) !== row.slot_ms || (poll.status === 'collected' ? stored.summaryIds.length === 0 : stored.summaryIds.length > 0)) return corrupt();
      return { poll, summaryIds: stored.summaryIds };
    } catch { return corrupt(); }
  }
  function summary(schedule: CollectionSchedule, row: SummaryRow): PreparedSummary {
    try {
      const value = validatePreparedSummary(schedule, decoded(row));
      if (value.id !== row.id || Date.parse(value.collectedAt) !== row.collected_ms || Date.parse(value.slotAt) !== row.slot_ms) return corrupt();
      return value;
    } catch { return corrupt(); }
  }
  function loadPoll(schedule: CollectionSchedule, key: string, slotMs: number): PreparedPoll | undefined {
    const row = database.prepare('SELECT slot_ms, body, sha256 FROM prepared_polls WHERE scope_key = ? AND slot_ms = ?').get(key, slotMs) as PollRow | undefined;
    if (!row) return undefined;
    const stored = pollHead(schedule, row);
    const rows = database.prepare('SELECT id, slot_ms, collected_ms, body, sha256 FROM prepared_summaries WHERE scope_key = ? AND slot_ms = ? ORDER BY id').all(key, slotMs) as unknown as SummaryRow[];
    if (rows.length !== stored.summaryIds.length) return corrupt();
    const byId = new Map(rows.map(row => [row.id, summary(schedule, row)]));
    try {
      const summaries = stored.summaryIds.map(id => { const value = byId.get(id); if (!value) return corrupt(); return value; });
      return validatePoll(schedule, { ...stored.poll, summaries });
    } catch { return corrupt(); }
  }
  return {
    async register(value) {
      const schedule = validateSchedule(value); const key = scope(schedule); const body = canonical(schedule); const hash = scheduleFingerprint(schedule);
      transaction(true, () => {
        const row = database.prepare('SELECT body, sha256 FROM prepared_schedules WHERE scope_key = ?').get(key) as StoredRow | undefined;
        if (row) { registered(schedule); return; }
        database.prepare('INSERT INTO prepared_schedules (scope_key, body, sha256) VALUES (?, ?, ?)').run(key, body, hash);
      });
    },
    async readPoll(value, slotAt) {
      const schedule = validateSchedule(value); const slotMs = slotMilliseconds(schedule, slotAt);
      return transaction(false, () => loadPoll(schedule, registered(schedule), slotMs));
    },
    async publish(value, candidate) {
      const schedule = validateSchedule(value); const poll = validatePoll(schedule, candidate); const slotMs = Date.parse(poll.slotAt);
      const { summaries, ...metadata } = poll;
      const stored: StoredPoll = { poll: metadata, summaryIds: summaries.map(item => item.id) };
      transaction(true, () => {
        const key = registered(schedule); const previous = loadPoll(schedule, key, slotMs);
        if (previous) {
          if (preparedDigest(previous) !== preparedDigest(poll)) throw new SummaryStoreError('poll_conflict', 'The scheduled poll already has a different immutable result.');
          return;
        }
        database.prepare('INSERT INTO prepared_polls (scope_key, slot_ms, body, sha256) VALUES (?, ?, ?, ?)').run(key, slotMs, canonical(stored), preparedDigest(stored));
        const insert = database.prepare('INSERT INTO prepared_summaries (scope_key, id, slot_ms, collected_ms, body, sha256) VALUES (?, ?, ?, ?, ?, ?)');
        for (const item of summaries) insert.run(key, item.id, slotMs, Date.parse(item.collectedAt), canonical(item), preparedDigest(item));
      });
    },
    async latestSlot(value) {
      const schedule = validateSchedule(value);
      return transaction(false, () => {
        const key = registered(schedule);
        const row = database.prepare('SELECT slot_ms, body, sha256 FROM prepared_polls WHERE scope_key = ? ORDER BY slot_ms DESC LIMIT 1').get(key) as PollRow | undefined;
        return row ? pollHead(schedule, row).poll.slotAt : undefined;
      });
    },
    async query(request) {
      const schedule = validateSchedule(request.schedule);
      const { startMs, endMs, limit, expectedPolls } = validatePreparedRange(schedule, request.start, request.end, request.limit);
      return transaction(false, () => {
        const key = registered(schedule); const heads = new Map<number, StoredPoll>(); let metadataBytes = 0;
        // The first branch reads the poll-time primary key. The second starts at
        // the receipt-time index, retaining late receipts from earlier slots.
        const rows = database.prepare(`
          SELECT slot_ms, body, sha256 FROM prepared_polls WHERE scope_key = ? AND slot_ms >= ? AND slot_ms < ?
          UNION
          SELECT p.slot_ms, p.body, p.sha256 FROM prepared_polls AS p
          JOIN (SELECT DISTINCT slot_ms FROM prepared_summaries WHERE scope_key = ? AND collected_ms >= ? AND collected_ms < ?) AS receipts
          ON p.slot_ms = receipts.slot_ms WHERE p.scope_key = ?
          ORDER BY slot_ms LIMIT ?
        `).iterate(key, startMs, endMs, key, startMs, endMs, key, PREPARED_MAX_QUERY_POLLS + 1);
        for (const raw of rows) {
          const row = raw as unknown as PollRow; metadataBytes += Buffer.byteLength(row.body);
          if (heads.size >= PREPARED_MAX_QUERY_POLLS || metadataBytes > MAX_QUERY_BYTES) throw new PreparedContractError();
          heads.set(row.slot_ms, pollHead(schedule, row));
        }
        const coverage: PreparedQueryResult['coverage'] = {
          expectedPolls, collectedPolls: 0, emptyPolls: 0, failedPolls: 0, missedPolls: 0, pendingPolls: expectedPolls,
          additionalReceiptPolls: 0, meaning: 'scheduled-slots-and-collection-receipts',
        };
        const counts = { ...coverage };
        for (const [slot, head] of heads) {
          if (slot < startMs || slot >= endMs) { counts.additionalReceiptPolls++; continue; }
          counts.pendingPolls--;
          if (head.poll.status === 'collected') counts.collectedPolls++;
          else if (head.poll.status === 'empty') counts.emptyPolls++;
          else if (head.poll.status === 'failed') counts.failedPolls++;
          else counts.missedPolls++;
        }
        const summaries: PreparedSummary[] = []; let summaryBytes = 0; let truncated = false;
        const records = database.prepare('SELECT id, slot_ms, collected_ms, body, sha256 FROM prepared_summaries WHERE scope_key = ? AND collected_ms >= ? AND collected_ms < ? ORDER BY collected_ms, id LIMIT ?').iterate(key, startMs, endMs, limit + 1);
        for (const raw of records) {
          const row = raw as unknown as SummaryRow;
          if (summaries.length >= limit || summaryBytes + Buffer.byteLength(row.body) > MAX_QUERY_BYTES) { truncated = true; break; }
          const item = summary(schedule, row); const parent = heads.get(row.slot_ms);
          if (!parent?.summaryIds.includes(item.id)) return corrupt();
          summaries.push(item); summaryBytes += Buffer.byteLength(row.body);
        }
        return clone({ summaries, polls: [...heads.values()].map(head => head.poll), coverage: counts, truncated });
      });
    },
    async readSummary(value, id) {
      const schedule = validateSchedule(value);
      if (typeof id !== 'string' || !/^[a-f0-9]{64}$/.test(id)) throw new PreparedContractError();
      return transaction(false, () => {
        const key = registered(schedule);
        const row = database.prepare('SELECT id, slot_ms, collected_ms, body, sha256 FROM prepared_summaries WHERE scope_key = ? AND id = ?').get(key, id) as SummaryRow | undefined;
        if (!row) return undefined;
        const item = summary(schedule, row);
        const parent = database.prepare('SELECT slot_ms, body, sha256 FROM prepared_polls WHERE scope_key = ? AND slot_ms = ?').get(key, row.slot_ms) as PollRow | undefined;
        if (!parent || !pollHead(schedule, parent).summaryIds.includes(id)) return corrupt();
        return item;
      });
    },
    close() { if (!closed) { guard(() => database.close()); closed = true; } },
  };
}
