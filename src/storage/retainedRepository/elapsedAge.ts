import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError } from './contracts.js';
const invalid = (): never => { throw new RepositoryRetentionError('REPOSITORY_CLOCK_UNAVAILABLE'); };
interface SavedAge { age: number; revision: number }
interface Anchor extends SavedAge { baseAge: number; origin: number }
/** A foreign acceptance invalidates every unpublished interval predating it. */
export class ElapsedAge {
  private anchor: Anchor | undefined;
  private transaction: Anchor | undefined;
  private pending: Anchor | undefined;
  private last: number | undefined;
  private accepted = false;
  constructor(private readonly db: DatabaseSync, private readonly clock: () => number) {
    const saved = this.saved();
    // Sample after the durable snapshot: a new process never credits downtime.
    this.anchor = { ...saved, baseAge: saved.age, origin: this.sample() };
  }
  private saved(): SavedAge {
    const row = this.db.prepare('SELECT ageMs,revision FROM elapsed_clock WHERE id=1').get();
    if (!Number.isSafeInteger(row?.ageMs) || Number(row?.ageMs) < 0 ||
      !Number.isSafeInteger(row?.revision) || Number(row?.revision) < 0) return invalid();
    return { age: Number(row!.ageMs), revision: Number(row!.revision) };
  }
  private sample(): number {
    let value: number;
    try { value = this.clock(); } catch { return invalid(); }
    if (!Number.isFinite(value) || value < 0 || value > Number.MAX_SAFE_INTEGER ||
      (this.last !== undefined && value < this.last)) return invalid();
    this.last = value;
    return value;
  }
  begin(): void {
    const saved = this.saved();
    // Refuse exhaustion before the transaction can select or delete any data.
    if (saved.revision === Number.MAX_SAFE_INTEGER) return invalid();
    const sample = this.sample(), anchor = this.anchor;
    this.transaction = anchor?.revision === saved.revision
      ? { ...anchor, age: saved.age } : { ...saved, baseAge: saved.age, origin: sample };
    this.pending = undefined;
    this.accepted = false;
    this.value(sample, false);
  }
  private value(sample: number, acceptance: boolean): number {
    const transaction = this.transaction;
    if (!transaction) return invalid();
    const elapsed = sample - transaction.origin;
    const value = transaction.baseAge + (acceptance ? Math.ceil(elapsed) : Math.floor(elapsed));
    if (!Number.isSafeInteger(value) || value < 0) return invalid();
    return Math.max(transaction.age, value);
  }
  now(): number { return this.value(this.sample(), false); }
  // Rounding acceptance upward prevents the preceding fractional millisecond
  // from being charged to the new record when expiry is rounded downward.
  acceptedNow(): number {
    const age = this.value(this.sample(), true);
    this.accepted = true;
    return age;
  }
  checkpoint(): void {
    const age = this.now(), transaction = this.transaction!;
    const revision = transaction.revision + (this.accepted ? 1 : 0);
    if (!Number.isSafeInteger(revision)) return invalid();
    this.db.prepare('UPDATE elapsed_clock SET ageMs=?,revision=? WHERE id=1').run(age, revision);
    this.pending = { ...transaction, age, revision };
  }
  // Called only after COMMIT succeeds; no sampling or other fallible work here.
  // Keeping the original local origin preserves fractional time across our own
  // commits. Any later foreign acceptance invalidates it at the next begin().
  committed(): void {
    this.anchor = this.pending;
    this.transaction = this.pending = undefined;
    this.accepted = false;
  }
  rolledBack(): void {
    // Failed work (including a refused final COMMIT) establishes no new clock
    // authority. Drop its entire interval and retain the backward-clock check.
    this.anchor = this.transaction = this.pending = undefined;
    this.accepted = false;
  }
}
