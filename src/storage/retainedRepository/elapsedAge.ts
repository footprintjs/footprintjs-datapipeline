import type { DatabaseSync } from 'node:sqlite';
import { RepositoryRetentionError } from './contracts.js';
const invalid = (): never => { throw new RepositoryRetentionError('REPOSITORY_CLOCK_UNAVAILABLE'); };
/** One durable high-water mark. Independent handles take a maximum, never sum their clocks. */
export class ElapsedAge {
  private readonly baseAge: number;
  private readonly origin: number;
  private last: number;
  private minimum: number;
  constructor(private readonly db: DatabaseSync, private readonly clock: () => number) {
    this.minimum = this.baseAge = this.saved();
    this.last = this.origin = this.sample();
  }
  private saved(): number {
    const value = this.db.prepare('SELECT ageMs FROM elapsed_clock WHERE id=1').get()?.ageMs;
    if (!Number.isSafeInteger(value) || Number(value) < 0) return invalid();
    return Number(value);
  }
  private sample(): number {
    let value: number;
    try { value = this.clock(); } catch { return invalid(); }
    if (!Number.isFinite(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) return invalid();
    return value;
  }
  begin(): void { this.minimum = this.saved(); this.now(); }
  now(): number {
    const sample = this.sample();
    if (sample < this.last) return invalid();
    this.last = sample;
    const value = this.baseAge + Math.floor(sample - this.origin);
    if (!Number.isSafeInteger(value) || value < 0) return invalid();
    return Math.max(this.minimum, value);
  }
  checkpoint(): void {
    const age = this.now();
    this.db.prepare('UPDATE elapsed_clock SET ageMs=? WHERE id=1 AND ageMs<=?').run(age, age);
  }
}
