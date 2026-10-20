/** Bound inflates per repository; share only overlapping reads, never cache completed data. */
export class PackedReads<T> {
  private readonly pending = new Map<string, Promise<T>>();
  private readonly waiting: (() => void)[] = [];
  private active = 0;
  constructor(private readonly load: (path: string) => Promise<T>) {}

  read(path: string): Promise<T> {
    const existing = this.pending.get(path);
    if (existing) return existing;
    const promise = this.run(path).finally(() => this.pending.delete(path));
    this.pending.set(path, promise);
    return promise;
  }

  private async run(path: string): Promise<T> {
    if (this.active >= 4) await new Promise<void>(resolve => this.waiting.push(resolve));
    else this.active++;
    try { return await this.load(path); }
    finally {
      const next = this.waiting.shift();
      if (next) next(); else this.active--;
    }
  }
}
