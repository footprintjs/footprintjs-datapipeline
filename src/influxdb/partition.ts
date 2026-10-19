import type { InfluxLimits } from './contracts.js';

export interface EncodedBatch { readonly body: string; readonly pointCount: number }

export function partitionLines(lines: readonly string[], limits: Required<InfluxLimits>): readonly EncodedBatch[] {
  const batches: EncodedBatch[] = [];
  let pending: string[] = [];
  let bytes = 0;
  function flush() {
    if (!pending.length) return;
    batches.push(Object.freeze({ body: pending.join('\n'), pointCount: pending.length }));
    pending = [];
    bytes = 0;
  }
  for (const line of lines) {
    const size = Buffer.byteLength(line);
    if (pending.length && (pending.length >= limits.maxBatchPoints || bytes + 1 + size > limits.maxBatchBytes)) {
      flush();
    }
    pending.push(line);
    bytes += size + (pending.length > 1 ? 1 : 0);
  }
  flush();
  return Object.freeze(batches);
}
