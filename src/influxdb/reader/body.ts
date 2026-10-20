import { createHash } from 'node:crypto';
import { ReadFault } from './contracts.js';
import type { createWriteBudget } from '../budget.js';

export async function readQueryBody(response: Response, maximum: number, budget: ReturnType<typeof createWriteBudget>) {
  if (!response.body) return { text: '', sha256: createHash('sha256').update('').digest('hex') };
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = []; let total = 0;
  const cancel = () => { void reader.cancel().catch(() => {}); };
  budget.signal.addEventListener('abort', cancel, { once: true });
  try {
    while (true) {
      const chunk = await budget.wait(() => reader.read());
      if (chunk.done) break;
      total += chunk.value.byteLength;
      if (total > maximum) throw new ReadFault('limit-exceeded');
      chunks.push(chunk.value);
    }
    const bytes = Buffer.concat(chunks);
    let text: string;
    try { text = new TextDecoder('utf-8', { fatal: true }).decode(bytes); }
    catch { throw new ReadFault('invalid-response'); }
    return { text, sha256: createHash('sha256').update(bytes).digest('hex') };
  } finally {
    budget.signal.removeEventListener('abort', cancel); cancel();
    try { reader.releaseLock(); } catch { /* A cancelled foreign stream may still be settling. */ }
  }
}
