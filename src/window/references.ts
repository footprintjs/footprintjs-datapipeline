import type { TraceRef } from '../contracts.js';

export function identifier(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 512 && !/[\u0000-\u001f\u007f]/.test(value);
}

export function traceReference(ack: unknown): TraceRef | undefined {
  if (ack === null || typeof ack !== 'object') return undefined;
  const kind = Object.getOwnPropertyDescriptor(ack, 'kind');
  const id = Object.getOwnPropertyDescriptor(ack, 'id');
  if (!kind || !('value' in kind) || kind.value !== 'trace' || !id || !('value' in id) || !identifier(id.value)) return undefined;
  return { kind: 'trace', id: id.value };
}

