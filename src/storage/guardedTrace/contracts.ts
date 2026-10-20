import type { TraceRef } from '../../contracts.js';
export const TRACE_RETENTION_MS = 30 * 86400_000;
export const TRACE_METADATA_ALLOWANCE = 64 * 1024;
export type TraceStorageCode = 'TRACE_INVALID_OPTIONS' | 'TRACE_UNSAFE_FILESYSTEM' | 'TRACE_DISK_PRESSURE' | 'TRACE_BUDGET_EXCEEDED' | 'TRACE_BUDGET_WARNING' | 'TRACE_EXPIRED' | 'TRACE_STORAGE_UNCONFIRMED' | 'TRACE_STORE_BUSY';
export class TraceStorageError extends Error {
 constructor(readonly code: TraceStorageCode) { super('Full trace storage was refused.'); this.name = 'TraceStorageError'; }
}
export interface TraceStorageHealth { readonly code: TraceStorageCode; readonly receiptSaved: boolean; readonly traceRef?: TraceRef }
export interface GuardedTraceOptions {
 readonly enabled?: boolean;
 readonly directory: string;
 readonly maxTraceBytesOnDisk: number;
 readonly warningAtBytes: number;
 /** Leave this free for other workloads; additional receipt reserve is separate. */
 readonly minFreeBytes: number;
 /** At least 16 MiB; reserved from trace writes, not preallocated or guaranteed. */
 readonly receiptReserveBytes: number;
 /** Defaults true; false is an explicit host exception, used by local fixtures. */
 readonly requireSeparateFilesystem?: boolean;
 readonly clock?: () => number;
 /** Must report independently of this evidence directory; callbacks cannot authorize writes. */
 readonly onHealth: (event: TraceStorageHealth) => void;
}
export interface TraceStorageReceipt { readonly id: string; readonly acceptedAt: number; readonly expiresAt: number; readonly bytes: number; readonly status: 'pending' | 'stored' | 'expired' }
export interface TraceStorageGapReceipt { readonly id: string; readonly at: number; readonly status: 'not-saved'; readonly code: TraceStorageCode }
export function checked(options: GuardedTraceOptions): void {
 const sizes = [options.maxTraceBytesOnDisk, options.warningAtBytes, options.minFreeBytes, options.receiptReserveBytes];
 if (sizes.some(n => !Number.isSafeInteger(n) || n < 1) || options.warningAtBytes > options.maxTraceBytesOnDisk || options.receiptReserveBytes < 16 * 1024 * 1024 || typeof options.onHealth !== 'function') throw new TraceStorageError('TRACE_INVALID_OPTIONS');
}
export function traceId(ref: TraceRef): string {
 if (ref.kind !== 'trace' || !/^[a-f0-9]{64}$/.test(ref.id)) throw new TraceStorageError('TRACE_INVALID_OPTIONS');
 return ref.id;
}
