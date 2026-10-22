import { SealedHistoryError, type SealedHistoryCode } from './contracts.js';

const SQLITE_PERM = 3, SQLITE_AUTH = 23, SQLITE_CORRUPT = 11, SQLITE_NOTADB = 26;
export const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'ENOENT';
export const exists = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === 'EEXIST';
function accessDenied(error: unknown): boolean {
  return ['EACCES', 'EPERM'].includes((error as NodeJS.ErrnoException)?.code ?? '');
}
export function classifyReadError(error: unknown): SealedHistoryCode {
  if (error instanceof SealedHistoryError) return error.code;
  return accessDenied(error) ? 'SEALED_HISTORY_UNMANAGED' : 'SEALED_HISTORY_UNAVAILABLE';
}
export function classifyMarkerReadError(error: unknown): SealedHistoryCode {
  if (error instanceof SealedHistoryError) return error.code;
  const extended = (error as { errcode?: number })?.errcode;
  const code = typeof extended === 'number' ? extended & 0xff : undefined;
  if (code === SQLITE_PERM || code === SQLITE_AUTH) return 'SEALED_HISTORY_UNMANAGED';
  if (code === SQLITE_CORRUPT || code === SQLITE_NOTADB) return 'SEALED_HISTORY_CORRUPT';
  return classifyReadError(error);
}
