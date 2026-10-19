import { InfluxValidationError } from './contracts.js';

const abortedGetter = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')!.get!;

/** Use the native brand check without invoking an input's property accessors. */
export function signalAborted(signal: AbortSignal): boolean {
  return abortedGetter.call(signal) as boolean;
}

export function writeSignal(options: unknown): AbortSignal | undefined {
  try {
    if (!options || typeof options !== 'object') throw new Error();
    const descriptor = Object.getOwnPropertyDescriptor(options, 'signal');
    if (!descriptor) return undefined;
    if (!('value' in descriptor)) throw new Error();
    if (descriptor.value === undefined) return undefined;
    signalAborted(descriptor.value as AbortSignal);
    return descriptor.value as AbortSignal;
  } catch {
    throw new InfluxValidationError('invalid-options');
  }
}

/** Counts are diagnostic on refusal; successful counts come from encoded lines. */
export function inspectPointCount(points: unknown): number {
  try {
    if (!Array.isArray(points)) return 0;
    const descriptor = Object.getOwnPropertyDescriptor(points, 'length');
    const value: unknown = descriptor && 'value' in descriptor ? descriptor.value : undefined;
    return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : 0;
  } catch {
    return 0;
  }
}
