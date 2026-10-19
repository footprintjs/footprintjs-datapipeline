/** JSON-safe values; integer fields and nanoseconds never pass through Number. */
export type InfluxField =
  | { readonly type: 'float'; readonly value: number }
  | { readonly type: 'integer'; readonly value: string }
  | { readonly type: 'boolean'; readonly value: boolean }
  | { readonly type: 'string'; readonly value: string };

/** Ordered pairs retain caller order; duplicate keys are invalid. */
export interface InfluxPoint {
  readonly measurement: string;
  readonly tags: readonly (readonly [string, string])[];
  readonly fields: readonly (readonly [string, InfluxField])[];
  /** Caller-chosen observation/poll moment, expressed as integer nanoseconds. */
  readonly timestampNs: string;
}

/** Optional lower ceilings; the implementation never silently truncates points. */
export interface InfluxLimits {
  readonly maxPoints?: number;
  readonly maxBytes?: number;
  readonly maxBatchPoints?: number;
  readonly maxBatchBytes?: number;
}

export const INFLUX_DEFAULT_LIMITS = Object.freeze({
  maxPoints: 10_000,
  maxBytes: 8 * 1024 * 1024,
  maxBatchPoints: 5_000,
  maxBatchBytes: 1024 * 1024,
});

export type InfluxValidationCode = 'invalid-data' | 'limit-exceeded' | 'invalid-options';

export class InfluxValidationError extends Error {
  readonly name = 'InfluxValidationError';
  constructor(readonly code: InfluxValidationCode) {
    super('InfluxDB input or options were refused.');
  }
}

export type InfluxWriteCode = InfluxValidationCode | 'credentials-unavailable' |
  'cancelled' | 'timeout' | 'http-error' | 'transport-error';

/** A dispatched request may have been partially stored even when it failed. */
export interface InfluxBatchReceipt {
  readonly index: number;
  readonly pointCount: number;
  readonly status: 'acknowledged' | 'unknown';
  readonly code?: InfluxWriteCode;
  readonly httpStatus?: number;
}

export interface InfluxWriteReceipt {
  readonly status: 'empty' | 'acknowledged' | 'partial' | 'unknown' | 'not-written';
  readonly totalPoints: number;
  readonly acknowledgedPoints: number;
  readonly uncertainPoints: number;
  readonly notAttemptedPoints: number;
  readonly batches: readonly InfluxBatchReceipt[];
  readonly code?: InfluxWriteCode;
}

export interface InfluxDbSinkOptions {
  /** Trusted host configuration, never a model-selected URL. */
  readonly url: string;
  readonly org: string;
  readonly bucket: string;
  /** Secrets stay outside point data and returned receipts. */
  readonly getToken: () => string | Promise<string>;
  readonly limits?: InfluxLimits;
  /** Whole write budget, including credential resolution; default/max 30 s. */
  readonly timeoutMs?: number;
  readonly fetch?: typeof globalThis.fetch;
}

export interface InfluxDbSink {
  write(points: readonly InfluxPoint[], options?: { readonly signal?: AbortSignal }): Promise<InfluxWriteReceipt>;
}
