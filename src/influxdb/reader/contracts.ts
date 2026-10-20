import type { DeriveWindow, JsonObject, VersionedRef } from '../../contracts.js';

export interface InfluxQueryTemplate {
  readonly ref: VersionedRef;
  /** Trusted, versioned Flux text. {{bucket}}, {{start}}, {{end}}, {{limit}} are reserved bindings. */
  readonly text: string;
  readonly parameters: Readonly<Record<string, 'string' | 'integer' | 'boolean'>>;
}
export interface InfluxReadRequest {
  readonly templateRef: VersionedRef;
  readonly window: DeriveWindow;
  readonly parameters: JsonObject;
}
export type InfluxReadCode = 'invalid-request' | 'limit-exceeded' | 'credentials-unavailable' |
  'http-error' | 'invalid-response' | 'secret-echo' | 'transport-error' | 'timeout' | 'cancelled';
export interface InfluxReadReceipt {
  readonly source: 'influxdb-query';
  readonly status: 'complete' | 'failed';
  readonly requestedAt: string | null;
  readonly finishedAt: string | null;
  readonly dispatched: boolean;
  readonly sourceMutable: true;
  /** Aggregate-only reads cannot prove whether contributing source rows change later. */
  readonly sourceRowsFingerprint: 'not-captured';
  /** Effective enforced bounds, also present when the request fails. */
  readonly limits: { readonly maxRows: number; readonly maxBytes: number; readonly maxWindowMs: number; readonly timeoutMs: number };
  readonly rowCount: number;
  readonly code?: InfluxReadCode;
  readonly httpStatus?: number;
  /** Bounded server reference only; response/error bodies are never persisted. */
  readonly requestId?: string;
  readonly bucket?: string;
  readonly org?: string;
  readonly templateRef?: VersionedRef;
  readonly window?: DeriveWindow;
  readonly parameters?: JsonObject;
  readonly queryText?: string;
  readonly querySha256?: string;
  readonly responseSha256?: string;
  readonly resultSha256?: string;
}
export interface InfluxReadResult {
  readonly outcome: 'complete' | 'failed';
  /** CSV cells remain text, including int64 values and identifiers with leading zeroes. */
  readonly rows: readonly Readonly<Record<string, string>>[];
  readonly receipt: InfluxReadReceipt;
}
export interface InfluxDbReader {
  read(request: unknown, options?: { readonly signal?: AbortSignal }): Promise<InfluxReadResult>;
}
export interface InfluxDbReaderOptions {
  readonly url: string;
  readonly org: string;
  readonly bucket: string;
  /** Host must provision a read-only token; this client cannot infer its server grants. */
  readonly getToken: () => string | Promise<string>;
  readonly templates: readonly InfluxQueryTemplate[];
  readonly maxRows?: number;
  readonly maxBytes?: number;
  readonly maxWindowMs?: number;
  readonly timeoutMs?: number;
  readonly fetch?: typeof globalThis.fetch;
  readonly clock?: () => string;
}
export class ReadFault extends Error {
  constructor(readonly code: InfluxReadCode) { super('InfluxDB read could not complete.'); }
}
