import { describe, expect, it } from 'vitest';
import {
  assertRef,
  assertTime,
  canonical,
  cloneJson,
  digest,
  validateCandidate,
} from '../src/validation.js';

const at = '2026-10-16T10:00:00.000Z';
const later = '2026-10-16T11:00:00.000Z';
const limits = { maxObservations: 10, maxBytes: 100_000 };

function candidate() {
  return {
    key: 'poll-1',
    outcome: 'complete' as const,
    coverage: {
      status: 'complete' as const,
      time: { kind: 'point' as const, at, basis: 'source' as const },
      selection: { cluster: 'example' },
    },
    observations: [{
      key: 'node-1',
      subject: { node: '1' },
      schemaRef: { id: 'example.metric', version: '1' },
      time: { kind: 'point' as const, at, basis: 'source' as const },
      collectedAt: later,
      payload: { value: 10, samples: [8, 12] },
    }],
    details: { selection: { topN: 25 } },
  };
}

describe('JSON boundary', () => {
  it('canonicalizes object keys recursively while preserving array order', () => {
    const first = { z: [{ b: 2, a: 1 }, 3], a: 'value' };
    const reordered = { a: 'value', z: [{ a: 1, b: 2 }, 3] };
    expect(canonical(first)).toBe(canonical(reordered));
    expect(digest(first)).toBe(digest(reordered));
    expect(digest(first)).not.toBe(digest({ a: 'value', z: [3, { a: 1, b: 2 }] }));
    expect(digest(first)).toMatch(/^[a-f0-9]{64}$/);
  });

  it.each([
    ['undefined', undefined],
    ['undefined property', { missing: undefined }],
    ['undefined array element', [undefined]],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['negative Infinity', Number.NEGATIVE_INFINITY],
    ['bigint', 1n],
    ['symbol', Symbol('value')],
    ['function', () => 1],
    ['Date', new Date(at)],
    ['Map', new Map([['value', 1]])],
    ['Set', new Set([1])],
    ['class instance', new (class Value { value = 1; })()],
  ])('rejects %s rather than silently changing its meaning', (_name, value) => {
    expect(() => canonical(value)).toThrow();
  });

  it('rejects cycles but accepts repeated acyclic JSON values', () => {
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(() => canonical(cyclic)).toThrow();
    const shared = { value: 1 };
    expect(JSON.parse(canonical({ first: shared, second: shared }))).toEqual({
      first: { value: 1 }, second: { value: 1 },
    });
  });

  it('rejects sparse arrays even when an extra property masks the key count', () => {
    const sparse = new Array(1) as unknown[] & { extra?: number };
    sparse.extra = 1;
    expect(() => canonical(sparse)).toThrow();
  });

  it('rejects array accessors without invoking caller code', () => {
    let invoked = false;
    const accessor: unknown[] = [];
    Object.defineProperty(accessor, '0', { enumerable: true, get() { invoked = true; return 1; } });
    expect(() => canonical(accessor)).toThrow();
    expect(invoked).toBe(false);
  });

  it('clones nested JSON so a boundary consumer cannot mutate the caller', () => {
    const original = { values: [{ value: 1 }] };
    const detached = cloneJson(original);
    detached.values[0]!.value = 2;
    expect(original.values[0]!.value).toBe(1);
    expect(detached.values).not.toBe(original.values);
  });
});

describe('references and clocks', () => {
  it('requires both parts of a pinned reference', () => {
    expect(() => assertRef({ id: 'adapter', version: '1' })).not.toThrow();
    for (const value of [null, 'adapter', [], {}, { id: 'adapter' },
      { id: '', version: '1' }, { id: 'adapter', version: '' },
      { id: 'adapter', version: 1 }]) {
      expect(() => assertRef(value)).toThrow();
    }
  });

  it('accepts explicit source, collection and unknown-source time', () => {
    expect(() => assertTime({ kind: 'point', at, basis: 'source' })).not.toThrow();
    expect(() => assertTime({ kind: 'point', at, basis: 'collection' })).not.toThrow();
    expect(() => assertTime({ kind: 'interval', start: at, end: later, basis: 'source' })).not.toThrow();
    expect(() => assertTime({ kind: 'source-time-unknown', reason: 'API omits its window' })).not.toThrow();
  });

  it.each([
    ['missing basis', { kind: 'point', at }],
    ['unrecognized basis', { kind: 'point', at, basis: 'estimated' }],
    ['invalid timestamp', { kind: 'point', at: 'yesterday', basis: 'source' }],
    ['impossible calendar date', { kind: 'point', at: '2026-02-30T10:00:00.000Z', basis: 'source' }],
    ['reversed interval', { kind: 'interval', start: later, end: at, basis: 'source' }],
    ['missing interval endpoint', { kind: 'interval', start: at, basis: 'source' }],
  ])('rejects %s', (_name, value) => {
    expect(() => assertTime(value)).toThrow();
  });
});

describe('candidate validation', () => {
  it('returns a detached, validated candidate including capture details', () => {
    const original = candidate();
    const validated = validateCandidate(original, limits);
    expect(validated).toEqual(original);
    expect(validated).not.toBe(original);
    expect(validated.observations).not.toBe(original.observations);
    expect(validated.observations[0]!.payload).not.toBe(original.observations[0]!.payload);
    original.observations[0]!.payload.value = 99;
    original.details.selection.topN = 1;
    expect(validated.observations[0]!.payload).toEqual({ value: 10, samples: [8, 12] });
    expect(validated.details).toEqual({ selection: { topN: 25 } });
  });

  it('accepts an explicitly empty complete selection', () => {
    expect(validateCandidate({ ...candidate(), observations: [] }, limits).observations).toEqual([]);
  });

  it('retains collection time when source time and coverage are unknown', () => {
    const valid = candidate();
    const result = validateCandidate({
      ...valid,
      coverage: { status: 'unknown', time: { kind: 'source-time-unknown' } },
      observations: [{ ...valid.observations[0], time: { kind: 'source-time-unknown' } }],
    }, limits);
    expect(result.observations[0]!.time.kind).toBe('source-time-unknown');
    expect(result.observations[0]!.collectedAt).toBe(later);
  });

  it('rejects duplicate logical observation keys', () => {
    const valid = candidate();
    valid.observations.push({ ...valid.observations[0]! });
    expect(() => validateCandidate(valid, limits)).toThrow();
  });

  it('requires exactly one inline payload or external artifact reference', () => {
    const valid = candidate();
    const { payload: _payload, ...withoutPayload } = valid.observations[0]!;
    const externalArtifactRef = { uri: 'file:///capture.json', sha256: 'a'.repeat(64), sizeBytes: 100 };
    const external = { ...withoutPayload, externalArtifactRef };
    expect(validateCandidate({ ...valid, observations: [external] }, limits).observations[0])
      .toEqual(external);
    expect(() => validateCandidate({ ...valid, observations: [withoutPayload] }, limits)).toThrow();
    expect(() => validateCandidate({
      ...valid, observations: [{ ...valid.observations[0], externalArtifactRef }],
    }, limits)).toThrow();
  });

  it.each([
    ['null reference', null],
    ['false reference', false],
    ['bad digest', { uri: 'file:///capture.json', sha256: 'not-a-digest', sizeBytes: 1 }],
    ['negative byte count', { uri: 'file:///capture.json', sha256: 'a'.repeat(64), sizeBytes: -1 }],
    ['missing URI', { sha256: 'a'.repeat(64), sizeBytes: 1 }],
  ])('rejects artifact metadata with %s', (_name, externalArtifactRef) => {
    const valid = candidate();
    const { payload: _payload, ...withoutPayload } = valid.observations[0]!;
    expect(() => validateCandidate({
      ...valid, observations: [{ ...withoutPayload, externalArtifactRef }],
    }, limits)).toThrow();
  });

  it('rejects malformed observation schemas, keys, times and support references', () => {
    const valid = candidate();
    for (const replacement of [
      { ...valid.observations[0], key: '' },
      { ...valid.observations[0], schemaRef: { id: 'example.metric' } },
      { ...valid.observations[0], time: { kind: 'point', at: 'bad', basis: 'source' } },
      { ...valid.observations[0], collectedAt: 'bad' },
      { ...valid.observations[0], inputRefs: [{ kind: 'evidence', id: 'wrong-kind' }] },
    ]) {
      expect(() => validateCandidate({ ...valid, observations: [replacement] }, limits)).toThrow();
    }
  });

  it('bounds observation count independently of bytes', () => {
    const valid = candidate();
    valid.observations.push({ ...valid.observations[0]!, key: 'node-2' });
    expect(() => validateCandidate(valid, { ...limits, maxObservations: 1 })).toThrow();
  });

  it('bounds the complete JSON batch in UTF-8 bytes', () => {
    const valid = candidate();
    valid.observations[0]!.payload = { value: 10, samples: [8, 12], text: '💾'.repeat(100) } as typeof valid.observations[0]['payload'];
    const bytes = Buffer.byteLength(canonical(valid), 'utf8');
    expect(() => validateCandidate(valid, { ...limits, maxBytes: bytes })).not.toThrow();
    expect(() => validateCandidate(valid, { ...limits, maxBytes: bytes - 1 })).toThrow();
  });

  it('rejects unsupported values anywhere in a candidate', () => {
    const valid = candidate();
    expect(() => validateCandidate({ ...valid, details: { invalid: Number.NaN } }, limits)).toThrow();
  });

  it('requires the details envelope to be a JSON object', () => {
    expect(() => validateCandidate({ ...candidate(), details: 'opaque receipt' }, limits)).toThrow();
  });
});
