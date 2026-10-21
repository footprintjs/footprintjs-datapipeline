import { readFile } from 'node:fs/promises';
import { expect, it } from 'vitest';
import { CHECKSUM_BYTES, HEADER_BYTES, OPERATION_BYTES, REFERENCE_BYTES, sizeOf } from '../src/storage/sealedHistory/layout.js';

const report = JSON.parse(await readFile(new URL('../docs/measurements/sealed-history-2026-10-21.json', import.meta.url), 'utf8')) as {
  format: { headerBytes: number; operationBytes: number; referenceBytes: number; footerBytes: number; historyHorizon: null };
  cases: { namespaces: { originalBytes: number; saved: { bytes: number; operations: number; references: number };
    operationChecks: number; referenceChecks: number; totalBytes: number; fileAllocatedBytes: number;
    files: { name: string; bytes: number; allocatedBytes: number }[] }[] }[];
};
const documentation = await readFile(new URL('../docs/SEALED_HISTORY.md', import.meta.url), 'utf8');
function totals(sample: (typeof report.cases)[number]) {
  const namespaces = sample.namespaces;
  const files = namespaces.flatMap(namespace => namespace.files);
  return {
    original: namespaces.reduce((sum, namespace) => sum + namespace.originalBytes, 0),
    snapshots: namespaces.reduce((sum, namespace) => sum + namespace.saved.bytes, 0),
    markers: files.filter(file => file.name === 'retention.sqlite').reduce((sum, file) => sum + file.bytes, 0),
    catalogues: files.filter(file => file.name === 'catalogue.json').reduce((sum, file) => sum + file.bytes, 0),
    logical: files.reduce((sum, file) => sum + file.bytes, 0),
    allocated: files.reduce((sum, file) => sum + file.allocatedBytes, 0),
    index: namespaces.reduce((sum, namespace) => sum + namespace.saved.references * REFERENCE_BYTES, 0),
  };
}
it('pins the current layout against the historical report without claiming a replay of unavailable inputs', () => {
  expect(report.format).toEqual({ headerBytes: HEADER_BYTES, operationBytes: OPERATION_BYTES, referenceBytes: REFERENCE_BYTES, footerBytes: CHECKSUM_BYTES, historyHorizon: null });
  expect(sizeOf(3000, 9000)).toBe(669096);
  expect(sizeOf(3000, 12000)).toBe(777096);
  for (const sample of report.cases) for (const namespace of sample.namespaces) {
    expect(sizeOf(namespace.saved.operations, namespace.saved.references)).toBe(namespace.saved.bytes);
    expect(namespace.operationChecks).toBe(namespace.saved.operations);
    expect(namespace.referenceChecks).toBe(namespace.saved.references);
    expect(namespace.totalBytes).toBe(namespace.files.reduce((sum, file) => sum + file.bytes, 0));
    expect(namespace.fileAllocatedBytes).toBe(namespace.files.reduce((sum, file) => sum + file.allocatedBytes, 0));
  }
});
it('recomputes every documented table value and the 4.43× ratio for the single distinct size point', () => {
  expect(report.cases).toHaveLength(2);
  const measured = totals(report.cases[0]!);
  expect(totals(report.cases[1]!)).toEqual(measured);
  const sizes = (sample: (typeof report.cases)[number]) => sample.namespaces.map(({ saved }) => ({ operations: saved.operations, references: saved.references, bytes: saved.bytes }));
  expect(sizes(report.cases[1]!)).toEqual(sizes(report.cases[0]!));
  const labels = { original: 'Original two closed SQLite ledgers', snapshots: 'Two binary snapshots, including headers/checksums/index',
    markers: 'Two version-marker databases, including empty guard indexes', catalogues: 'Two catalogues', logical: 'Total logical files', allocated: 'File allocation on the test filesystem' } as const;
  for (const [key, label] of Object.entries(labels)) {
    const line = documentation.split('\n').find(row => row.startsWith('| ' + label + ' |'))!;
    expect(Number(line?.split('|')[2]?.replace(/[^\d]/g, ''))).toBe(measured[key as keyof typeof labels]);
  }
  expect((measured.original / measured.logical).toFixed(2)).toBe('4.43');
  expect(documentation).toContain('**4.43×**');
  expect(measured.index).toBe(756000);
  expect(documentation).toContain('756,000 bytes');
});
it('recomputes the explicitly projected 122 MB/day and 44.5 GB/year from complete logical file bytes', () => {
  const bytesPerUnit = totals(report.cases[0]!).logical;
  const dailyBytes = bytesPerUnit * (14 * 86_400 / 5 / 3000);
  expect(Math.round(dailyBytes / 1_000_000)).toBe(122);
  expect((dailyBytes * 365 / 1_000_000_000).toFixed(1)).toBe('44.5');
  expect(documentation).toContain('3,000-poll units at 14 sources every 5 seconds');
  expect(documentation).toContain('**122 MB/day or 44.5 GB/year**');
});
