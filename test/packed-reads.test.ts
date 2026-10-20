import { expect, it } from 'vitest';
import { PackedReads } from '../src/storage/packedReads.js';

it('admits four inflates, shares overlapping keys, and reloads completed entries', async () => {
  const started: string[] = [], release = new Map<string, () => void>();
  const reads = new PackedReads(async path => {
    started.push(path);
    await new Promise<void>(resolve => release.set(path, resolve));
    return path;
  });
  const first = reads.read('a');
  expect(reads.read('a')).toBe(first);
  const rest = ['b', 'c', 'd', 'e'].map(path => reads.read(path));
  expect(started).toEqual(['a', 'b', 'c', 'd']);
  release.get('a')!();
  await first;
  expect(started).toEqual(['a', 'b', 'c', 'd', 'e']);
  for (const key of ['b', 'c', 'd', 'e']) release.get(key)!();
  await Promise.all(rest);
  const again = reads.read('a');
  expect(started).toEqual(['a', 'b', 'c', 'd', 'e', 'a']);
  release.get('a')!(); await again;
});

it('a failed inflate releases its slot and is never retained as a cached result', async () => {
  let attempts = 0;
  const reads = new PackedReads(async () => { if (++attempts === 1) throw Error('refused'); return 'fresh'; });
  await expect(reads.read('one')).rejects.toThrow('refused');
  expect(await reads.read('one')).toBe('fresh');
});
