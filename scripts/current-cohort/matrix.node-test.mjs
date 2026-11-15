// Inert candidate metadata/orchestration controls, never native runtime proof.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { runEngineCohorts } from './matrix.mjs';

function output(t) {
  const directory = mkdtempSync(join(tmpdir(), 'dp-matrix-data-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return join(directory, 'evidence');
}
const candidate = () => ({ name: 'footprintjs-datapipeline', version: '0.11.0', bytes: 100, sha256: 'a'.repeat(64) });
function save(directory, value) {
  mkdirSync(directory);
  writeFileSync(join(directory, 'candidate.json'), JSON.stringify(value));
}
const summary = directory => JSON.parse(readFileSync(join(directory, 'MATRIX.json'), 'utf8'));

test('matrix runs both declared profiles and joins the same candidate metadata', async t => {
  const directory = output(t), calls = [];
  await runEngineCohorts('source-data', directory, async (source, at, profile) => {
    calls.push({ source, at, profile }); save(at, candidate());
  });
  assert.deepEqual(calls, [
    { source: 'source-data', at: directory, profile: '11' },
    { source: 'source-data', at: join(directory, 'engine-12'), profile: '12' },
  ]);
  assert.deepEqual(summary(directory), { status: 'PASSED', completed: ['11', '12'], sameArchive: true, profiles: ['11', '12'] });
});

test('different candidate bytes cannot inherit a two-engine acceptance result', async t => {
  const directory = output(t);
  await assert.rejects(runEngineCohorts('source-data', directory, async (_source, at, profile) => {
    save(at, { ...candidate(), sha256: (profile === '12' ? 'b' : 'a').repeat(64) });
  }), /candidate sha256 drift/);
  assert.equal(summary(directory).status, 'FAILED');
  assert.equal(summary(directory).sameArchive, false);
});

test('missing candidate identity, size or digest is unknown rather than identical', async t => {
  const directory = output(t);
  await assert.rejects(runEngineCohorts('source-data', directory, async (_source, at) => {
    save(at, {});
  }), /Invalid engine matrix candidate/);
  assert.equal(summary(directory).sameArchive, false);
});

test('a failed second runtime profile stays the primary error and never becomes completed', async t => {
  const directory = output(t), primary = new Error('native-profile-failed');
  await assert.rejects(runEngineCohorts('source-data', directory, async (_source, at, profile) => {
    save(at, candidate()); if (profile === '12') throw primary;
  }), error => error === primary);
  assert.deepEqual(summary(directory), { status: 'FAILED', completed: ['11'], sameArchive: false, profiles: ['11', '12'] });
});
