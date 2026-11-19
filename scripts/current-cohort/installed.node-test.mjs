import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { physicalOwners } from './installed.mjs';

function tree(t) {
  const directory = realpathSync(mkdtempSync(join(tmpdir(), 'cohort-directory-facts-')));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  for (const name of ['footprintjs', 'foottrace']) packageData(join(directory, 'node_modules', name), name);
  return directory;
}
function packageData(path, name) {
  mkdirSync(path, { recursive: true });
  writeFileSync(join(path, 'package.json'), JSON.stringify({ name, version: name === 'foottrace' ? '2.1.0' : '14.0.0' }));
}
test('reads ordinary owner locations from real temporary directories', t => {
  const directory = tree(t);
  const { physical } = physicalOwners(directory);
  assert.deepEqual(physical, { footprintjs: [join(directory, 'node_modules/footprintjs')],
    foottrace: [join(directory, 'node_modules/foottrace')] });
});
for (const name of ['footprintjs', 'foottrace']) {
  test(`${name}: discovers an unlisted nested owner without relying on lock data or version difference`, t => {
    const directory = tree(t);
    const nested = join(directory, 'node_modules/@scope/consumer/dist/node_modules', name);
    packageData(join(directory, 'node_modules/@scope/consumer'), '@scope/consumer');
    packageData(nested, name);
    assert.deepEqual(physicalOwners(directory).physical[name], [join(directory, 'node_modules', name), nested].sort());
  });
}
test('legal hidden package assets are scanned and an alias owner inside them cannot disappear', t => {
  const directory = tree(t);
  mkdirSync(join(directory, 'node_modules/footprintjs/.claude/templates'), { recursive: true });
  assert.equal(physicalOwners(directory).physical.footprintjs.length, 1);
  const alias = join(directory, 'node_modules/footprintjs/.claude/templates/node_modules/engine-alias');
  packageData(alias, 'footprintjs');
  assert.deepEqual(physicalOwners(directory).physical.footprintjs,
    [join(directory, 'node_modules/footprintjs'), alias].sort());
});
test('normal and scoped npm alias slots are counted by manifest identity, with exact version retained', t => {
  const directory = tree(t);
  const plain = join(directory, 'node_modules/engine-alias'); packageData(plain, 'footprintjs');
  const scoped = join(directory, 'node_modules/@scope/trace-alias'); packageData(scoped, 'foottrace');
  const report = physicalOwners(directory);
  assert.equal(report.physical.footprintjs.length, 2); assert.equal(report.physical.foottrace.length, 2);
  assert.ok(report.nativePackages.some(row => row.installName === 'engine-alias' && row.name === 'footprintjs' && row.version === '14.0.0'));
  assert.ok(report.nativePackages.some(row => row.installName === '@scope/trace-alias' && row.name === 'foottrace' && row.version === '2.1.0'));
});
test('absent, unreadable or conflicting package identity is not proof of no native owner', t => {
  const directory = tree(t);
  const path = join(directory, 'node_modules/unidentified'); mkdirSync(path);
  assert.throws(() => physicalOwners(directory), /ENOENT/);
  writeFileSync(join(path, 'package.json'), '{');
  assert.throws(() => physicalOwners(directory), SyntaxError);
  rmSync(path, { recursive: true });
  writeFileSync(join(directory, 'node_modules/footprintjs/package.json'), JSON.stringify({ name: 'another-package', version: '14.0.0' }));
  assert.throws(() => physicalOwners(directory), /another package identity/);
});
test('does not silently skip a linked package tree or an unknown hidden package store', t => {
  const directory = tree(t);
  const linked = join(directory, 'node_modules/linked');
  symlinkSync(join(directory, 'node_modules/footprintjs'), linked, process.platform === 'win32' ? 'junction' : 'dir');
  assert.throws(() => physicalOwners(directory), /unclassified links/);
  rmSync(linked);
  mkdirSync(join(directory, 'node_modules/.store'));
  assert.throws(() => physicalOwners(directory), /unclassified package stores/);
});
