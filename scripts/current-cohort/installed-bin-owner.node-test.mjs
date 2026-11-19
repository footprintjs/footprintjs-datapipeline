// Finite directory/JSON resolver controls; no package is loaded or executed.
// Only package manifests/JSON DATA are resolved; no fixture module is loaded.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, realpathSync, renameSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { inspectInstalled, physicalOwners } from './installed.mjs';

const VERSIONS = { footprintjs: '14.0.0', foottrace: '2.1.0' };
const CANDIDATE = '0.10.1'; // Fixture DATA; no source/version mutation.

function packageData(directory, name, version = VERSIONS[name] || '0.0.0') {
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, 'package.json'), JSON.stringify({ name, version, main: './entry.json' }));
  writeFileSync(join(directory, 'entry.json'), JSON.stringify({ fixtureDataOnly: true }));
}

function consumer(t) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'dp-bin-owner-data-')));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  writeFileSync(join(root, 'package.json'), JSON.stringify({ name: 'inert-consumer', private: true, version: '0.0.0' }));
  packageData(join(root, 'node_modules/footprintjs-datapipeline'), 'footprintjs-datapipeline', CANDIDATE);
  for (const [name, version] of Object.entries(VERSIONS)) packageData(join(root, 'node_modules', name), name, version);
  writeFileSync(join(root, 'package-lock.json'), JSON.stringify({ lockfileVersion: 3, packages: {
    '': { name: 'inert-consumer', version: '0.0.0' },
    'node_modules/footprintjs-datapipeline': { version: CANDIDATE },
    ...Object.fromEntries(Object.entries(VERSIONS).map(([name, version]) => [`node_modules/${name}`, { version }])),
  } }));
  return root;
}

function probe(probeDirectory, requested) {
  mkdirSync(probeDirectory, { recursive: true });
  const anchor = join(probeDirectory, 'probe.cjs');
  writeFileSync(anchor, '// Resolver anchor DATA; this file is never executed.\n');
  return createRequire(anchor).resolve(requested);
}

test('DATA-only resolver fixture has one actual root/candidate owner before any duplicate is added', t => {
  const root = consumer(t);
  const result = inspectInstalled(root, CANDIDATE);
  for (const name of Object.keys(VERSIONS)) {
    assert.deepEqual(result.physical[name], [join(root, 'node_modules', name)]);
    assert.equal(probe(root, name), join(root, 'node_modules', name, 'entry.json'));
  }
});

const placements = [
  { label: 'ordinary .bin directory', at: '.bin/hidden', slot: name => name },
  { label: 'dot descendants of .bin', at: '.bin/.hidden/.cache', slot: name => name },
  { label: 'plain alias inside .bin', at: '.bin/hidden', slot: name => `${name}-alias` },
  { label: 'scoped alias inside .bin', at: '.bin/hidden', slot: name => `@fixture/${name}-alias` },
  { label: 'nested node_modules/.bin directory', at: '.bin/hidden/node_modules/.bin/deeper', slot: name => name },
];

for (const name of Object.keys(VERSIONS)) {
  for (const placement of placements) {
    test(`${name}: resolver-reachable same-version owner in ${placement.label} is counted and rejected`, t => {
      const root = consumer(t);
      // This positive precondition prevents an incomplete DP/entry fixture from
      // disguising the old inspector bypass as a setup/import failure.
      inspectInstalled(root, CANDIDATE);
      const at = join(root, 'node_modules', placement.at);
      const installName = placement.slot(name);
      const duplicate = join(at, 'node_modules', installName);
      packageData(duplicate, name);
      assert.equal(probe(at, installName), join(duplicate, 'entry.json'),
        'The real Node resolver must reach the second DATA entry before owner assertions');
      assert.deepEqual(physicalOwners(root).physical[name], [join(root, 'node_modules', name), duplicate].sort());
      assert.throws(() => inspectInstalled(root, CANDIDATE), /duplicate physical owner/);
    });
  }
}

function ordinaryBin(t) {
  const root = consumer(t), bin = join(root, 'node_modules/.bin');
  mkdirSync(bin);
  const owner = join(root, 'node_modules/fixture-tool');
  packageData(owner, 'fixture-tool');
  const target = join(owner, 'bin/command-data');
  mkdirSync(dirname(target));
  writeFileSync(target, 'Executable-file fixture DATA; never run.\n', { mode: 0o755 });
  return { root, bin, target };
}

test('ordinary direct .bin executable-file link into node_modules preserves the genuine singleton', t => {
  const { root, bin, target } = ordinaryBin(t);
  symlinkSync(target, join(bin, 'fixture-tool'), 'file');
  const result = inspectInstalled(root, CANDIDATE);
  for (const name of Object.keys(VERSIONS)) assert.deepEqual(result.physical[name], [join(root, 'node_modules', name)]);
});

test('a whole .bin directory symlink is not an admitted executable-file link', t => {
  const root = consumer(t), target = join(root, 'node_modules/bin-directory-data');
  packageData(target, 'bin-directory-data');
  symlinkSync(target, join(root, 'node_modules/.bin'), 'dir');
  assert.throws(() => physicalOwners(root), /unclassified links/);
});

test('a direct .bin link to an internal directory is refused', t => {
  const { root, bin } = ordinaryBin(t);
  symlinkSync(join(root, 'node_modules/foottrace'), join(bin, 'directory'), 'dir');
  assert.throws(() => physicalOwners(root), /unclassified links/);
});

for (const outside of ['elsewhere', 'node_modules-sibling']) {
  test(`a direct .bin file link outside node_modules (${outside}) is refused`, t => {
    const { root, bin } = ordinaryBin(t);
    const target = join(root, outside, 'command-data');
    mkdirSync(dirname(target)); writeFileSync(target, 'outside fixture DATA\n');
    symlinkSync(target, join(bin, 'outside'), 'file');
    assert.throws(() => physicalOwners(root), /unclassified links/);
  });
}

for (const below of ['hidden', '.hidden']) {
  test(`a nested .bin/${below} link is refused even when its final regular target is internal`, t => {
    const { root, bin, target } = ordinaryBin(t);
    mkdirSync(join(bin, below));
    symlinkSync(target, join(bin, below, 'unknown-link'), 'file');
    assert.throws(() => physicalOwners(root), /unclassified links/);
  });
}

test('traversing .bin does not turn a hidden nested node_modules package store into admitted DATA', t => {
  const { root, bin } = ordinaryBin(t);
  mkdirSync(join(bin, 'hidden/node_modules/.store'), { recursive: true });
  assert.throws(() => physicalOwners(root), /unclassified package stores/);
});

test('a linked node_modules root is not an ordinary inspected install tree', t => {
  const root = consumer(t), ordinary = join(root, 'ordinary-install-tree');
  // Rename only this fixture's owned tree; no installed package or runtime is loaded.
  renameSync(join(root, 'node_modules'), ordinary);
  symlinkSync(ordinary, join(root, 'node_modules'), 'dir');
  assert.throws(() => physicalOwners(root), /unclassified links/);
});

// A structural node_modules container may also be a resolver-reachable package.
// Its own identity must be judged before its descendants, never skipped by name.
for (const name of Object.keys(VERSIONS)) {
  for (const nested of [false, true]) {
    test(`${name}: the ${nested ? 'nested ' : ''}.bin container's OWN same-version package identity is counted`, t => {
      const root = consumer(t);
      inspectInstalled(root, CANDIDATE);
      const parent = nested ? join(root, 'node_modules/container-parent') : root;
      if (nested) packageData(parent, 'container-parent');
      const bin = join(parent, 'node_modules/.bin');
      packageData(bin, name);
      assert.equal(probe(parent, '.bin'), join(bin, 'entry.json'),
        'Real Node resolution must reach the second DATA package before owner assertions');
      const report = physicalOwners(root);
      assert.deepEqual(report.physical[name], [join(root, 'node_modules', name), bin].sort());
      assert.ok(report.nativePackages.some(row => row.path === bin && row.installName === '.bin'
        && row.name === name && row.version === VERSIONS[name]));
      assert.throws(() => inspectInstalled(root, CANDIDATE), /duplicate physical owner/);
    });
  }
  test(`${name}: a direct scope container's OWN manifest is not skipped before scoped children`, t => {
    const root = consumer(t), scope = join(root, 'node_modules/@container');
    inspectInstalled(root, CANDIDATE);
    packageData(scope, name);
    packageData(join(scope, 'ordinary-child'), '@container/ordinary-child');
    // This is an identity-census control, not a claim that the ESM bare-name
    // grammar accepts a scope with no package component.
    assert.deepEqual(physicalOwners(root).physical[name], [join(root, 'node_modules', name), scope].sort());
    assert.throws(() => inspectInstalled(root, CANDIDATE), /duplicate physical owner/);
  });
}

test('a classified non-native .bin container still preserves genuine immediate executable links', t => {
  const { root, bin, target } = ordinaryBin(t);
  packageData(bin, 'fixture-command-container');
  symlinkSync(target, join(bin, 'fixture-tool'), 'file');
  assert.equal(probe(root, '.bin'), join(bin, 'entry.json'));
  const result = inspectInstalled(root, CANDIDATE);
  for (const name of Object.keys(VERSIONS)) assert.deepEqual(result.physical[name], [join(root, 'node_modules', name)]);
});

for (const kind of ['malformed-json', 'missing-name', 'missing-native-version', 'manifest-directory', 'manifest-link']) {
  test(`a .bin container's own ${kind} identity refuses instead of proving absence`, t => {
    const { root, bin } = ordinaryBin(t);
    const manifest = join(bin, 'package.json');
    if (kind === 'manifest-directory') mkdirSync(manifest);
    else if (kind === 'manifest-link') symlinkSync(join(root, 'node_modules/foottrace/package.json'), manifest, 'file');
    else writeFileSync(manifest, kind === 'malformed-json' ? '{' : JSON.stringify(kind === 'missing-name'
      ? { version: '2.1.0' } : { name: 'foottrace' }));
    const failure = kind === 'malformed-json' ? SyntaxError : kind === 'missing-name' ? /Missing installed package identity/
      : kind === 'missing-native-version' ? /Missing native-owner version/ : /Unreadable installed package identity/;
    assert.throws(() => physicalOwners(root), failure);
  });
}
