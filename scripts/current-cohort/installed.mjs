import assert from 'node:assert/strict';
import { lstatSync, readFileSync, realpathSync, readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { basename, dirname, isAbsolute, join, relative, sep } from 'node:path';
import { assertCohort, COHORT } from './graph.mjs';

const json = path => JSON.parse(readFileSync(path, 'utf8'));
function owner(entry, name) {
  let at = dirname(realpathSync(entry));
  for (let depth = 0; depth < 16; depth++) {
    try { if (json(join(at, 'package.json')).name === name) return realpathSync(at); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    const parent = dirname(at);
    assert.notEqual(parent, at, `No actual package owner for ${name}`);
    at = parent;
  }
  throw new Error(`Package-owner bound exceeded: ${name}`);
}

function admitExecutableLink(path, modulesRoot) {
  const target = realpathSync(path);
  const within = relative(modulesRoot, target);
  assert.ok(within !== '' && within !== '..' && !within.startsWith(`..${sep}`) && !isAbsolute(within),
    'Installed-tree inspection refuses unclassified links outside node_modules');
  assert.ok(lstatSync(target).isFile(), 'Installed-tree inspection refuses unclassified links to non-files');
}

/** Read-only bounded ordinary npm tree scan; unknown links/stores refuse, never count as no duplicates. */
export function physicalOwners(consumer) {
  const names = Object.keys(COHORT);
  const physical = Object.fromEntries(names.map(name => [name, []]));
  const nativePackages = [];
  const modules = join(consumer, 'node_modules');
  const root = lstatSync(modules);
  assert.ok(root.isDirectory() && !root.isSymbolicLink(), 'Installed-tree inspection refuses unclassified links at module roots');
  const modulesRoot = realpathSync(modules);
  const pending = [modules];
  let entries = 0;
  while (pending.length) {
    const directory = pending.pop();
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      assert.ok(++entries <= 100_000, 'Installed-tree inspection bound exceeded');
      const path = join(directory, entry.name);
      const direct = basename(directory) === 'node_modules';
      const scoped = basename(dirname(directory)) === 'node_modules' && basename(directory).startsWith('@');
      const binaryDirectory = basename(directory) === '.bin' && basename(dirname(directory)) === 'node_modules';
      const packageContainer = direct && (entry.name === '.bin' || entry.name.startsWith('@'));
      // Admit only npm's immediate executable-file links, not the whole directory.
      if (binaryDirectory && entry.isSymbolicLink()) {
        admitExecutableLink(path, modulesRoot);
        continue;
      }
      assert.ok(!entry.isSymbolicLink(), 'Installed-tree inspection refuses unclassified links');
      if (!entry.isDirectory()) continue;
      // A package may legitimately ship hidden assets. Only package-store positions have this refusal.
      if (direct || scoped) assert.ok(!entry.name.startsWith('.') || (direct && entry.name === '.bin'), 'Installed-tree inspection refuses unclassified package stores');
      if (direct || scoped) {
        const manifestPath = join(path, 'package.json');
        let stat;
        try { stat = lstatSync(manifestPath); }
        catch (error) { if (!packageContainer || error.code !== 'ENOENT') throw error; }
        // Containers may have no own package, but a present identity is never skipped.
        if (stat === undefined) { pending.push(path); continue; }
        assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.size <= 1_048_576, 'Unreadable installed package identity');
        const manifest = json(manifestPath);
        assert.ok(typeof manifest.name === 'string' && manifest.name.length > 0, 'Missing installed package identity');
        if (names.includes(entry.name)) assert.equal(manifest.name, entry.name, 'Native slot has another package identity');
        if (names.includes(manifest.name)) {
          assert.ok(typeof manifest.version === 'string' && manifest.version.length > 0, 'Missing native-owner version');
          const actual = realpathSync(path);
          physical[manifest.name].push(actual);
          nativePackages.push({ path: actual, installName: scoped ? `${basename(directory)}/${entry.name}` : entry.name,
            name: manifest.name, version: manifest.version });
        }
      }
      pending.push(path);
    }
  }
  for (const owners of Object.values(physical)) owners.sort();
  return { physical, nativePackages, inspectedEntries: entries };
}

export function inspectInstalled(consumer, candidateVersion, profile = '11') {
  const names = Object.keys(COHORT);
  const { physical, nativePackages, inspectedEntries } = physicalOwners(consumer);
  const rootRequire = createRequire(join(consumer, 'package.json'));
  // Datapipeline is ESM-only: do not invent a require export just to inspect its dependency context.
  const dpRoot = realpathSync(join(consumer, 'node_modules', 'footprintjs-datapipeline'));
  assert.equal(json(join(dpRoot, 'package.json')).name, 'footprintjs-datapipeline');
  const dpRequire = createRequire(join(dpRoot, 'package.json'));
  const root = Object.fromEntries(names.map(name => [name, owner(rootRequire.resolve(name), name)]));
  const dp = Object.fromEntries(names.map(name => [name, owner(dpRequire.resolve(name), name)]));
  const lock = json(join(consumer, 'package-lock.json'));
  const lockPaths = Object.fromEntries(names.map(name => [name, Object.keys(lock.packages)
    .filter(path => path.endsWith(`node_modules/${name}`)).sort()]));
  const versions = Object.fromEntries([...names, 'footprintjs-datapipeline'].map(name =>
    [name, json(join(consumer, 'node_modules', name, 'package.json')).version]));
  return assertCohort({ versions, root, dp, physical, lockPaths, nativePackages, inspectedEntries }, candidateVersion, profile);
}
