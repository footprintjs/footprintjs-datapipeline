import assert from 'node:assert/strict';
import test from 'node:test';
import { assertCohort } from './graph.mjs';

// Inert path/version DATA only. No fake modules, package install, runtime import or native proof.
function facts() {
  return {
    versions: { 'footprintjs-datapipeline': 'candidate-source-version', footprintjs: '14.0.0', foottrace: '2.1.0' },
    root: { footprintjs: '/consumer/node_modules/footprintjs', foottrace: '/consumer/node_modules/foottrace' },
    dp: { footprintjs: '/consumer/node_modules/footprintjs', foottrace: '/consumer/node_modules/foottrace' },
    physical: { footprintjs: ['/consumer/node_modules/footprintjs'], foottrace: ['/consumer/node_modules/foottrace'] },
    lockPaths: { footprintjs: ['node_modules/footprintjs'], foottrace: ['node_modules/foottrace'] },
  };
}
test('admits only the explicit exact current cohort with one shared physical owner', () => {
  const f = facts(); assert.equal(assertCohort(f, 'candidate-source-version'), f);
});
for (const name of ['footprintjs', 'foottrace']) {
  test(`${name}: a hidden same-version copy is refused even when the lock and root/DP resolutions look clean`, () => {
    const f = facts(); f.physical[name].push(`/consumer/node_modules/unlisted/node_modules/${name}`);
    assert.throws(() => assertCohort(f, 'candidate-source-version'), /duplicate physical owner/);
  });
  test(`${name}: wrong version, another candidate owner and nested lock path each refuse`, () => {
    const version = facts(); version.versions[name] = '0.0.0';
    assert.throws(() => assertCohort(version, 'candidate-source-version'), /wrong current cohort/);
    const resolution = facts(); resolution.dp[name] = `/consumer/other/${name}`;
    assert.throws(() => assertCohort(resolution, 'candidate-source-version'), /another physical owner/);
    const lock = facts(); lock.lockPaths[name].push(`node_modules/dependency/node_modules/${name}`);
    assert.throws(() => assertCohort(lock, 'candidate-source-version'), /nested lock owner/);
  });
}
test('does not mistake an old candidate for the newly packed source', () => {
  assert.throws(() => assertCohort(facts(), 'another-source-version'), /candidate version drift/);
});

test('refuses each old engine line and an earlier trace even at the right owner path', () => {
  for (const version of ['11.0.0', '12.0.0', '13.0.0']) {
    const f = facts(); f.versions.footprintjs = version;
    assert.throws(() => assertCohort(f, 'candidate-source-version'), /footprintjs: wrong current cohort/);
  }
  for (const version of ['1.11.1', '2.0.0']) {
    const f = facts(); f.versions.foottrace = version;
    assert.throws(() => assertCohort(f, 'candidate-source-version'), /foottrace: wrong current cohort/);
  }
});
