import assert from 'node:assert/strict';

export const COHORT = Object.freeze({ footprintjs: '11.0.0', foottrace: '1.11.1' });

/** Ordinary npm package-owner facts, never entry-file equality across ESM/CJS conditions. */
export function assertCohort(facts, candidateVersion) {
  assert.equal(facts.versions['footprintjs-datapipeline'], candidateVersion, 'Installed candidate version drift');
  for (const [name, version] of Object.entries(COHORT)) {
    assert.equal(facts.versions[name], version, `${name}: wrong current cohort`);
    assert.deepEqual(facts.lockPaths[name], [`node_modules/${name}`], `${name}: nested lock owner`);
    assert.equal(facts.physical[name].length, 1, `${name}: duplicate physical owner, even if versions agree`);
    assert.equal(facts.root[name], facts.physical[name][0], `${name}: root is not the only physical owner`);
    assert.equal(facts.dp[name], facts.root[name], `${name}: candidate resolves another physical owner`);
  }
  return facts;
}
