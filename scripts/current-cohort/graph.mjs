import assert from 'node:assert/strict';

export const COHORT = Object.freeze({ footprintjs: '11.0.0', foottrace: '1.11.1' });
const COHORTS = Object.freeze({
  '11': COHORT,
  '12': Object.freeze({ footprintjs: '12.0.0', foottrace: '1.11.1' }),
});

export function cohortFor(profile = '11') {
  assert.ok(typeof profile === 'string' && Object.hasOwn(COHORTS, profile), 'Unknown native cohort');
  return COHORTS[profile];
}

/** Ordinary npm package-owner facts, never entry-file equality across ESM/CJS conditions. */
export function assertCohort(facts, candidateVersion, profile = '11') {
  const cohort = cohortFor(profile);
  assert.equal(facts.versions['footprintjs-datapipeline'], candidateVersion, 'Installed candidate version drift');
  for (const [name, version] of Object.entries(cohort)) {
    assert.equal(facts.versions[name], version, `${name}: wrong current cohort`);
    assert.deepEqual(facts.lockPaths[name], [`node_modules/${name}`], `${name}: nested lock owner`);
    assert.equal(facts.physical[name].length, 1, `${name}: duplicate physical owner, even if versions agree`);
    assert.equal(facts.root[name], facts.physical[name][0], `${name}: root is not the only physical owner`);
    assert.equal(facts.dp[name], facts.root[name], `${name}: candidate resolves another physical owner`);
  }
  return facts;
}
