import assert from 'node:assert/strict';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { runCurrentCohort } from './run.mjs';

/** Both ordinary consumers must execute the same packed source bytes. */
export async function runEngineCohorts(source, evidence, run = runCurrentCohort) {
  const completed = [];
  let failure;
  let sameArchive = false;
  try {
    await run(source, evidence, '11');
    completed.push('11');
    const next = join(evidence, 'engine-12');
    await run(source, next, '12');
    completed.push('12');
    const first = JSON.parse(readFileSync(join(evidence, 'candidate.json'), 'utf8'));
    const second = JSON.parse(readFileSync(join(next, 'candidate.json'), 'utf8'));
    assert.ok(first?.name === 'footprintjs-datapipeline' && typeof first.version === 'string'
      && /^\d+\.\d+\.\d+$/.test(first.version) && Number.isSafeInteger(first.bytes) && first.bytes > 0
      && typeof first.sha256 === 'string' && /^[a-f0-9]{64}$/.test(first.sha256), 'Invalid engine matrix candidate');
    for (const field of ['name', 'version', 'bytes', 'sha256']) {
      assert.equal(second[field], first[field], `Engine matrix candidate ${field} drift`);
    }
    sameArchive = true;
  } catch (error) { failure = error; }
  if (existsSync(evidence)) {
    try {
      writeFileSync(join(evidence, 'MATRIX.json'), JSON.stringify({
        status: failure ? 'FAILED' : 'PASSED', completed, sameArchive,
        profiles: ['11', '12'],
      }, null, 2) + '\n');
    } catch (error) {
      failure = failure ? new AggregateError([failure, error], 'Matrix proof and evidence write failed') : error;
    }
  }
  if (failure) throw failure;
}
