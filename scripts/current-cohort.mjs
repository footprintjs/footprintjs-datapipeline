import { fileURLToPath } from 'node:url';
import { runCurrentCohort } from './current-cohort/run.mjs';

await runCurrentCohort(fileURLToPath(new URL('../', import.meta.url)), process.argv[2]);
