import { fileURLToPath } from 'node:url';
import { runEngineCohorts } from './current-cohort/matrix.mjs';

await runEngineCohorts(fileURLToPath(new URL('../', import.meta.url)), process.argv[2]);
