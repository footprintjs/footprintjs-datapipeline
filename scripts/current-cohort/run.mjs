import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFileSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { cohortFor } from './graph.mjs';
import { inspectInstalled } from './installed.mjs';

const json = path => JSON.parse(readFileSync(path, 'utf8'));
const ownFolder = fileURLToPath(new URL('.', import.meta.url));
const inside = (parent, child) => {
  const name = relative(parent, child);
  return name === '' || (!isAbsolute(name) && name !== '..' && !name.startsWith(`..${sep}`));
};

/** Hosted proof driver only. No publication or consumer/source manifest rewriting. */
export async function runCurrentCohort(sourceInput, evidenceInput, profile = '11') {
  const cohort = cohortFor(profile);
  assert.ok(typeof evidenceInput === 'string' && isAbsolute(evidenceInput), 'Pass one fresh absolute evidence directory');
  const source = realpathSync(sourceInput);
  const evidence = resolve(evidenceInput);
  mkdirSync(evidence); // Fails on existing output instead of mixing histories.
  const work = realpathSync(mkdtempSync(join(process.env.RUNNER_TEMP || tmpdir(), 'dp-current-cohort-')));
  assert.ok(!inside(source, work) && !inside(work, source), 'Consumer must be outside the source checkout');
  assert.ok(!inside(work, evidence) && !inside(evidence, work), 'Evidence must not include the install tree');
  const userConfig = join(work, 'empty-user.npmrc');
  const globalConfig = join(work, 'empty-global.npmrc');
  writeFileSync(userConfig, '', { flag: 'wx', mode: 0o600 });
  writeFileSync(globalConfig, '', { flag: 'wx', mode: 0o600 });
  const environment = { ...process.env, NPM_CONFIG_USERCONFIG: userConfig,
    NPM_CONFIG_GLOBALCONFIG: globalConfig, NPM_CONFIG_REGISTRY: 'https://registry.npmjs.org/',
    NPM_CONFIG_FETCH_RETRIES: '0' };
  const errors = [];
  let count = 0;
  let timedOut = false;
  const save = (name, value) => writeFileSync(join(evidence, name), JSON.stringify(value, null, 2) + '\n');
  save('cohort.json', { profile, dependencies: cohort });
  const command = (program, args, cwd) => {
    const id = String(++count).padStart(2, '0');
    const result = spawnSync(program, args, { cwd, env: environment, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024,
      timeout: 240_000, killSignal: 'SIGKILL' });
    writeFileSync(join(evidence, `${id}-stdout.log`), result.stdout || '');
    writeFileSync(join(evidence, `${id}-stderr.log`), result.stderr || '');
    save(`${id}-command.json`, { program, args, cwd, status: result.status, signal: result.signal,
      ...(result.error ? { error: { code: result.error.code, message: result.error.message } } : {}) });
    if (result.error?.code === 'ETIMEDOUT') timedOut = true;
    assert.ok(!result.error && result.status === 0 && result.signal === null, `Current-cohort command ${id} failed; see retained command/log files`);
    return result.stdout;
  };
  try {
    const manifest = json(join(source, 'package.json'));
    assert.equal(manifest.name, 'footprintjs-datapipeline');
    const pack = join(work, 'pack'); mkdirSync(pack);
    const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
    command(npm, ['pack', '--pack-destination', pack], source); // Normal own prepack/build, never --ignore-scripts.
    const entries = readdirSync(pack);
    assert.equal(entries.length, 1, 'Own pack must produce exactly one candidate');
    assert.equal(basename(entries[0]), entries[0]); assert.match(entries[0], /\.tgz$/);
    const archive = join(pack, entries[0]);
    const stat = lstatSync(archive);
    assert.ok(stat.isFile() && !stat.isSymbolicLink() && stat.nlink === 1 && stat.size > 0 && stat.size <= 64 * 1024 * 1024);
    const bytes = readFileSync(archive);
    const archiveSha256 = createHash('sha256').update(bytes).digest('hex');
    copyFileSync(archive, join(evidence, 'candidate.tgz'));
    save('candidate.json', { name: manifest.name, version: manifest.version, bytes: bytes.length,
      sha256: archiveSha256 });
    const consumer = join(work, 'consumer'); mkdirSync(consumer);
    const consumerManifest = { name: 'datapipeline-current-cohort-check', version: '0.0.0', private: true, type: 'module',
      dependencies: { 'footprintjs-datapipeline': `file:${archive}`, ...cohort },
      devDependencies: { typescript: '5.9.3', '@types/node': '22.19.0' } };
    writeFileSync(join(consumer, 'package.json'), JSON.stringify(consumerManifest, null, 2) + '\n');
    writeFileSync(join(consumer, 'tsconfig.json'), JSON.stringify({ compilerOptions: {
      target: 'ES2023', module: 'NodeNext', moduleResolution: 'NodeNext', strict: true,
      skipLibCheck: false, types: ['node'], typeRoots: ['./node_modules/@types'], outDir: 'dist',
    }, include: ['consumer.ts'] }, null, 2) + '\n');
    copyFileSync(join(ownFolder, 'consumer.ts'), join(consumer, 'consumer.ts'));
    command(npm, ['install', '--strict-peer-deps', '--no-audit', '--no-fund'], consumer);
    const lockSha256 = createHash('sha256').update(readFileSync(join(consumer, 'package-lock.json'))).digest('hex');
    copyFileSync(join(consumer, 'package-lock.json'), join(evidence, 'consumer-lock.json'));
    copyFileSync(join(consumer, 'package.json'), join(evidence, 'consumer-package.json'));
    command(npm, ['ls', '--all', '--json'], consumer);
    save('installed-before.json', inspectInstalled(consumer, manifest.version, profile));
    command('python3', [join(ownFolder, 'archive_payload.py'), archive, consumer,
      join(evidence, 'archive-before.json'), manifest.version, archiveSha256, lockSha256], consumer);
    command(process.execPath, [join(consumer, 'node_modules/typescript/bin/tsc'), '--project', join(consumer, 'tsconfig.json')], consumer);
    command(process.execPath, [join(consumer, 'dist/consumer.js')], consumer);
    command('python3', [join(ownFolder, 'archive_payload.py'), archive, consumer,
      join(evidence, 'archive-after.json'), manifest.version, archiveSha256, lockSha256], consumer);
    save('installed-after.json', inspectInstalled(consumer, manifest.version, profile));
  } catch (error) { errors.push(error); }
  // Do not assert, throw or write evidence from a finally block that could hide the primary error.
  // A timed-out hosted job owns process-tree teardown; this driver does not claim descendant cleanup.
  if (!timedOut) {
    try { rmSync(work, { recursive: true, force: true }); } catch (error) { errors.push(error); }
  }
  save('RESULT.json', { status: errors.length ? 'FAILED' : 'PASSED', node: process.version, profile,
    commands: count, work, cleanup: timedOut ? 'host-teardown-required' : 'owned-directory-removal-attempted',
    errors: errors.map(error => ({ name: error?.name, message: error?.message })) });
  if (errors.length) throw new AggregateError(errors, 'Current-cohort packed check failed; see evidence');
}
