// Parsed actual workflow bodies with finite inert npm/curl ports.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

// Fixed repository sources; no environment-selected workflow path.
const PUBLISH = fileURLToPath(new URL('../.github/workflows/publish.yml', import.meta.url));
const CI = fileURLToPath(new URL('../.github/workflows/ci.yml', import.meta.url));
const CHECKOUT = 'actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683';
const SETUP = 'actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020';
const UPLOAD = 'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02';
const DOWNLOAD = 'actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093';
const SOURCE_INPUT = "${{ github.event_name == 'release' && github.sha || '' }}";
const TESTED_NAME = 'dp-tested-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}';
const clone = value => structuredClone(value);

function yaml(path) {
  assert.ok(existsSync(path), 'The release source document must exist');
  // Ruby/Psych is an explicitly required system tool, not an undeclared npm import.
  // Quoted "on" in both source files avoids YAML1.1's implicit boolean-key rule.
  const result = spawnSync('ruby', ['-rjson', '-ryaml', '-e',
    'puts JSON.generate(YAML.safe_load(File.read(ARGV.fetch(0)), permitted_classes: [], permitted_symbols: [], aliases: false))', path],
  { encoding: 'utf8', timeout: 5_000, maxBuffer: 1024 * 1024 });
  assert.equal(result.error, undefined, 'Ruby/Psych must be available before guard controls run');
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

function step(job, name) {
  const matches = job.steps.filter(row => row.name === name);
  assert.equal(matches.length, 1, `Exactly one ${name} step is required`);
  return matches[0];
}

function noSuppression(job) {
  assert.equal(Object.hasOwn(job, 'continue-on-error'), false);
  for (const row of job.steps || []) {
    assert.equal(Object.hasOwn(row, 'continue-on-error'), false);
    if (Object.hasOwn(row, 'if')) assert.equal(row.if, 'always()');
  }
}

function policy(publish, ci) {
  assert.deepEqual(publish.on, { release: { types: ['published'] } });
  assert.deepEqual(publish.permissions, { contents: 'read' });
  assert.equal(publish.concurrency['cancel-in-progress'], false);
  assert.deepEqual(Object.keys(publish.jobs).sort(), ['candidate', 'gates', 'publish', 'verify']);
  const jobs = publish.jobs;
  for (const [name, job] of Object.entries(jobs)) {
    noSuppression(job);
    if (name !== 'gates') assert.equal(job['runs-on'], 'ubuntu-24.04');
    if (name !== 'verify') assert.equal(Object.hasOwn(job, 'if'), false);
    if (name !== 'publish' && job.permissions) assert.deepEqual(job.permissions, { contents: 'read' });
  }
  assert.equal(jobs.verify['timeout-minutes'], 20, 'Read-only wait plus artifact retention must fit');
  assert.equal(jobs.gates.uses, './.github/workflows/ci.yml');
  assert.equal(jobs.gates.secrets, undefined);
  assert.equal(jobs.candidate.needs, 'gates');
  assert.deepEqual(jobs.publish.needs, ['gates', 'candidate']);
  assert.deepEqual(jobs.publish.permissions, { contents: 'read', 'id-token': 'write' });
  assert.deepEqual(jobs.verify.permissions, { contents: 'read' });
  assert.deepEqual(jobs.verify.needs, ['candidate', 'publish']);
  assert.equal(jobs.verify.if, "${{ always() && needs.candidate.result == 'success' && (needs.publish.result == 'success' || needs.publish.result == 'failure') }}");
  assert.equal(jobs.candidate.steps[0].uses, CHECKOUT);
  assert.equal(jobs.candidate.steps[0].with.ref, '${{ github.sha }}');
  const intake = jobs.candidate.steps.find(row => row.uses === DOWNLOAD);
  assert.equal(intake.with.name, TESTED_NAME);
  const bind = step(jobs.candidate, 'Bind the exact tested candidate to the release commit');
  assert.ok(bind.run.includes('test "$(git rev-parse HEAD)" = "$GITHUB_SHA"'));
  assert.ok(bind.run.includes('test "$(git rev-parse --verify "refs/tags/$RELEASE_TAG^{commit}")" = "$GITHUB_SHA"'));
  const setup = jobs.publish.steps.find(row => row.uses === SETUP);
  assert.deepEqual(setup.with, { 'node-version': '24.21.0' });
  assert.equal(jobs.publish.steps.length, 3, 'No source checkout/install step in publisher');
  const download = jobs.publish.steps.find(row => row.uses === DOWNLOAD);
  assert.deepEqual(download.with, {
    'artifact-ids': '${{ needs.candidate.outputs.artifact }}',
    path: '${{ runner.temp }}/dp-incoming', 'merge-multiple': true,
  });
  const submit = step(jobs.publish, 'Admit and submit the one bound archive');
  assert.equal(submit.env.NODE_AUTH_TOKEN, '');
  assert.equal(submit.env.NPM_TOKEN, '');
  assert.equal(submit.env.NPM_CONFIG_PROVENANCE, 'false');
  assert.equal(/\bnpm\s+(pack|install|ci)\b/.test(submit.run), false);
  const lines = submit.run.split('\n').filter(line => line.trim().startsWith('npm publish'));
  assert.deepEqual(lines.map(line => line.trim()), [
    'npm publish "$ARCHIVE" --ignore-scripts --access public --registry=https://registry.npmjs.org',
  ]);
  assert.deepEqual(ci.on, { push: null, pull_request: null, workflow_call: null });
  assert.deepEqual(ci.permissions, { contents: 'read' });
  assert.equal(ci.jobs['file-size'].uses,
    'footprintjs/ci/.github/workflows/file-size.yml@378ab4e25a73620619f423bf661c292fcb6de538');
  assert.equal(ci.jobs['file-size'].with.tool_sha, '378ab4e25a73620619f423bf661c292fcb6de538');
  assert.equal(ci.jobs['file-size'].with.source_sha, SOURCE_INPUT);
  assert.deepEqual(Object.keys(ci.jobs['file-size'].with).sort(), ['source_sha', 'tool_sha']);
  assert.equal(ci.jobs['minimum-runtime'].steps[1].with['node-version'], '22.13.0');
  assert.ok(ci.jobs['minimum-runtime'].steps.some(row => row.run === 'npx vitest run test/repository-clock-min-runtime.test.ts'));
  const check = ci.jobs.check;
  assert.equal(check['runs-on'], 'ubuntu-24.04');
  assert.equal(ci.jobs['minimum-runtime']['runs-on'], 'ubuntu-24.04');
  assert.deepEqual(check.strategy.matrix.node, [22, 24]);
  assert.equal(check.strategy['fail-fast'], false);
  assert.equal(check.services.influxdb.image, 'influxdb:2.7.12');
  assert.equal(check.env.INFLUX_TEST_REQUIRED, '1');
  for (const job of [check, ci.jobs['minimum-runtime']]) {
    assert.equal(Object.hasOwn(job, 'if'), false);
    assert.equal(Object.hasOwn(job, 'continue-on-error'), false);
    assert.equal(job.steps[0].uses, CHECKOUT);
    assert.equal(job.steps[0].with.ref, '${{ github.sha }}');
    assert.equal(job.steps[0].with['persist-credentials'], false);
  }
  for (const command of ['npm ci', 'npm run check', 'npm run example', 'npm pack --dry-run',
    'node scripts/current-cohort.mjs "$RUNNER_TEMP/datapipeline-current-cohort-evidence"']) {
    const selected = check.steps.filter(row => row.run === command);
    assert.equal(selected.length, 1, `Current required command stays active: ${command}`);
    assert.equal(selected[0].if, undefined);
    assert.equal(selected[0]['continue-on-error'], undefined);
  }
  const tested = step(check, 'Preserve the release-tested Node24 candidate');
  assert.equal(tested.uses, UPLOAD);
  assert.equal(tested.if, "${{ github.event_name == 'release' && matrix.node == 24 }}");
  assert.equal(tested.with.name, TESTED_NAME);
  assert.equal(tested.with['if-no-files-found'], 'error');
  assert.ok(tested.with.path.includes('candidate.tgz\n'));
  assert.ok(tested.with.path.trim().endsWith('candidate.json'));
  assert.ok(check.steps.indexOf(tested) > check.steps.indexOf(step(check, 'Check actual packed current cohort')));
}

const npmStub = String.raw`
const fs=require('node:fs'),path=require('node:path');
const args=process.argv.slice(2),e=process.env;
fs.appendFileSync(e.CALLS,JSON.stringify({tool:'npm',args,cwd:process.cwd()})+'\n');
if(args.length===1&&args[0]==='--version'){console.log(e.STUB_NPM_VERSION||'11.19.0');process.exit(0);}
if(JSON.stringify(args)!==JSON.stringify(['publish',e.ARCHIVE,'--ignore-scripts','--access','public','--registry=https://registry.npmjs.org']))process.exit(81);
if(process.cwd()!==e.PUBLISH_DIRECTORY||!path.isAbsolute(args[1])||e.NODE_AUTH_TOKEN!==''||e.NPM_TOKEN!==''||e.NPM_CONFIG_PROVENANCE!=='false')process.exit(82);
for(const key of ['NPM_CONFIG_USERCONFIG','NPM_CONFIG_GLOBALCONFIG']){
 if(path.dirname(e[key])!==process.cwd()||fs.readFileSync(e[key],'utf8')!=='')process.exit(83);
}
process.exit(Number(e.STUB_NPM_EXIT||0));
`;
const curlStub = String.raw`
const fs=require('node:fs'),path=require('node:path'),e=process.env,args=process.argv.slice(2),url=args.at(-1);
function refuse(message){console.error('INERT_STUB: '+message);process.exit(87);}
const option=(long,short)=>{const at=args.indexOf(long)>=0?args.indexOf(long):args.indexOf(short);return at<0?undefined:args[at+1];};
if(args.includes('--location')||args.includes('-L')||args.some(x=>x.startsWith('Authorization:')))refuse('unexpected transport');
let file,key;
if(url==='https://registry.npmjs.org/footprintjs-datapipeline/0.11.0'){file=e.STUB_METADATA;key='METADATA';}
else if(url==='https://registry.npmjs.org/footprintjs-datapipeline/-/footprintjs-datapipeline-0.11.0.tgz'){file=e.STUB_TAR;key='TAR';}
else refuse('unexpected URL');
const maxTime=Number(option('--max-time','-m')),limit=Number(option('--max-filesize','unused'));
if(!args.includes('--fail')||!args.includes('--proto')||!Number.isFinite(maxTime)||maxTime<=0||!Number.isFinite(limit)||limit<=0)refuse('missing finite curl bounds');
const output=option('--output','-o'),writeOut=option('--write-out','-w');
if(writeOut!==undefined&&writeOut!=='%{http_code}')refuse('unsupported write-out');
if(output!==undefined){const rel=path.relative(e.INERT_ROOT,path.resolve(output));if(rel===''||rel==='..'||rel.startsWith('..'+path.sep)||path.isAbsolute(rel))refuse('foreign output');}
const cursors=JSON.parse(fs.readFileSync(e.STUB_CURSORS,'utf8')),index=cursors[key]||0;
cursors[key]=index+1;fs.writeFileSync(e.STUB_CURSORS,JSON.stringify(cursors));
const legacy=Number(e['STUB_'+key+'_EXIT']||0);
let row={status:legacy?0:200,exit:legacy,elapsed:0};
if(e['STUB_'+key+'_PLAN']){const plan=JSON.parse(e['STUB_'+key+'_PLAN']);row=plan.rows[index]||(plan.repeatLast?plan.rows.at(-1):undefined);if(!row)refuse('response plan exhausted');}
const status=Number(row.status),code=Number(row.exit),elapsed=Number(row.elapsed||0),at=Number(fs.readFileSync(e.STUB_CLOCK,'utf8'));
if(!Number.isInteger(status)||!(status===0||(status>=100&&status<=599))||!Number.isInteger(code)||code<0||code>255||!Number.isInteger(elapsed)||elapsed<0||elapsed>maxTime||!Number.isInteger(at)||at<0)refuse('invalid scripted response');
fs.appendFileSync(e.CALLS,JSON.stringify({tool:'curl',args,cwd:process.cwd(),status,exit:code,at,maxTime,elapsed})+'\n');
fs.writeFileSync(e.STUB_CLOCK,String(at+elapsed)+'\n');
if(code===0){const b=fs.readFileSync(file);if(b.length>limit)refuse('scripted body exceeds declared limit');if(output!==undefined)fs.writeFileSync(output,b);else process.stdout.write(b);}
if(writeOut!==undefined)process.stdout.write(String(status).padStart(3,'0'));
process.exit(code);
`;
// Only these test-owned integer clock ports advance time; no real sleep occurs.
const dateStub = `#!/bin/sh
set -eu
[ "$#" -eq 1 ] && [ "$1" = '+%s' ] || { printf 'INERT_STUB: unsupported date\\n' >&2; exit 87; }
IFS= read -r now < "$STUB_CLOCK"
case "$now" in ''|*[!0-9]*) printf 'INERT_STUB: invalid clock\\n' >&2; exit 87;; esac
printf '{"tool":"date","at":%s}\\n' "$now" >> "$CLOCK_CALLS"
printf '%s\\n' "$now"
`;
const sleepStub = `#!/bin/sh
set -eu
[ "$#" -eq 1 ] || { printf 'INERT_STUB: unsupported sleep\\n' >&2; exit 87; }
case "$1" in ''|*[!0-9]*) printf 'INERT_STUB: invalid sleep\\n' >&2; exit 87;; esac
IFS= read -r now < "$STUB_CLOCK"
case "$now" in ''|*[!0-9]*) printf 'INERT_STUB: invalid clock\\n' >&2; exit 87;; esac
printf '{"tool":"sleep","at":%s,"seconds":%s}\\n' "$now" "$1" >> "$CLOCK_CALLS"
printf '%s\\n' "$((now + $1))" > "$STUB_CLOCK"
`;

function fixture(t) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'dp-route-inert-')));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const name of ['bin', 'home', 'incoming']) mkdirSync(join(root, name));
  const calls = join(root, 'calls.jsonl');
  writeFileSync(calls, '');
  const clock = join(root, 'clock'), clockCalls = join(root, 'clock-calls.jsonl'), cursors = join(root, 'curl-cursors.json');
  writeFileSync(clock, '0\n'); writeFileSync(clockCalls, ''); writeFileSync(cursors, '{}');
  for (const [name, source] of [['date', dateStub], ['sleep', sleepStub]]) writeFileSync(join(root, 'bin', name), source, { mode: 0o755 });
  for (const [name, source] of [['npm', npmStub], ['curl', curlStub]]) {
    writeFileSync(join(root, `${name}.cjs`), source);
    writeFileSync(join(root, 'bin', name), `#!/bin/sh\nexec "$REAL_NODE" "$STUB_${name.toUpperCase()}_HELPER" "$@"\n`, { mode: 0o755 });
  }
  // These are inert readback bytes, not a substitute for an actual npm archive.
  const bytes = Buffer.from('finite registry tar DATA; no module is imported');
  const hash = createHash('sha256').update(bytes).digest('hex');
  const integrity = `sha512-${createHash('sha512').update(bytes).digest('base64')}`;
  const archive = join(root, 'incoming', 'package.tgz');
  const metadata = join(root, 'metadata.json'), tar = join(root, 'registry.tgz');
  writeFileSync(archive, bytes); writeFileSync(tar, bytes);
  const registry = { name: 'footprintjs-datapipeline', version: '0.11.0', dist: {
    tarball: 'https://registry.npmjs.org/footprintjs-datapipeline/-/footprintjs-datapipeline-0.11.0.tgz', integrity,
  } };
  writeFileSync(metadata, JSON.stringify(registry));
  const env = {
    PATH: `${join(root, 'bin')}${delimiter}${process.env.PATH || ''}`,
    HOME: process.env.HOME, TMPDIR: root, REAL_NODE: process.execPath,
    STUB_NPM_HELPER: join(root, 'npm.cjs'), STUB_CURL_HELPER: join(root, 'curl.cjs'), CALLS: calls,
    STUB_METADATA: metadata, STUB_TAR: tar, ARCHIVE: archive,
    INERT_ROOT: root, STUB_CLOCK: clock, CLOCK_CALLS: clockCalls, STUB_CURSORS: cursors,
    PUBLISH_DIRECTORY: join(root, 'publish'), EVIDENCE: join(root, 'evidence'),
    NODE_AUTH_TOKEN: '', NPM_TOKEN: '', NODE_OPTIONS: '', NODE_PATH: '',
    NPM_CONFIG_PROVENANCE: 'false', NPM_CONFIG_FETCH_RETRIES: '0',
    EXPECTED_SHA256: hash, EXPECTED_INTEGRITY: integrity, VERSION: '0.11.0',
    GITHUB_SHA: 'a'.repeat(40), EXPECTED_SOURCE: 'a'.repeat(40),
    GITHUB_REF_NAME: 'v0.11.0', EXPECTED_TAG: 'v0.11.0',
    GITHUB_RUN_ID: '123', EXPECTED_RUN: '123', GITHUB_RUN_ATTEMPT: '2', EXPECTED_ATTEMPT: '2',
  };
  const rows = file => readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  return { root, env, archive, metadata, tar, registry, calls: () => rows(calls),
    clock: () => Number(readFileSync(clock, 'utf8')), sleeps: () => rows(clockCalls).filter(row => row.tool === 'sleep') };
}

function body(kind) {
  const source = yaml(PUBLISH);
  return step(source.jobs[kind], kind === 'publish'
    ? 'Admit and submit the one bound archive'
    : 'Read back public registry metadata and the actual tarball').run;
}

function run(script, f) {
  const result = spawnSync('bash', ['--noprofile', '--norc', '-c', script],
    { cwd: f.root, env: f.env, encoding: 'utf8', timeout: 5_000, maxBuffer: 1024 * 1024, killSignal: 'SIGKILL' });
  assert.equal(result.error, undefined, 'Inert body must finish under its test bound');
  assert.equal(result.signal, null, 'No timeout/termination can count as a guard refusal');
  assert.doesNotMatch(result.stderr, /INERT_STUB:/, 'A broken inert port is not a workflow refusal');
  return result;
}
const submits = f => f.calls().filter(row => row.tool === 'npm' && row.args[0] === 'publish');

test('actual workflow pair keeps complete gates, release-only source binding and the one fixed producer', () => {
  policy(yaml(PUBLISH), yaml(CI));
});

test('structural guard mutations are rejected against a passing parsed-source control', () => {
  const p = yaml(PUBLISH), c = yaml(CI); policy(p, c);
  const mutations = [
    ['dispatch', x => { x.on.workflow_dispatch = {}; }],
    ['floating verify runner', x => { x.jobs.verify['runs-on'] = 'ubuntu-latest'; }],
    ['short verify budget', x => { x.jobs.verify['timeout-minutes'] = 5; }],
    ['global OIDC', x => { x.permissions['id-token'] = 'write'; }],
    ['candidate OIDC', x => { x.jobs.candidate.permissions = { 'id-token': 'write' }; }],
    ['skip all gates', x => { x.jobs.gates.if = false; }],
    ['ignore failure', x => { x.jobs.publish['continue-on-error'] = true; }],
    ['missing gates', x => { x.jobs.publish.needs = ['candidate']; }],
    ['wildcard download', x => { x.jobs.publish.steps[1].with = { pattern: '*' }; }],
    ['invalid v4 input', x => { x.jobs.publish.steps[0].with['package-manager-cache'] = false; }],
  ];
  for (const [name, mutate] of mutations) {
    const changed = clone(p); mutate(changed);
    assert.throws(() => policy(changed, c), { name: 'AssertionError' }, name);
  }
  const ciMutations = [
    ['missing release SHA', x => { delete x.jobs['file-size'].with.source_sha; }],
    ['blanket baseline change', x => { x.jobs['file-size'].with.source_sha = '${{ github.sha }}'; }],
    ['one Node only', x => { x.jobs.check.strategy.matrix.node = [24]; }],
    ['floating called runner', x => { x.jobs.check['runs-on'] = 'ubuntu-latest'; }],
    ['skip full', x => { x.jobs.check.steps.find(s => s.run === 'npm run check').if = false; }],
    ['weaken full', x => { x.jobs.check.steps.find(s => s.run === 'npm run check')['continue-on-error'] = true; }],
    ['wrong producer', x => { step(x.jobs.check, 'Preserve the release-tested Node24 candidate').if = "${{ matrix.node == 22 }}"; }],
    ['cross-attempt name', x => { step(x.jobs.check, 'Preserve the release-tested Node24 candidate').with.name = 'candidate'; }],
    ['skip Influx', x => { x.jobs.check.env.INFLUX_TEST_REQUIRED = '0'; }],
  ];
  for (const [name, mutate] of ciMutations) {
    const changed = clone(c); mutate(changed);
    assert.throws(() => policy(p, changed), { name: 'AssertionError' }, name);
  }
});

test('actual publisher body submits exactly once from fresh cwd/configs with the bound absolute tar', t => {
  const f = fixture(t), result = run(body('publish'), f);
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(submits(f).map(row => ({ args: row.args, cwd: row.cwd })), [{ args: [
    'publish', f.archive, '--ignore-scripts', '--access', 'public', '--registry=https://registry.npmjs.org',
  ], cwd: f.env.PUBLISH_DIRECTORY }]);
  assert.deepEqual(f.calls().map(row => row.args[0]), ['--version', 'publish']);
});

for (const [field, value] of [['EXPECTED_SOURCE', 'b'.repeat(40)], ['EXPECTED_TAG', 'v0.11.9'],
  ['EXPECTED_RUN', '124'], ['EXPECTED_ATTEMPT', '3'], ['EXPECTED_SHA256', 'not-a-digest'], ['STUB_NPM_VERSION', '11.18.0']]) {
  test(`actual publisher refuses ${field} drift before submission`, t => {
    const f = fixture(t); f.env[field] = value;
    const result = run(body('publish'), f);
    assert.notEqual(result.status, 0); assert.equal(submits(f).length, 0);
  });
}

test('actual publisher refuses substituted bytes under the same immutable expected identity', t => {
  const f = fixture(t); writeFileSync(f.archive, 'different bytes, same path');
  const result = run(body('publish'), f);
  assert.notEqual(result.status, 0); assert.equal(submits(f).length, 0);
  assert.match(result.stderr, /archive-drift/);
});

test('actual publisher refuses extra intake config and never reads it as npm configuration', t => {
  const f = fixture(t); writeFileSync(join(f.root, 'incoming', '.npmrc'), 'inert extra DATA');
  const result = run(body('publish'), f);
  assert.notEqual(result.status, 0); assert.equal(submits(f).length, 0);
  assert.match(result.stderr, /archive-intake/);
});

test('actual publisher propagates auth/submission failure with no fallback or second submit', t => {
  const f = fixture(t); f.env.STUB_NPM_EXIT = '42';
  const result = run(body('publish'), f);
  assert.equal(result.status, 42); assert.equal(submits(f).length, 1);
  assert.deepEqual(f.calls().map(row => row.args[0]), ['--version', 'publish']);
});

test('actual readback checks metadata and actual bytes before emitting PASSED', t => {
  const f = fixture(t), result = run(body('verify'), f);
  assert.equal(result.status, 0, result.stderr);
  const receipt = JSON.parse(readFileSync(join(f.env.EVIDENCE, 'RESULT.json'), 'utf8'));
  assert.equal(receipt.status, 'PASSED'); assert.equal(receipt.sha256, f.env.EXPECTED_SHA256);
  assert.equal(receipt.integrity, f.env.EXPECTED_INTEGRITY);
  assert.equal(f.calls().filter(row => row.tool === 'curl').length, 2);
  assert.equal(submits(f).length, 0);
});

for (const [name, mutate] of [
  ['name', m => { m.name = 'other-package'; }],
  ['version', m => { m.version = '0.11.1'; }],
  ['host', m => { m.dist.tarball = 'https://elsewhere.invalid/package.tgz'; }],
  ['path', m => { m.dist.tarball += '?other'; }],
  ['integrity', m => { m.dist.integrity = 'sha512-other'; }],
]) {
  test(`actual readback refuses metadata ${name} drift before tar fetch`, t => {
    const f = fixture(t); mutate(f.registry); writeFileSync(f.metadata, JSON.stringify(f.registry));
    const result = run(body('verify'), f);
    assert.notEqual(result.status, 0); assert.equal(f.calls().length, 1);
    assert.equal(existsSync(join(f.env.EVIDENCE, 'RESULT.json')), false);
  });
}

test('actual readback refuses duplicate registry keys', t => {
  const f = fixture(t); writeFileSync(f.metadata, '{"name":"one","name":"two"}');
  const result = run(body('verify'), f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /duplicate-registry-key/);
  assert.equal(f.calls().length, 1);
});

test('actual readback refuses different tar bytes despite valid metadata', t => {
  const f = fixture(t); writeFileSync(f.tar, 'changed after metadata');
  const result = run(body('verify'), f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /registry-bytes-drift/);
  assert.equal(existsSync(join(f.env.EVIDENCE, 'RESULT.json')), false);
});

for (const [field, code, expectedCalls] of [['STUB_METADATA_EXIT', '22', 1], ['STUB_METADATA_EXIT', '28', 1], ['STUB_TAR_EXIT', '22', 2]]) {
  test(`actual readback propagates ${field}=${code} with no publish/fallback`, t => {
    const f = fixture(t); f.env[field] = code;
    const result = run(body('verify'), f);
    assert.equal(result.status, Number(code)); assert.equal(f.calls().length, expectedCalls);
    assert.equal(submits(f).length, 0);
    assert.equal(existsSync(join(f.env.EVIDENCE, 'RESULT.json')), false);
  });
}

test('copied-body witness: removing digest refusal exposes a submission the original body stops', t => {
  const before = fixture(t); writeFileSync(before.archive, 'same substituted DATA');
  const source = body('publish'), needle = "require(hashlib.sha256(p.read_bytes()).hexdigest() == e['EXPECTED_SHA256'], 'archive-drift')";
  assert.equal(source.split(needle).length, 2, 'Fault must target one actual guard');
  assert.notEqual(run(source, before).status, 0); assert.equal(submits(before).length, 0);
  const fault = fixture(t); writeFileSync(fault.archive, 'same substituted DATA');
  const result = run(source.replace(needle, "require(True, 'archive-drift')"), fault);
  assert.equal(result.status, 0, result.stderr); assert.equal(submits(fault).length, 1);
});

test('copied-body witness: removing readback byte comparison falsely emits PASSED for corrupt bytes', t => {
  const before = fixture(t); writeFileSync(before.tar, 'substituted registry DATA');
  const source = body('verify');
  const needle = "if digest != e['EXPECTED_SHA256'] or integrity != e['EXPECTED_INTEGRITY']:";
  assert.equal(source.split(needle).length, 2, 'Fault must target one actual readback guard');
  assert.notEqual(run(source, before).status, 0);
  assert.equal(existsSync(join(before.env.EVIDENCE, 'RESULT.json')), false);
  const fault = fixture(t); writeFileSync(fault.tar, 'substituted registry DATA');
  const result = run(source.replace(needle, 'if False:'), fault);
  assert.equal(result.status, 0, result.stderr);
  const receipt = JSON.parse(readFileSync(join(fault.env.EVIDENCE, 'RESULT.json'), 'utf8'));
  assert.equal(receipt.status, 'PASSED');
  assert.notEqual(receipt.sha256, fault.env.EXPECTED_SHA256);
});

// Plans are external-response DATA; every assertion executes the actual verify body.
const notFound = { status: 404, exit: 22 }, available = { status: 200, exit: 0 };
const plan = (f, phase, rows, repeatLast = false) => { f.env[`STUB_${phase}_PLAN`] = JSON.stringify({ rows, repeatLast }); };
const reads = f => f.calls().filter(row => row.tool === 'curl');
function noPublish(f) { assert.equal(submits(f).length, 0); assert.ok(f.calls().every(row => row.tool === 'curl')); }
function noReceipt(f) { assert.equal(existsSync(join(f.env.EVIDENCE, 'RESULT.json')), false); noPublish(f); }

for (const phase of ['METADATA', 'TAR']) {
  test(`actual verify waits for ${phase} 404 then reads the same candidate once it is available`, t => {
    const f = fixture(t); plan(f, phase, [notFound, available]);
    const result = run(body('verify'), f);
    assert.equal(result.status, 0, result.stderr); noPublish(f);
    assert.equal(reads(f).length, 3); assert.deepEqual(f.sleeps().map(row => row.seconds), [30]);
    assert.equal(f.clock(), 30); assert.equal(JSON.parse(readFileSync(join(f.env.EVIDENCE, 'RESULT.json'))).status, 'PASSED');
    assert.ok(reads(f).every(row => row.maxTime <= (row.args.at(-1).endsWith('.tgz') ? 60 : 30)));
  });
}

test('actual verify stops at one 900-second deadline with no read at the deadline', t => {
  const f = fixture(t); plan(f, 'METADATA', [...Array(30).fill(notFound), available]);
  const result = run(body('verify'), f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /registry-read-deadline/); noReceipt(f);
  assert.equal(reads(f).length, 30); assert.equal(f.sleeps().length, 30); assert.equal(f.clock(), 900);
  assert.ok(reads(f).every(row => row.at < 900)); assert.ok(f.sleeps().every(row => row.seconds === 30));
});

test('metadata and tar share the deadline instead of giving the tar a second budget', t => {
  const f = fixture(t); plan(f, 'METADATA', [...Array(28).fill(notFound), available]); plan(f, 'TAR', [notFound, notFound, available]);
  const result = run(body('verify'), f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /registry-read-deadline/); noReceipt(f);
  assert.equal(reads(f).filter(row => row.args.at(-1).endsWith('.tgz')).length, 2);
  assert.equal(reads(f).length, 31); assert.equal(f.sleeps().length, 30); assert.equal(f.clock(), 900);
  assert.ok(reads(f).every(row => row.at < 900));
});

test('actual verify clamps request time and the last sleep to the remaining shared budget', t => {
  const f = fixture(t); plan(f, 'METADATA', [...Array(28).fill(notFound), { ...available, elapsed: 29 }]);
  plan(f, 'TAR', [{ ...notFound, elapsed: 10 }, available]);
  const result = run(body('verify'), f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /registry-read-deadline/); noReceipt(f);
  const tar = reads(f).filter(row => row.args.at(-1).endsWith('.tgz'));
  assert.equal(tar.length, 1); assert.equal(tar[0].at, 869); assert.equal(tar[0].maxTime, 31);
  assert.equal(f.sleeps().at(-1).seconds, 21); assert.equal(f.clock(), 900);
});

for (const phase of ['METADATA', 'TAR']) for (const response of [
  { status: 500, exit: 22 }, { status: 403, exit: 22 }, { status: 0, exit: 28 },
  { status: 404, exit: 7 }, { status: 404, exit: 0 },
]) {
  test(`actual verify ${phase} status${response.status}/exit${response.exit} never authorizes retry or publish`, t => {
    const f = fixture(t); plan(f, phase, [response], true);
    const result = run(body('verify'), f);
    assert.notEqual(result.status, 0); noReceipt(f);
    assert.equal(reads(f).length, phase === 'METADATA' ? 1 : 2); assert.deepEqual(f.sleeps(), []); assert.equal(f.clock(), 0);
  });
}

test('a successful 404 wait does not retry changed registry bytes or publish them', t => {
  const f = fixture(t); plan(f, 'METADATA', [notFound, available]); writeFileSync(f.tar, 'changed after metadata');
  const result = run(body('verify'), f);
  assert.notEqual(result.status, 0); assert.match(result.stderr, /registry-bytes-drift/); noReceipt(f);
  assert.equal(reads(f).length, 3); assert.deepEqual(f.sleeps().map(row => row.seconds), [30]); assert.equal(f.clock(), 30);
});
