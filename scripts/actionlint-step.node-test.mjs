// Actual syntax-step source with finite inert tool ports; no real tool/network.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const NAME = 'Check workflow syntax and expression contexts';
const VERSION = '1.7.12';
const SHA = '8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8';
const DOWNLOAD_URL = `https://github.com/rhysd/actionlint/releases/download/v${VERSION}/actionlint_${VERSION}_linux_amd64.tar.gz`;
const FILES = ['.github/workflows/publish.yml', '.github/workflows/ci.yml'];

function yaml(path) {
  assert.ok(existsSync(path), 'The source document must be present');
  const result = spawnSync('ruby', ['-rjson', '-ryaml', '-e',
    'puts JSON.generate(YAML.safe_load(File.read(ARGV.fetch(0)), permitted_classes: [], permitted_symbols: [], aliases: false))', path],
  { encoding: 'utf8', timeout: 5_000, maxBuffer: 1024 * 1024 });
  assert.equal(result.error, undefined, 'Ruby/Psych must be installed before controls run');
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

// Read the actual repository CI file; no environment-selected source.
function source() {
  return yaml(fileURLToPath(new URL('../.github/workflows/ci.yml', import.meta.url)));
}

function requiredStep(ci) {
  assert.deepEqual(ci.permissions, { contents: 'read' });
  assert.deepEqual(Object.keys(ci.jobs).sort(), ['check', 'file-size', 'minimum-runtime']);
  const job = ci.jobs.check;
  assert.equal(job.if, undefined, 'The existing check job stays active');
  assert.equal(job['continue-on-error'], undefined);
  const all = Object.values(ci.jobs).flatMap(row => row.steps || []).filter(row => row.name === NAME);
  assert.equal(all.length, 1, 'One active syntax step is required');
  const step = all[0];
  assert.equal(job.steps.indexOf(step), 1, 'The tool runs directly after the existing checkout');
  assert.ok(job.steps[0].uses.startsWith('actions/checkout@'));
  assert.equal(step.if, undefined);
  assert.equal(step['continue-on-error'], undefined);
  assert.equal(step.uses, undefined);
  assert.equal(step['timeout-minutes'], 2);
  assert.equal(step.shell, undefined);
  assert.equal(step.env?.ACTIONLINT_VERSION, VERSION);
  assert.equal(step.env?.ACTIONLINT_SHA256, SHA);
  assert.equal(typeof step.run, 'string');
  assert.ok(step.run.length > 0);
  return step;
}

// Commands record finite argv/results only; no archive is downloaded, hashed,
// extracted or executed. The original shell body itself is what runs.
const stub = String.raw`
const fs=require('node:fs'),path=require('node:path'),e=process.env,args=process.argv.slice(3),kind=process.argv[2];
const record=row=>fs.appendFileSync(e.LOG,JSON.stringify(row)+'\n');
if(kind==='curl'){
 record({command:'download',args});
 const i=args.indexOf('--output');
 if(i<0||!args[i+1])process.exit(81);
 fs.writeFileSync(args[i+1],'inert archive DATA; never downloaded/extracted');
}else if(kind==='sha256sum'){
 record({command:'checksum',args,input:fs.readFileSync(0,'utf8')});
 process.exit(e.REJECT_CHECKSUM==='yes'?1:0);
}else if(kind==='tar'){
 record({command:'extract',args});
 const i=args.indexOf('-C');if(i<0||!args[i+1])process.exit(82);
 fs.writeFileSync(path.join(args[i+1],'actionlint'),'#!/bin/sh\nexec "$REAL_NODE" "$STUB" actionlint "$@"\n',{mode:0o700});
}else if(kind==='actionlint'){
 record({command:'lint',args,cwd:process.cwd()});
}else process.exit(83);
`;

function fixture(t, step, reject = false) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'dp syntax inert ')));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const name of ['bin', 'workspace', 'runner temporary', 'home']) mkdirSync(join(root, name));
  const helper = join(root, 'stub.cjs'), log = join(root, 'commands.jsonl');
  writeFileSync(helper, stub); writeFileSync(log, '');
  for (const name of ['curl', 'sha256sum', 'tar']) {
    writeFileSync(join(root, 'bin', name), `#!/bin/sh\nexec "$REAL_NODE" "$STUB" ${name} "$@"\n`, { mode: 0o700 });
  }
  const workspace = join(root, 'workspace'), temporary = join(root, 'runner temporary');
  const env = { PATH: `${join(root, 'bin')}${delimiter}${process.env.PATH || ''}`,
    HOME: process.env.HOME, TMPDIR: temporary, RUNNER_TEMP: temporary,
    REAL_NODE: process.execPath, STUB: helper, LOG: log,
    ACTIONLINT_VERSION: step.env.ACTIONLINT_VERSION, ACTIONLINT_SHA256: step.env.ACTIONLINT_SHA256,
    REJECT_CHECKSUM: reject ? 'yes' : 'no', NODE_OPTIONS: '', NODE_PATH: '' };
  const result = spawnSync('bash', ['--noprofile', '--norc', '-c', step.run], {
    cwd: workspace, env, encoding: 'utf8', timeout: 5_000, maxBuffer: 64 * 1024, killSignal: 'SIGKILL',
  });
  assert.equal(result.error, undefined, 'Setup/timeout cannot count as target guard refusal');
  assert.equal(result.signal, null);
  const rows = readFileSync(log, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  return { result, rows, workspace, temporary };
}

test('optional delta creates one active pinned check without a job/permission change', () => {
  requiredStep(source());
});

test('actual proposed shell checks the pinned download before extraction and lints exactly two workflows', t => {
  const { result, rows, workspace, temporary } = fixture(t, requiredStep(source()));
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(rows.map(row => row.command), ['download', 'checksum', 'extract', 'lint']);
  const [download, checked, extracted, lint] = rows;
  const archive = download.args[download.args.indexOf('--output') + 1];
  assert.ok(archive.startsWith(temporary + '/'));
  assert.deepEqual(download.args, ['--fail', '--silent', '--show-error', '--location', DOWNLOAD_URL, '--output', archive]);
  assert.deepEqual(checked.args, ['--check', '--strict', '-']);
  assert.equal(checked.input, `${SHA}  ${archive}\n`);
  const directory = extracted.args[extracted.args.indexOf('-C') + 1];
  assert.equal(archive, join(directory, 'actionlint.tar.gz'));
  assert.deepEqual(extracted.args, ['-xzf', archive, '-C', directory, 'actionlint']);
  assert.deepEqual(lint.args, ['-shellcheck=', '-pyflakes=', ...FILES]);
  assert.equal(lint.cwd, workspace);
});

test('forced checksum failure stops the actual shell before extraction/tool invocation', t => {
  const { result, rows } = fixture(t, requiredStep(source()), true);
  assert.notEqual(result.status, 0);
  assert.deepEqual(rows.map(row => row.command), ['download', 'checksum']);
});

test('deleted/duplicated/disabled checks and altered pins are rejected structurally', () => {
  const original = source(); requiredStep(original);
  const cases = [
    ['deleted', ci => { ci.jobs.check.steps.splice(1, 1); }],
    ['duplicated', ci => { ci.jobs.check.steps.push(structuredClone(ci.jobs.check.steps[1])); }],
    ['job disabled', ci => { ci.jobs.check.if = false; }],
    ['job ignored', ci => { ci.jobs.check['continue-on-error'] = true; }],
    ['step disabled', ci => { ci.jobs.check.steps[1].if = '${{ false }}'; }],
    ['step ignored', ci => { ci.jobs.check.steps[1]['continue-on-error'] = true; }],
    ['wrong version', ci => { ci.jobs.check.steps[1].env.ACTIONLINT_VERSION = '1.7.11'; }],
    ['wrong hash', ci => { ci.jobs.check.steps[1].env.ACTIONLINT_SHA256 = '0'.repeat(64); }],
  ];
  for (const [name, change] of cases) {
    const ci = structuredClone(original); change(ci);
    assert.throws(() => requiredStep(ci), { name: 'AssertionError' }, name);
  }
});

test('copied-body removal of checksum exposes extraction/tool execution rejected by the original negative', t => {
  const step = requiredStep(source()), control = fixture(t, step, true);
  assert.notEqual(control.result.status, 0);
  assert.deepEqual(control.rows.map(row => row.command), ['download', 'checksum']);
  const lines = step.run.split('\n');
  const guards = lines.filter(line => line.includes('sha256sum --check --strict -'));
  assert.equal(guards.length, 1);
  const changed = { ...step, run: lines.filter(line => line !== guards[0]).join('\n') };
  const fault = fixture(t, changed, true);
  assert.equal(fault.result.status, 0, fault.result.stderr);
  assert.deepEqual(fault.rows.map(row => row.command), ['download', 'extract', 'lint']);
});

test('copied-body reordering extracts before failed checksum and is visible to the negative control', t => {
  const step = requiredStep(source()), lines = step.run.split('\n');
  const checksum = lines.findIndex(line => line.includes('sha256sum --check --strict -'));
  const extract = lines.findIndex(line => line.trim().startsWith('tar -xzf'));
  assert.ok(checksum >= 0 && extract === checksum + 1);
  [lines[checksum], lines[extract]] = [lines[extract], lines[checksum]];
  const fault = fixture(t, { ...step, run: lines.join('\n') }, true);
  assert.notEqual(fault.result.status, 0);
  assert.deepEqual(fault.rows.map(row => row.command), ['download', 'extract', 'checksum']);
});
