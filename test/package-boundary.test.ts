import { execFileSync } from 'node:child_process';
import { lstatSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

const root = fileURLToPath(new URL('../', import.meta.url));
const manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { files: string[] };

it('publishes supporting documents only through explicit regular-file entries', () => {
  expect(manifest.files).toContain('dist');
  expect(new Set(manifest.files).size).toBe(manifest.files.length);
  for (const entry of manifest.files) {
    if (entry === 'dist') continue;
    // A directory or glob can silently publish a later local experiment.
    expect(entry).not.toMatch(/[*?{}[\]\\]|(^|\/)\.\.(\/|$)|^\//);
    const stat = lstatSync(new URL('../' + entry, import.meta.url));
    expect(stat.isFile(), entry).toBe(true);
    expect(stat.isSymbolicLink(), entry).toBe(false);
  }
});

it('keeps the actual npm file list inside the reviewed distribution boundary', () => {
  const cache = mkdtempSync(join(tmpdir(), 'datapipeline-pack-'));
  try {
    const output = execFileSync(process.platform === 'win32' ? 'npm.cmd' : 'npm',
      ['pack', '--dry-run', '--ignore-scripts', '--json', '--offline', '--cache', cache],
      { cwd: root, encoding: 'utf8', timeout: 20_000 });
    const plans = JSON.parse(output) as { files: { path: string }[] }[];
    expect(plans).toHaveLength(1);
    const allowed = new Set(['package.json', ...manifest.files.filter(entry => entry !== 'dist')]);
    const paths = plans[0]!.files.map(file => file.path);
    expect(paths).toContain('README.md');
    expect(paths).toContain('LICENSE');
    for (const path of paths) {
      expect(path.startsWith('dist/') || allowed.has(path), path).toBe(true);
    }
  } finally { rmSync(cache, { recursive: true, force: true }); }
}, 25_000);
