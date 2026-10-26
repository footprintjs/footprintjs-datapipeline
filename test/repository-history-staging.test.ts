import { mkdir, symlink, writeFile, readFile, readdir, lstat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { expect, it } from 'vitest';
import { fixture, scopedWrite } from './helpers/history-repository.js';
it('never follows a linked stage or removes unknown content in a managed stage', async () => {
  const f = await fixture(), first = f.open(0);
  try {
    const outside = join(f.directory, 'outside'); await mkdir(outside); await writeFile(join(outside, 'keep'), 'do not delete');
    const linkWrite = await scopedWrite(first.repository, 'linked');
    const parent = join(f.directory, 'operations'); await mkdir(parent);
    const stage = (id: string) => join(parent, '.stage-' + createHash('sha256').update(id).digest('hex'));
    await symlink(outside, stage(linkWrite.operationId));
    await expect(first.repository.commit(linkWrite)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(await readFile(join(outside, 'keep'), 'utf8')).toBe('do not delete');
    expect((await lstat(stage(linkWrite.operationId))).isSymbolicLink()).toBe(true);
    const unknown = await scopedWrite(first.repository, 'unknown-stage');
    await mkdir(stage(unknown.operationId)); await writeFile(join(stage(unknown.operationId), 'keep'), 'unknown private content');
    await expect(first.repository.commit(unknown)).rejects.toMatchObject({ code: 'REPOSITORY_UNCONFIRMED' });
    expect(await readdir(stage(unknown.operationId))).toEqual(['keep']);
  } finally { await f.close(); }
});
