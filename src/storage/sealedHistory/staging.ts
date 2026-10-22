import { lstat, mkdir, readdir, rmdir, unlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { fail } from './contracts.js';
import { missing } from './classify.js';
import { owned, privateDirectory } from './files.js';

const markerDirectory = /^marker-([1-9][0-9]*)-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const markerFiles = new Set(['retention.sqlite', 'retention.sqlite-journal']);
function processExists(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'ESRCH') return false;
    if (code === 'EPERM') return true;
    throw error;
  }
}
/** Never recurse into or remove an unknown entry, link, owner or permission set. */
async function checkedStageFiles(directory: string): Promise<string[]> {
  await privateDirectory(directory);
  const names = await readdir(directory);
  for (const name of names) {
    if (!markerFiles.has(name)) fail('SEALED_HISTORY_UNMANAGED');
    try {
      const info = await lstat(join(directory, name));
      if (!info.isFile() || !owned(info.uid) || (info.mode & 0o077) !== 0) fail('SEALED_HISTORY_UNMANAGED');
    } catch (error) { if (!missing(error)) throw error; }
  }
  return names;
}
export async function removeMarkerStage(directory: string): Promise<void> {
  for (const name of await checkedStageFiles(directory)) {
    try { await unlink(join(directory, name)); } catch (error) { if (!missing(error)) throw error; }
  }
  await rmdir(directory);
}
async function checkedMarkerStages(directory: string): Promise<{ path: string; pid: number }[]> {
  const staging = join(directory, '.staging');
  try { await privateDirectory(staging); } catch (error) { if (missing(error)) return []; throw error; }
  const stages: { path: string; pid: number }[] = [];
  for (const name of await readdir(staging)) {
    const match = markerDirectory.exec(name), pid = Number(match?.[1]);
    if (!match || !Number.isSafeInteger(pid) || pid > 0x7fffffff) fail('SEALED_HISTORY_UNMANAGED');
    const path = join(staging, name);
    try {
      await checkedStageFiles(path);
      stages.push({ path, pid });
    } catch (error) { if (!missing(error)) throw error; }
  }
  return stages;
}
/** Read-side validation never probes PIDs or removes stages. */
export async function checkMarkerStages(directory: string): Promise<void> {
  await checkedMarkerStages(directory);
}
/** Write path only; all publishers must share one host and PID namespace. */
export async function reclaimDeadMarkerStages(directory: string): Promise<void> {
  for (const stage of await checkedMarkerStages(directory)) {
    try { if (!processExists(stage.pid)) await removeMarkerStage(stage.path); }
    catch (error) { if (!missing(error)) throw error; }
  }
}
export async function createMarkerStage(directory: string): Promise<{ directory: string; marker: string }> {
  const staging = join(directory, '.staging');
  await mkdir(staging, { mode: 0o700, recursive: true });
  await privateDirectory(staging);
  const path = join(staging, `marker-${process.pid}-${randomUUID()}`);
  await mkdir(path, { mode: 0o700 });
  return { directory: path, marker: join(path, 'retention.sqlite') };
}
