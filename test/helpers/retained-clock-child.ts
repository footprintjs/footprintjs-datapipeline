import { createRetainedFilesystemRepository } from '../../src/storage/retainedRepository/index.js';
let wall = 1_790_000_000_000, elapsed = 0;
const repository = createRetainedFilesystemRepository({ enabled: true, directory: process.argv[2]!, retentionMs: 100,
  requireSeparateFilesystem: false, expiryClock: 'verified-uptime', clock: () => wall,
  monotonicClock: () => elapsed, onHealth() {} })!;
process.on('message', async (message: { wall: number; elapsed: number; id: string }) => {
  wall = message.wall; elapsed = message.elapsed;
  try { process.send?.({ value: await repository.read({ kind: 'batch', id: message.id }) }); }
  catch { process.send?.({ unavailable: true }); }
});
