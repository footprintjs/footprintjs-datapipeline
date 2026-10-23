/** Isolated test-process probes only. They collect timings, never argument values. */
import { createRequire, syncBuiltinESMExports } from 'node:module';
export interface PhaseEvent { label: string; startMs: number; durationMs: number; heapBefore: number; heapAfter: number; rssAfter: number }
export function phaseRecorder() {
  const events: PhaseEvent[] = [];
  const anchor = performance.now();
  let collecting = false;
  function finish(label: string, start: number, heap: number) {
    if (!collecting) return;
    const memory = process.memoryUsage();
    events.push({ label, startMs: start - anchor, durationMs: performance.now() - start, heapBefore: heap, heapAfter: memory.heapUsed, rssAfter: memory.rss });
  }
  async function timed<T>(label: string, task: () => Promise<T>): Promise<T> {
    const start = performance.now(), heap = process.memoryUsage().heapUsed;
    try { return await task(); } finally { finish(label, start, heap); }
  }
  function sync<T>(label: string, task: () => T): T {
    const start = performance.now(), heap = process.memoryUsage().heapUsed;
    try { return task(); } finally { finish(label, start, heap); }
  }
  return { events, timed, sync, start() { collecting = true; }, stop() { collecting = false; }, finish };
}
/** Install before loading the source/store modules, which promisify native gzip. */
export function installNativeProbes(recorder: ReturnType<typeof phaseRecorder>) {
  const require = createRequire(import.meta.url);
  const zlib = require('node:zlib'), fs = require('node:fs/promises');
  const originals = { gzip: zlib.gzip, gunzip: zlib.gunzip, open: fs.open };
  for (const name of ['gzip', 'gunzip'] as const) {
    zlib[name] = function (...args: any[]) {
      const callback = args.pop(), start = performance.now(), heap = process.memoryUsage().heapUsed;
      return originals[name].call(this, ...args, (...values: any[]) => {
        recorder.finish(`native.${name}`, start, heap);
        callback(...values);
      });
    };
  }
  fs.open = async (...args: any[]) => {
    const handle = await recorder.timed('fs.open', () => originals.open(...args));
    for (const method of ['writeFile', 'sync'] as const) {
      const original = handle[method].bind(handle);
      handle[method] = (...values: any[]) => recorder.timed(`fs.${method}`, () => original(...values));
    }
    return handle;
  };
  syncBuiltinESMExports();
  return () => { zlib.gzip = originals.gzip; zlib.gunzip = originals.gunzip; fs.open = originals.open; syncBuiltinESMExports(); };
}
/** These are nested inclusive durations: never sum flow/store/I/O columns. */
export function phaseTotals(events: readonly PhaseEvent[]) {
  const totals: Record<string, { count: number; inclusiveMs: number }> = {};
  for (const event of events) {
    const previous = totals[event.label] ?? { count: 0, inclusiveMs: 0 };
    previous.count++; previous.inclusiveMs += event.durationMs; totals[event.label] = previous;
  }
  return totals;
}
