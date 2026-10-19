import { signalAborted } from './writeOptions.js';

export type StopCode = 'cancelled' | 'timeout';

export class WriteStopped extends Error {
  constructor(readonly code: StopCode) { super('InfluxDB write stopped.'); }
}

/** Bounds waiting even when a host callback or fetch ignores its signal. */
export function createWriteBudget(timeoutMs: number, caller?: AbortSignal) {
  const initiallyAborted = caller ? signalAborted(caller) : false;
  const controller = new AbortController();
  const endsAt = performance.now() + timeoutMs;
  let stopped: StopCode | undefined;
  let rejectStop!: (reason: WriteStopped) => void;
  const stopPromise = new Promise<never>((_, reject) => { rejectStop = reject; });
  // Cancellation before the first await must never become an unhandled rejection.
  void stopPromise.catch(() => {});
  function stop(code: StopCode) {
    if (stopped) return;
    stopped = code;
    controller.abort();
    rejectStop(new WriteStopped(code));
  }
  const onAbort = () => stop('cancelled');
  if (caller) EventTarget.prototype.addEventListener.call(caller, 'abort', onAbort, { once: true });
  const timer = setTimeout(() => stop('timeout'), timeoutMs);
  if (initiallyAborted) stop('cancelled');
  function reason() {
    if (!stopped && performance.now() >= endsAt) stop('timeout');
    return stopped;
  }
  function check() {
    reason();
    if (stopped) throw new WriteStopped(stopped);
  }
  return {
    signal: controller.signal,
    check,
    reason,
    async wait<T>(start: () => T | PromiseLike<T>): Promise<T> {
      check();
      const result = await Promise.race([Promise.resolve().then(start), stopPromise]);
      check();
      return result;
    },
    close() {
      clearTimeout(timer);
      if (caller) EventTarget.prototype.removeEventListener.call(caller, 'abort', onAbort);
    },
  };
}
