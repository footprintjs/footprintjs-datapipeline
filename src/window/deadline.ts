import { WindowExecutionError, type WindowFailureContext } from './errors.js';
import type { ValidatedWindowOptions } from './options.js';

export interface WindowDeadline {
  readonly signal: AbortSignal;
  abortedContext(): WindowFailureContext;
}

export async function withDeadline<T>(options: ValidatedWindowOptions, work: (deadline: WindowDeadline) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  const abort = () => controller.abort();
  const deadline: WindowDeadline = { signal: controller.signal,
    abortedContext: () => ({ phase: 'aborted', category: timedOut ? 'timeout' : 'unknown' }) };
  if (options.signal?.aborted) throw new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext());
  options.signal?.addEventListener('abort', abort, { once: true });
  const timer = setTimeout(() => { timedOut = true; abort(); }, options.timeoutMs);
  try { return await work(deadline); }
  finally { clearTimeout(timer); options.signal?.removeEventListener('abort', abort); }
}

/** Bound asynchronous waiting without interpreting the thrown value. */
export function abortable<T>(work: Promise<T>, deadline: WindowDeadline): Promise<T> {
  const signal = deadline.signal;
  return new Promise<T>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener('abort', abort);
    const abort = () => { cleanup(); reject(new WindowExecutionError('WINDOW_ABORTED', deadline.abortedContext())); };
    // Install rejection handling even when the signal was already cancelled.
    work.then(value => { cleanup(); resolve(value); }, error => { cleanup(); reject(error); });
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
  });
}

