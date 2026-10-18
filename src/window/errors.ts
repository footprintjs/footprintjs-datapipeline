import type { AttemptCategory } from 'footprintjs';
import { identifier } from './references.js';

export type WindowErrorCode = 'WINDOW_INVALID' | 'WINDOW_ABORTED' | 'WINDOW_FAILED' | 'WINDOW_RECORD_LIMIT';
export interface WindowFailureContext {
  readonly phase: 'validation' | 'initialization' | 'execution' | 'serialization' | 'paused' | 'aborted';
  readonly category: AttemptCategory;
  /** Present only when a native failure or pause event identifies the execution. */
  readonly runtimeStageId?: string;
}
const DEFAULT_FAILURE_PHASE = {
  WINDOW_INVALID: 'validation', WINDOW_ABORTED: 'aborted', WINDOW_FAILED: 'execution', WINDOW_RECORD_LIMIT: 'serialization',
} as const;

/** Fixed refusal with safe native context; never copies raw error text or objects. */
export class WindowExecutionError extends Error {
  readonly context: Readonly<WindowFailureContext>;
  constructor(readonly code: WindowErrorCode, context: WindowFailureContext = { phase: DEFAULT_FAILURE_PHASE[code], category: 'unknown' }) {
    super({
      WINDOW_INVALID: 'Window execution options are invalid.',
      WINDOW_ABORTED: 'Window execution was cancelled or timed out.',
      WINDOW_FAILED: 'Window execution did not complete.',
      WINDOW_RECORD_LIMIT: 'Window record exceeds the configured size limit.',
    }[code]);
    this.name = 'WindowExecutionError';
    this.context = Object.freeze({ phase: context.phase, category: context.category,
      ...(identifier(context.runtimeStageId) ? { runtimeStageId: context.runtimeStageId } : {}) });
  }
}

