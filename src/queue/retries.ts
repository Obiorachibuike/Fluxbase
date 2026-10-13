import { UnrecoverableError, redactSecrets } from '../core/errors.js';
import type { BackoffOptions } from '../core/config.js';
import { computeBackoffDelay } from './backoff.js';

export interface RetryDecision {
  /** true when another attempt should be scheduled. */
  retry: boolean;
  /** Absolute epoch ms of the next attempt when `retry` is true. */
  retryAt: number | null;
}

/**
 * Decides what to do after a failed attempt.
 * `UnrecoverableError` never retries. Otherwise retry while attempts remain.
 */
export function decideRetry(
  error: unknown,
  attemptsMade: number,
  maxAttempts: number,
  backoff: BackoffOptions | null,
  now: number,
): RetryDecision {
  if (error instanceof UnrecoverableError) return { retry: false, retryAt: null };
  if (attemptsMade >= maxAttempts) return { retry: false, retryAt: null };
  return { retry: true, retryAt: now + computeBackoffDelay(backoff, attemptsMade) };
}

const MAX_REASON_LENGTH = 500;

/**
 * Produces the failure message stored on the job. Only the error name and a
 * truncated message are kept. Stack traces and secrets are not stored.
 */
export function describeError(error: unknown): string {
  if (error instanceof Error) {
    const message = error.message.length > 0 ? error.message : 'Unknown error';
    return redactSecrets(`${error.name}: ${message}`).slice(0, MAX_REASON_LENGTH);
  }
  return `Non-error value thrown: ${typeof error}`.slice(0, MAX_REASON_LENGTH);
}
