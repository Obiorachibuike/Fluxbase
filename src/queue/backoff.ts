import type { BackoffOptions } from '../core/config.js';

export const DEFAULT_MAX_BACKOFF_MS = 24 * 60 * 60 * 1000;

/**
 * Computes the delay before the next attempt.
 *
 * @param backoff  The configured backoff. `null` means retry immediately.
 * @param attemptsMade  Attempts already made, including the one that just failed (>= 1).
 * @returns delay in ms, clamped to `maxDelay`.
 *
 * - fixed:       delay
 * - exponential: delay * 2^(attemptsMade - 1)
 */
export function computeBackoffDelay(backoff: BackoffOptions | null, attemptsMade: number): number {
  if (!backoff || backoff.delay === 0) return 0;
  const cap = backoff.maxDelay ?? DEFAULT_MAX_BACKOFF_MS;
  if (backoff.type === 'fixed') {
    return Math.min(backoff.delay, cap);
  }
  const exponent = Math.max(0, attemptsMade - 1);
  // Guard against overflow: 2^n grows quickly; anything past the cap is capped.
  const raw = exponent >= 53 ? Number.POSITIVE_INFINITY : backoff.delay * 2 ** exponent;
  return Math.min(raw, cap);
}
