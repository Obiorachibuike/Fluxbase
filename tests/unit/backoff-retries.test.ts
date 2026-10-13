import { describe, expect, it } from 'vitest';
import { computeBackoffDelay } from '../../src/queue/backoff.js';
import { decideRetry, describeError } from '../../src/queue/retries.js';
import { UnrecoverableError } from '../../src/core/errors.js';

describe('computeBackoffDelay', () => {
  it('returns 0 with no backoff', () => {
    expect(computeBackoffDelay(null, 3)).toBe(0);
  });

  it('fixed delay is constant', () => {
    const b = { type: 'fixed' as const, delay: 500 };
    expect([1, 2, 5].map((n) => computeBackoffDelay(b, n))).toEqual([500, 500, 500]);
  });

  it('exponential delay doubles per attempt', () => {
    const b = { type: 'exponential' as const, delay: 1000 };
    expect([1, 2, 3, 4].map((n) => computeBackoffDelay(b, n))).toEqual([1000, 2000, 4000, 8000]);
  });

  it('clamps to maxDelay and never overflows', () => {
    const b = { type: 'exponential' as const, delay: 1000, maxDelay: 5000 };
    expect(computeBackoffDelay(b, 10)).toBe(5000);
    expect(computeBackoffDelay({ type: 'exponential', delay: 1, maxDelay: 9 }, 500)).toBe(9);
    expect(Number.isFinite(computeBackoffDelay({ type: 'exponential', delay: 1 }, 10_000))).toBe(true);
  });
});

describe('decideRetry', () => {
  const now = 1_000_000;
  it('retries while attempts remain, scheduled by backoff', () => {
    const d = decideRetry(new Error('x'), 1, 3, { type: 'fixed', delay: 100 }, now);
    expect(d).toEqual({ retry: true, retryAt: now + 100 });
  });

  it('gives up when attempts are exhausted', () => {
    expect(decideRetry(new Error('x'), 3, 3, null, now)).toEqual({ retry: false, retryAt: null });
  });

  it('never retries an UnrecoverableError', () => {
    expect(decideRetry(new UnrecoverableError('bad input'), 1, 10, null, now)).toEqual({ retry: false, retryAt: null });
  });
});

describe('describeError', () => {
  it('keeps the name and message but not the stack', () => {
    const e = new TypeError('bad thing');
    expect(describeError(e)).toBe('TypeError: bad thing');
    expect(describeError(e)).not.toContain('at ');
  });

  it('redacts credentials and secret assignments', () => {
    expect(describeError(new Error('connect https://admin:hunter2@db.example.com failed'))).not.toContain('hunter2');
    expect(describeError(new Error('auth failed, password=hunter2'))).not.toContain('hunter2');
  });

  it('truncates long messages and handles non-errors', () => {
    expect(describeError(new Error('x'.repeat(2000))).length).toBeLessThanOrEqual(500);
    expect(describeError('plain string')).toBe('Non-error value thrown: string');
  });
});
