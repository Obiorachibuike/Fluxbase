import { CronExpressionParser } from 'cron-parser';
import { ValidationError } from '../core/errors.js';

export const SCHEDULE_NAME_PATTERN = /^[A-Za-z0-9_.-]{1,100}$/;
const MAX_CRON_LENGTH = 100;

export interface CompiledCron {
  readonly expression: string;
  readonly timezone: string;
  /** Next occurrence strictly after `afterMs` (epoch ms). */
  nextAfter(afterMs: number): number;
}

/**
 * Validates a cron expression and IANA timezone, then returns a calculator.
 * Invalid input throws ValidationError at definition time, not at run time.
 *
 * Daylight-saving behavior (from cron-parser):
 * - a local time that does not exist (spring forward) is moved to the next valid time;
 * - a local time that occurs twice (fall back) runs once, on the first occurrence.
 */
export function compileCron(expression: unknown, timezone: unknown): CompiledCron {
  if (typeof expression !== 'string' || expression.trim().length === 0 || expression.length > MAX_CRON_LENGTH) {
    throw new ValidationError(`cron must be a non-empty string of at most ${MAX_CRON_LENGTH} characters.`);
  }
  const tz = timezone === undefined ? 'UTC' : timezone;
  if (typeof tz !== 'string' || tz.length === 0) {
    throw new ValidationError('timezone must be an IANA timezone name such as "UTC" or "Europe/London".');
  }
  const compiled: CompiledCron = {
    expression: expression.trim(),
    timezone: tz,
    nextAfter(afterMs: number): number {
      const iterator = CronExpressionParser.parse(compiled.expression, {
        currentDate: new Date(afterMs),
        tz,
      });
      return iterator.next().getTime();
    },
  };
  try {
    compiled.nextAfter(Date.now());
  } catch (cause) {
    const reason = (cause as Error).message;
    if (/timezone|time zone|unhandled timestamp|Invalid time value/i.test(reason)) {
      throw new ValidationError(`Unknown timezone "${tz}". Use an IANA timezone name such as "UTC" or "America/New_York".`);
    }
    throw new ValidationError(`Invalid cron expression "${expression}": ${reason}`);
  }
  return compiled;
}

export function assertScheduleName(name: unknown): asserts name is string {
  if (typeof name !== 'string' || !SCHEDULE_NAME_PATTERN.test(name)) {
    throw new ValidationError('Schedule names must be 1-100 characters from A-Z, a-z, 0-9, "_", ".", "-".');
  }
}
