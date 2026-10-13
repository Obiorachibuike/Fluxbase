import type { Logger } from '../observability/logger.js';
import type { NewJobRecord, ScheduleRecord, StorageAdapter } from '../storage/adapter.js';
import { compileCron } from './cron.js';
import { redactSecrets } from '../core/errors.js';
import { type ScheduleJobSpec } from './schedule-spec.js';

const BATCH_SIZE = 100;

/**
 * Turns due schedules into jobs.
 *
 * Duplicate prevention: each run has a deterministic job id `<schedule>:<slot>`,
 * and firing is a compare-and-set on the schedule's `nextRunAt`. Two schedulers,
 * in one process or on many, can both see a schedule as due, but only one wins
 * the CAS. The loser does nothing.
 *
 * Missed runs: when the scheduler was offline, all missed slots are coalesced
 * into ONE run for the earliest missed slot. The schedule then advances to the
 * next occurrence after now. `metadata.scheduledFor` records that slot.
 *
 * Precision: runs are enqueued on the first tick after their slot, so a run may
 * start up to `tickMs` (plus queue latency) late. Runs are not exact to the millisecond.
 */
export class Scheduler {
  private timer: NodeJS.Timeout | null = null;
  private stopped = true;
  private inFlight: Promise<number> | null = null;

  constructor(
    private readonly storage: StorageAdapter,
    private readonly tickMs: number,
    private readonly logger: Logger,
    private readonly now: () => number = Date.now,
    private readonly onFired: (queue: string) => void = () => undefined,
  ) {}

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    void this.tick().catch(() => undefined);
    this.schedule();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.inFlight) await this.inFlight.catch(() => undefined);
  }

  /** Runs one pass. Returns the number of runs enqueued. Concurrent calls share one pass. */
  tick(): Promise<number> {
    this.inFlight ??= this.runTick().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.tick()
        .catch(() => undefined)
        .finally(() => this.schedule());
    }, this.tickMs);
    this.timer.unref();
  }

  private async runTick(): Promise<number> {
    const now = this.now();
    let fired = 0;
    let due: ScheduleRecord[];
    try {
      due = await this.storage.listDueSchedules(now, BATCH_SIZE);
    } catch (error) {
      this.logger.warn('Could not read due schedules', { error: error as Error });
      return 0;
    }
    for (const schedule of due) {
      try {
        if (await this.fire(schedule, now)) fired++;
      } catch (error) {
        this.logger.error('Failed to enqueue scheduled job', {
          queue: schedule.queue,
          schedule: schedule.name,
          error: error as Error,
        });
      }
    }
    return fired;
  }

  private async fire(schedule: ScheduleRecord, now: number): Promise<boolean> {
    const compiled = compileCron(schedule.cron, schedule.timezone);
    const spec = JSON.parse(schedule.jobOptions) as ScheduleJobSpec;
    const slot = schedule.nextRunAt;
    const metadata = {
      ...(JSON.parse(spec.metadata) as Record<string, unknown>),
      schedule: schedule.name,
      scheduledFor: slot,
    };
    const job: NewJobRecord = {
      // Deterministic id: a repeated firing for the same slot is ignored by storage.
      id: `${schedule.name}:${slot}`,
      queue: schedule.queue,
      name: schedule.name,
      data: schedule.data,
      state: 'waiting',
      priority: spec.priority,
      attempts: spec.attempts,
      backoff: spec.backoff === null ? null : JSON.stringify(spec.backoff),
      timeoutMs: spec.timeoutMs,
      createdAt: now,
      runAt: slot,
      metadata: JSON.stringify(metadata),
      removeOnComplete: spec.removeOnComplete,
      removeOnFail: spec.removeOnFail,
    };
    const nextRunAt = compiled.nextAfter(now);
    const won = await this.storage.fireSchedule({
      queue: schedule.queue,
      name: schedule.name,
      expectedNextRunAt: slot,
      nextRunAt,
      now,
      job,
    });
    if (won) {
      this.onFired(schedule.queue);
      this.logger.debug('Scheduled job enqueued', { queue: schedule.queue, schedule: schedule.name, slot });
    }
    return won;
  }
}

/** Sanitizes a schedule error for logging. */
export function describeScheduleError(error: unknown): string {
  return redactSecrets(error instanceof Error ? error.message : String(error));
}
