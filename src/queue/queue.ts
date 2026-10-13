import type { ResolvedConfig } from '../core/config.js';
import { InvalidStateError, JobNotFoundError, ValidationError } from '../core/errors.js';
import type { Logger } from '../observability/logger.js';
import type { QueueMetrics, LocalQueueMetrics } from '../observability/metrics.js';
import type { NewJobRecord, ScheduleRecord, StorageAdapter } from '../storage/adapter.js';
import { assertJobName, assertQueueName, newId } from '../utils/ids.js';
import { serialize, deserialize } from '../utils/serialization.js';
import { assertScheduleName, compileCron } from './cron.js';
import { type Job, type JobOptions, type JobState, JOB_STATES, normalizeJobOptions, toPublicJob } from './job.js';
import type { ScheduleJobSpec } from './schedule-spec.js';
import { type ProcessOptions, type Processor, QueueWorker } from './worker.js';

/** Services a queue needs from its owning client. */
export interface QueueHost {
  readonly instanceId: string;
  readonly config: ResolvedConfig;
  readonly logger: Logger;
  readonly storage: StorageAdapter;
  now(): number;
  assertRunning(): void;
  metricsFor(queue: string): QueueMetrics;
  registerWorker(worker: QueueWorker<never, never>): void;
  unregisterWorker(worker: { id: string }): void;
  workersFor(queue: string): number;
}

export interface QueueOptions {
  /** Keep this many completed jobs. Older ones are deleted. Default: `retention.completed` (1000). */
  retention?: { completed?: number; failed?: number };
}

export interface ScheduleOptions extends Omit<JobOptions, 'jobId' | 'delay'> {
  /** Standard 5-field cron (`minute hour day month weekday`), or 6-field with seconds. */
  cron: string;
  /** IANA timezone. Default: "UTC". */
  timezone?: string;
}

export interface ScheduleInfo<T = unknown> {
  readonly queue: string;
  readonly name: string;
  readonly cron: string;
  readonly timezone: string;
  readonly data: T;
  readonly nextRunAt: number;
  readonly lastRunAt: number | null;
  readonly createdAt: number;
  readonly updatedAt: number;
}

export interface QueueStats {
  readonly queue: string;
  readonly paused: boolean;
  /** Authoritative counts from storage, shared across processes when the backend is shared. */
  readonly waiting: number;
  readonly delayed: number;
  readonly active: number;
  readonly completed: number;
  readonly failed: number;
  /** Workers running in THIS process for this queue. */
  readonly workers: number;
  /** Counters for THIS process since start. */
  readonly local: LocalQueueMetrics;
}

const DEFAULT_LIST_LIMIT = 50;
const MAX_LIST_LIMIT = 500;

/**
 * A named job queue. Obtain instances with `fluxbase.queue<Data, Result>(name)`.
 * The type parameters describe job payloads and processor results. They are compile-time only.
 */
export class Queue<T = unknown, R = unknown> {
  readonly name: string;
  private readonly keepCompleted: number;
  private readonly keepFailed: number;

  constructor(
    private readonly host: QueueHost,
    name: string,
    options: QueueOptions = {},
  ) {
    assertQueueName(name);
    this.name = name;
    const completed = options.retention?.completed ?? host.config.retention.completed;
    const failed = options.retention?.failed ?? host.config.retention.failed;
    for (const [label, value] of [
      ['retention.completed', completed],
      ['retention.failed', failed],
    ] as const) {
      if (!Number.isInteger(value) || value < 0 || value > 1_000_000) {
        throw new ValidationError(`${label} must be an integer between 0 and 1000000.`);
      }
    }
    this.keepCompleted = completed;
    this.keepFailed = failed;
  }

  /**
   * Adds a job. Resolves with the stored job once it is persisted. A job with
   * `delay` starts in the `delayed` state.
   *
   * Jobs may run more than once (at-least-once delivery). Make handlers idempotent.
   */
  async add(name: string, data: T, options?: JobOptions): Promise<Job<T, R>> {
    this.host.assertRunning();
    assertJobName(name);
    const spec = normalizeJobOptions(options, this.host.config.defaults, newId);
    const text = serialize(data, this.host.config.kv.maxValueBytes, 'Job data');
    const now = this.host.now();
    const record: NewJobRecord = {
      id: spec.id,
      queue: this.name,
      name,
      data: text,
      state: spec.delay > 0 ? 'delayed' : 'waiting',
      priority: spec.priority,
      attempts: spec.attempts,
      backoff: spec.backoff === null ? null : JSON.stringify(spec.backoff),
      timeoutMs: spec.timeout,
      createdAt: now,
      runAt: now + spec.delay,
      metadata: spec.metadata,
      removeOnComplete: spec.removeOnComplete,
      removeOnFail: spec.removeOnFail,
    };
    const stored = await this.host.storage.insertJob(record);
    if (record.state === 'waiting') {
      await this.host.storage.notifyQueue(this.name).catch((error: unknown) => {
        this.host.logger.debug('Wake-up notification failed; workers will poll', { queue: this.name, error: error as Error });
      });
    }
    return toPublicJob<T, R>(stored);
  }

  /** Returns the job, or null when it does not exist. */
  async getJob(id: string): Promise<Job<T, R> | null> {
    this.host.assertRunning();
    const record = await this.host.storage.getJob(this.name, id);
    return record ? toPublicJob<T, R>(record) : null;
  }

  /** Returns the job's state, or null when it does not exist. */
  async getJobState(id: string): Promise<JobState | null> {
    this.host.assertRunning();
    const record = await this.host.storage.getJob(this.name, id);
    return record ? record.state : null;
  }

  /** Lists jobs in one state. Waiting jobs come in dispatch order, finished jobs newest first. */
  async getJobs(state: JobState, options: { limit?: number } = {}): Promise<Job<T, R>[]> {
    this.host.assertRunning();
    if (!JOB_STATES.includes(state)) throw new ValidationError(`Unknown job state "${String(state)}".`);
    const limit = options.limit ?? DEFAULT_LIST_LIMIT;
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIST_LIMIT) {
      throw new ValidationError(`limit must be an integer between 1 and ${MAX_LIST_LIMIT}.`);
    }
    const records = await this.host.storage.listJobs(this.name, state, limit);
    return records.map((record) => toPublicJob<T, R>(record));
  }

  /**
   * Removes a job that is not active. Active jobs must finish, fail, or lose their lease first.
   * Throws JobNotFoundError or InvalidStateError.
   */
  async remove(id: string): Promise<void> {
    this.host.assertRunning();
    const result = await this.host.storage.removeJob(this.name, id);
    if (result === 'not_found') throw new JobNotFoundError(this.name, id);
    if (result === 'active') {
      throw new InvalidStateError(`Job "${id}" is active and cannot be removed. Wait for it to finish or fail.`);
    }
  }

  /**
   * Moves a failed job back to `waiting`, with its attempt counter reset to 0.
   * Throws JobNotFoundError, or InvalidStateError when the job is not failed.
   */
  async retry(id: string): Promise<void> {
    this.host.assertRunning();
    const result = await this.host.storage.retryJob(this.name, id, this.host.now());
    if (result === 'not_found') throw new JobNotFoundError(this.name, id);
    if (result === 'not_failed') {
      throw new InvalidStateError(`Only failed jobs can be retried. Job "${id}" is not in the failed state.`);
    }
    await this.host.storage.notifyQueue(this.name).catch(() => undefined);
  }

  /** Stops workers in every process from claiming new jobs. Adding jobs still works. */
  async pause(): Promise<void> {
    this.host.assertRunning();
    await this.host.storage.setPaused(this.name, true);
  }

  /** Resumes claiming after `pause()`. */
  async resume(): Promise<void> {
    this.host.assertRunning();
    await this.host.storage.setPaused(this.name, false);
    await this.host.storage.notifyQueue(this.name).catch(() => undefined);
  }

  async isPaused(): Promise<boolean> {
    this.host.assertRunning();
    return this.host.storage.isPaused(this.name);
  }

  async getStats(): Promise<QueueStats> {
    this.host.assertRunning();
    const [counts, paused] = await Promise.all([
      this.host.storage.countJobs(this.name),
      this.host.storage.isPaused(this.name),
    ]);
    return {
      queue: this.name,
      paused,
      waiting: counts.waiting,
      delayed: counts.delayed,
      active: counts.active,
      completed: counts.completed,
      failed: counts.failed,
      workers: this.host.workersFor(this.name),
      local: this.host.metricsFor(this.name).snapshot(),
    };
  }

  /**
   * Starts a worker in this process. Each job runs `handler`. Resolves nothing;
   * returns the worker handle, which provides `stop()` and `health()`.
   */
  process(handler: Processor<T, R>, options: ProcessOptions = {}): QueueWorker<T, R> {
    this.host.assertRunning();
    if (typeof handler !== 'function') throw new ValidationError('The job handler must be a function.');
    const concurrency = options.concurrency ?? this.host.config.worker.concurrency;
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 1000) {
      throw new ValidationError('concurrency must be an integer between 1 and 1000.');
    }
    const worker = new QueueWorker<T, R>(
      {
        storage: this.host.storage,
        queue: this.name,
        logger: this.host.logger,
        metrics: this.host.metricsFor(this.name),
        config: this.host.config,
        retention: { completed: this.keepCompleted, failed: this.keepFailed },
        now: () => this.host.now(),
        onStopped: (w) => this.host.unregisterWorker(w),
      },
      handler,
      concurrency,
    );
    this.host.registerWorker(worker as unknown as QueueWorker<never, never>);
    worker.start().catch((error: unknown) => {
      this.host.logger.error('Worker failed to start', { queue: this.name, error: error as Error });
      void worker.stop();
    });
    return worker;
  }

  /**
   * Creates or updates a recurring schedule. Each run enqueues a job named `name`.
   * Updating an existing schedule with the same cron, timezone, data, and options keeps its next run.
   */
  async schedule(name: string, data: T, options: ScheduleOptions): Promise<ScheduleInfo<T>> {
    this.host.assertRunning();
    assertScheduleName(name);
    const raw = options as ScheduleOptions & { jobId?: unknown; delay?: unknown };
    if (raw.jobId !== undefined || raw.delay !== undefined) {
      throw new ValidationError('Scheduled jobs do not accept jobId or delay. Each run gets a deterministic id and runs at its slot.');
    }
    const { cron, timezone: timezoneOption, ...jobOptions } = options;
    const timezone = timezoneOption ?? 'UTC';
    const compiled = compileCron(cron, timezone);
    const spec = normalizeJobOptions(jobOptions, this.host.config.defaults, newId);
    const jobSpec: ScheduleJobSpec = {
      priority: spec.priority,
      attempts: spec.attempts,
      backoff: spec.backoff,
      timeoutMs: spec.timeout,
      removeOnComplete: spec.removeOnComplete,
      removeOnFail: spec.removeOnFail,
      metadata: spec.metadata,
    };
    const dataText = serialize(data, this.host.config.kv.maxValueBytes, 'Schedule data');
    const optionsText = JSON.stringify(jobSpec);
    const now = this.host.now();
    const existing = await this.host.storage.getSchedule(this.name, name);
    const unchanged =
      existing !== null &&
      existing.cron === compiled.expression &&
      existing.timezone === timezone &&
      existing.data === dataText &&
      existing.jobOptions === optionsText;
    const record: ScheduleRecord = {
      queue: this.name,
      name,
      cron: compiled.expression,
      timezone,
      data: dataText,
      jobOptions: optionsText,
      nextRunAt: unchanged ? existing.nextRunAt : compiled.nextAfter(now),
      lastRunAt: existing?.lastRunAt ?? null,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
    };
    await this.host.storage.putSchedule(record);
    return this.toScheduleInfo(record);
  }

  /** Returns the schedule, or null when it does not exist. */
  async getSchedule(name: string): Promise<ScheduleInfo<T> | null> {
    this.host.assertRunning();
    const record = await this.host.storage.getSchedule(this.name, name);
    return record ? this.toScheduleInfo(record) : null;
  }

  async listSchedules(): Promise<ScheduleInfo<T>[]> {
    this.host.assertRunning();
    return (await this.host.storage.listSchedules(this.name)).map((r) => this.toScheduleInfo(r));
  }

  /** Deletes the schedule. Runs already enqueued are not affected. Returns false if it did not exist. */
  async removeSchedule(name: string): Promise<boolean> {
    this.host.assertRunning();
    return this.host.storage.removeSchedule(this.name, name);
  }

  private toScheduleInfo(record: ScheduleRecord): ScheduleInfo<T> {
    return {
      queue: record.queue,
      name: record.name,
      cron: record.cron,
      timezone: record.timezone,
      data: deserialize<T>(record.data, 'schedule data'),
      nextRunAt: record.nextRunAt,
      lastRunAt: record.lastRunAt,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    };
  }
}

