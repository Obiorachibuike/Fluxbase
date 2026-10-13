import type { JobState } from '../queue/job.js';
import type { StorageCapabilities } from './capabilities.js';

/** A stored job. `data`, `metadata`, `backoff`, and `returnValue` are JSON text. */
export interface JobRecord {
  id: string;
  queue: string;
  name: string;
  data: string;
  state: JobState;
  priority: number;
  attempts: number;
  attemptsMade: number;
  backoff: string | null;
  timeoutMs: number | null;
  createdAt: number;
  runAt: number;
  processedAt: number | null;
  finishedAt: number | null;
  failedReason: string | null;
  returnValue: string | null;
  metadata: string;
  /** Lease token of the current claim. Only set while active. Never exposed publicly. */
  lockToken: string | null;
  lockedUntil: number | null;
  workerId: string | null;
}

export interface NewJobRecord {
  id: string;
  queue: string;
  name: string;
  data: string;
  state: 'waiting' | 'delayed';
  priority: number;
  attempts: number;
  backoff: string | null;
  timeoutMs: number | null;
  createdAt: number;
  runAt: number;
  metadata: string;
  removeOnComplete: boolean;
  removeOnFail: boolean;
}

export interface Lease {
  id: string;
  token: string;
}

export interface ScheduleRecord {
  queue: string;
  name: string;
  cron: string;
  timezone: string;
  data: string;
  /** JSON-encoded JobOptions applied to each run. */
  jobOptions: string;
  nextRunAt: number;
  lastRunAt: number | null;
  createdAt: number;
  updatedAt: number;
}

export type RemoveResult = 'removed' | 'not_found' | 'active';
export type RetryResult = 'retried' | 'not_found' | 'not_failed';
export type FailResult = 'retry' | 'failed' | 'lost';

/**
 * The persistence contract used by the queue engine. The engine contains no
 * SQL or Redis commands. Each method is atomic in its backend: a claim, a
 * completion, or a schedule firing either happens completely or not at all.
 */
export interface StorageAdapter {
  readonly kind: 'sqlite' | 'redis';
  readonly capabilities: StorageCapabilities;

  open(): Promise<void>;
  close(): Promise<void>;
  /** Throws if the backend is unreachable. */
  ping(): Promise<void>;

  // ---- jobs ----
  /** Inserts a job. Throws DuplicateJobError when the id already exists in the queue. */
  insertJob(job: NewJobRecord): Promise<JobRecord>;
  getJob(queue: string, id: string): Promise<JobRecord | null>;
  listJobs(queue: string, state: JobState, limit: number): Promise<JobRecord[]>;
  countJobs(queue: string): Promise<Record<JobState, number>>;

  /**
   * Atomically: promotes due delayed jobs, recovers expired leases, and, unless
   * the queue is paused, claims up to `limit` waiting jobs as active with new
   * lease tokens. Returns the claimed records.
   */
  claimJobs(args: { queue: string; workerId: string; limit: number; leaseMs: number; now: number }): Promise<JobRecord[]>;

  /** Extends the leases still held by these tokens. Returns the ids that were renewed. */
  renewLeases(queue: string, leases: Lease[], leaseMs: number, now: number): Promise<string[]>;

  /**
   * Marks an active job completed if the token still holds the lease. Jobs created
   * with `removeOnComplete` are deleted instead. Retention trims older completed jobs.
   */
  completeJob(args: {
    queue: string;
    id: string;
    token: string;
    returnValue: string | null;
    now: number;
    keepCompleted: number;
  }): Promise<boolean>;

  /**
   * Records a failed attempt. With `retryAt` the job goes to `delayed` (or `waiting`
   * when retryAt <= now). Without it the job becomes `failed` (or is deleted when
   * created with `removeOnFail`).
   */
  failJob(args: {
    queue: string;
    id: string;
    token: string;
    reason: string;
    now: number;
    retryAt: number | null;
    keepFailed: number;
  }): Promise<FailResult>;

  /** Returns an active job to `waiting` without counting the attempt (used during shutdown). */
  releaseJob(queue: string, id: string, token: string): Promise<boolean>;

  /** Moves a failed job back to waiting with its attempt counter reset. */
  retryJob(queue: string, id: string, now: number): Promise<RetryResult>;

  removeJob(queue: string, id: string): Promise<RemoveResult>;

  /** Deletes the oldest finished jobs in `state` beyond `keep`. Returns the number deleted. */
  trimFinished(queue: string, state: 'completed' | 'failed', keep: number): Promise<number>;

  setPaused(queue: string, paused: boolean): Promise<void>;
  isPaused(queue: string): Promise<boolean>;

  /** Wakes workers for `queue`. On backends without cross-process notifications this is local only. */
  notifyQueue(queue: string): Promise<void>;
  /** Subscribes to wake-ups for `queue`. Returns an unsubscribe function. */
  subscribeQueue(queue: string, callback: () => void): Promise<() => void>;

  // ---- schedules ----
  putSchedule(record: ScheduleRecord): Promise<void>;
  getSchedule(queue: string, name: string): Promise<ScheduleRecord | null>;
  listSchedules(queue: string): Promise<ScheduleRecord[]>;
  removeSchedule(queue: string, name: string): Promise<boolean>;
  listDueSchedules(now: number, limit: number): Promise<ScheduleRecord[]>;
  /**
   * Compare-and-set: if the schedule still has `expectedNextRunAt`, advance it
   * to `nextRunAt`, record `lastRunAt`, and insert `job` (idempotent on job id).
   * Returns false when another scheduler already advanced this schedule.
   */
  fireSchedule(args: {
    queue: string;
    name: string;
    expectedNextRunAt: number;
    nextRunAt: number;
    now: number;
    job: NewJobRecord;
  }): Promise<boolean>;

  // ---- key-value ----
  /** Returns the stored JSON text, or null when missing or expired. */
  kvGet(key: string, now: number): Promise<string | null>;
  kvGetMany(keys: string[], now: number): Promise<(string | null)[]>;
  kvSet(key: string, value: string, expiresAt: number | null): Promise<void>;
  kvDelete(key: string): Promise<boolean>;
  /** Deletes up to `limit` expired entries. Returns the number deleted. */
  kvSweep(now: number, limit: number): Promise<number>;

  // ---- cross-process events (only when capabilities.broadcastEvents) ----
  publishBroadcast?(channel: string, message: string): Promise<void>;
  subscribeBroadcast?(channel: string, callback: (message: string) => void): Promise<() => void>;
}
