import type { BackoffOptions, JobDefaults, ResolvedConfig } from '../core/config.js';
import { InvalidStateError, ValidationError } from '../core/errors.js';
import { assertJobId, assertJobName, limits } from '../utils/ids.js';
import { serialize, deserialize } from '../utils/serialization.js';
import { validateBackoff } from '../core/config.js';

/** Lifecycle states of a job. */
export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed';

/** Pseudo-target used for permanent removal of a job record. */
export type JobTransitionTarget = JobState | 'deleted';

/**
 * The explicit state machine. Every state change in the engine and in both
 * storage adapters must be one of these edges.
 *
 * - waiting -> active: a worker claims the job.
 * - active -> completed: the handler succeeded.
 * - active -> failed: the handler failed and no attempts remain.
 * - active -> delayed: the handler failed; a retry is scheduled for later.
 * - active -> waiting: retry is due immediately, the claim is released during
 *   shutdown, or an expired lease is recovered with attempts remaining.
 * - delayed -> waiting: the delay has elapsed (promotion).
 * - failed -> waiting: manual retry through `queue.retry(id)`.
 * - completed | failed | waiting | delayed -> deleted: `queue.remove(id)` or retention.
 * - active jobs cannot be removed; they must finish, fail, or lose their lease first.
 */
export const JOB_TRANSITIONS: Readonly<Record<JobState, readonly JobTransitionTarget[]>> = {
  waiting: ['active', 'deleted'],
  delayed: ['waiting', 'deleted'],
  active: ['completed', 'failed', 'delayed', 'waiting'],
  completed: ['deleted'],
  failed: ['waiting', 'deleted'],
};

export function canTransition(from: JobState, to: JobTransitionTarget): boolean {
  return JOB_TRANSITIONS[from].includes(to);
}

export function assertTransition(from: JobState, to: JobTransitionTarget): void {
  if (!canTransition(from, to)) {
    throw new InvalidStateError(`Invalid job state transition: ${from} -> ${to}.`);
  }
}

export const JOB_STATES: readonly JobState[] = ['waiting', 'delayed', 'active', 'completed', 'failed'];

/** Options accepted by `queue.add()`. */
export interface JobOptions {
  /** Custom unique id within the queue. Adding a duplicate id throws DuplicateJobError. */
  jobId?: string;
  /** Milliseconds to wait before the job becomes eligible. Default: 0. */
  delay?: number;
  /** Higher values run first. Integer from -1000 to 1000. Default: 0. Equal priorities run FIFO. */
  priority?: number;
  /** Maximum total attempts including the first. 1-100. */
  attempts?: number;
  backoff?: BackoffOptions;
  /** Per-attempt timeout in ms. The handler's signal is aborted when it elapses. */
  timeout?: number;
  /** Delete the record immediately on success instead of keeping it for retention. */
  removeOnComplete?: boolean;
  /** Delete the record immediately on final failure. */
  removeOnFail?: boolean;
  /** Small JSON object stored with the job (max 16 KiB). Must not contain secrets. */
  metadata?: Record<string, unknown>;
}

/** The public, read-only view of a job. */
export interface Job<T = unknown, R = unknown> {
  readonly id: string;
  readonly queue: string;
  readonly name: string;
  readonly data: T;
  readonly state: JobState;
  readonly priority: number;
  /** Maximum total attempts configured for this job. */
  readonly maxAttempts: number;
  /** Attempts started so far (incremented when the job is claimed). */
  readonly attemptsMade: number;
  readonly backoff: BackoffOptions | null;
  readonly timeout: number | null;
  /** Creation time, epoch ms. */
  readonly createdAt: number;
  /** Time the job became or becomes eligible to run, epoch ms. */
  readonly runAt: number;
  /** Time of the most recent claim, epoch ms. */
  readonly processedAt: number | null;
  /** Time the job completed or permanently failed, epoch ms. */
  readonly finishedAt: number | null;
  /** Message of the most recent failure. Stack traces are not stored. */
  readonly failedReason: string | null;
  readonly returnValue: R | null;
  readonly metadata: Record<string, unknown>;
  /** Lease expiry while active, epoch ms. */
  readonly lockedUntil: number | null;
  readonly workerId: string | null;
}

/** Normalized job spec ready to be stored by an adapter. */
export interface NormalizedJobSpec {
  id: string;
  delay: number;
  priority: number;
  attempts: number;
  backoff: BackoffOptions | null;
  timeout: number | null;
  removeOnComplete: boolean;
  removeOnFail: boolean;
  metadata: string;
}

function int(name: string, value: unknown, min: number, max: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw new ValidationError(`${name} must be an integer between ${min} and ${max}. Received: ${String(value)}.`);
  }
  return value;
}

/** Validates user-supplied job options and applies queue/global defaults. */
export function normalizeJobOptions(
  options: JobOptions | undefined,
  defaults: ResolvedConfig['defaults'],
  generateId: () => string,
): NormalizedJobSpec {
  const o: JobOptions = options ?? {};
  if (typeof o !== 'object' || o === null) throw new ValidationError('Job options must be an object.');

  const id = o.jobId ?? generateId();
  assertJobId(id);

  const delay = o.delay === undefined ? 0 : int('delay', o.delay, 0, limits.maxDelayMs);
  const priority = o.priority === undefined ? 0 : int('priority', o.priority, -limits.maxPriority, limits.maxPriority);
  const attempts = o.attempts === undefined ? defaults.attempts : int('attempts', o.attempts, 1, limits.maxAttempts);
  const backoffSource: JobDefaults['backoff'] | undefined = o.backoff ?? defaults.backoff ?? undefined;
  const backoff = backoffSource === undefined ? null : validateBackoff(backoffSource, 'backoff');
  const timeoutSource = o.timeout ?? defaults.timeout ?? undefined;
  const timeout = timeoutSource === undefined ? null : int('timeout', timeoutSource, 1, limits.maxTimeoutMs);

  let metadata = '{}';
  if (o.metadata !== undefined) {
    if (typeof o.metadata !== 'object' || o.metadata === null || Array.isArray(o.metadata)) {
      throw new ValidationError('metadata must be a plain object.');
    }
    metadata = serialize(o.metadata, limits.maxMetadataBytes, 'metadata');
  }

  return {
    id,
    delay,
    priority,
    attempts,
    backoff,
    timeout,
    removeOnComplete: o.removeOnComplete ?? defaults.removeOnComplete,
    removeOnFail: o.removeOnFail ?? defaults.removeOnFail,
    metadata,
  };
}

export function assertAddArguments(name: unknown): asserts name is string {
  assertJobName(name);
}

/** Converts a stored record into the public Job view. Parsing failures surface as SerializationError. */
export function toPublicJob<T, R>(record: StoredJobFields): Job<T, R> {
  return {
    id: record.id,
    queue: record.queue,
    name: record.name,
    data: deserialize<T>(record.data, 'job data'),
    state: record.state,
    priority: record.priority,
    maxAttempts: record.attempts,
    attemptsMade: record.attemptsMade,
    backoff: record.backoff === null ? null : (deserialize<BackoffOptions>(record.backoff, 'backoff')),
    timeout: record.timeoutMs,
    createdAt: record.createdAt,
    runAt: record.runAt,
    processedAt: record.processedAt,
    finishedAt: record.finishedAt,
    failedReason: record.failedReason,
    returnValue: record.returnValue === null ? null : deserialize<R>(record.returnValue, 'return value'),
    metadata: deserialize<Record<string, unknown>>(record.metadata, 'job metadata'),
    lockedUntil: record.lockedUntil,
    workerId: record.workerId,
  };
}

/** The fields shared by stored records and public jobs (serialized forms). */
export interface StoredJobFields {
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
  lockedUntil: number | null;
  workerId: string | null;
}
