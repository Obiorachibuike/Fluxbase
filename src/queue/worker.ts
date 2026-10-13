import type { BackoffOptions, ResolvedConfig } from '../core/config.js';
import {
  JobTimeoutError,
  LeaseLostError,
  SerializationError,
  UnrecoverableError,
} from '../core/errors.js';
import type { Logger } from '../observability/logger.js';
import type { QueueMetrics } from '../observability/metrics.js';
import type { JobRecord, StorageAdapter } from '../storage/adapter.js';
import { newId } from '../utils/ids.js';
import { deserialize, serialize } from '../utils/serialization.js';
import { decideRetry, describeError } from './retries.js';
import { type Job, toPublicJob } from './job.js';

export interface ProcessContext {
  /**
   * Aborted when the attempt times out, the lease is lost, or the worker is
   * shutting down. Handlers should observe it and stop promptly. Fluxbase cannot
   * force-kill code that ignores the signal.
   */
  readonly signal: AbortSignal;
}

/** Job handler. Its resolved value is stored as the job's return value (must be JSON-serializable). */
export type Processor<T, R> = (job: Job<T, R>, context: ProcessContext) => R | Promise<R>;

export interface ProcessOptions {
  /** Jobs this worker runs in parallel. Default: `worker.concurrency` from the client config (1). */
  concurrency?: number;
}

export type WorkerStatus = 'running' | 'degraded' | 'stopping' | 'stopped';

export interface WorkerHealth {
  id: string;
  queue: string;
  status: WorkerStatus;
  concurrency: number;
  /** Jobs this worker is running right now. */
  active: number;
  /** Jobs this worker completed since it started. */
  processed: number;
  /** Jobs this worker failed permanently since it started. */
  failed: number;
  /** Consecutive storage errors. Non-zero means the worker is degraded and retrying. */
  consecutiveStorageErrors: number;
  lastError: string | null;
  /** Epoch ms of the last successful claim poll, or null. */
  lastPollAt: number | null;
}

export interface WorkerDeps {
  storage: StorageAdapter;
  queue: string;
  logger: Logger;
  metrics: QueueMetrics;
  config: ResolvedConfig;
  /** Retention applied to this queue's finished jobs. */
  retention: { completed: number; failed: number };
  now: () => number;
  /** Called after the worker stops, so the host can unregister it. */
  onStopped: (worker: { id: string }) => void;
}

interface ActiveAttempt {
  job: JobRecord;
  token: string;
  controller: AbortController;
  timedOut: boolean;
  /** The lease expired or was recovered by someone else. The result must be discarded. */
  lost: boolean;
}

/** Abort reason used when the worker shuts down. Identity-compared. */
const SHUTDOWN_REASON = new Error('Fluxbase worker is shutting down.');

/**
 * Claims jobs, runs the handler with bounded concurrency, renews leases, and
 * records the outcome. It uses one heartbeat timer per worker, not per job, and
 * one poll loop. The loop wakes on local enqueues, on cross-process notifications,
 * when a job finishes, and otherwise on the poll interval.
 */
export class QueueWorker<T = unknown, R = unknown> {
  readonly id = newId();
  readonly queue: string;
  private state: 'running' | 'stopping' | 'stopped' = 'running';
  private readonly active = new Map<string, ActiveAttempt>();
  private readonly concurrency: number;
  private loopDone: Promise<void> = Promise.resolve();
  private heartbeat: NodeJS.Timeout | null = null;
  private heartbeatInFlight = false;
  private unsubscribe: (() => void) | null = null;
  private wakeResolver: (() => void) | null = null;
  private pendingWake = false;
  private stopPromise: Promise<void> | null = null;
  private processed = 0;
  private failed = 0;
  private consecutiveErrors = 0;
  private lastError: string | null = null;
  private lastPollAt: number | null = null;

  constructor(
    private readonly deps: WorkerDeps,
    private readonly handler: Processor<T, R>,
    concurrency: number,
  ) {
    this.queue = deps.queue;
    this.concurrency = concurrency;
  }

  /** @internal Starts polling. Called by Queue.process(). */
  async start(): Promise<void> {
    const { storage, queue } = this.deps;
    this.unsubscribe = await storage.subscribeQueue(queue, () => this.wake());
    this.heartbeat = setInterval(() => void this.renewActiveLeases(), Math.max(100, Math.floor(this.deps.config.worker.leaseMs / 3)));
    this.loopDone = this.loop();
  }

  /**
   * Stops the worker. New claims stop immediately. Active jobs get up to
   * `shutdownTimeoutMs` to finish. Jobs still running after that are aborted,
   * and their leases are released so another worker can run them. Attempts
   * released during shutdown do not count against `attempts`.
   */
  stop(): Promise<void> {
    this.stopPromise ??= this.doStop();
    return this.stopPromise;
  }

  health(): WorkerHealth {
    const status: WorkerStatus =
      this.state === 'stopped' ? 'stopped' : this.state === 'stopping' ? 'stopping' : this.consecutiveErrors > 0 ? 'degraded' : 'running';
    return {
      id: this.id,
      queue: this.queue,
      status,
      concurrency: this.concurrency,
      active: this.active.size,
      processed: this.processed,
      failed: this.failed,
      consecutiveStorageErrors: this.consecutiveErrors,
      lastError: this.lastError,
      lastPollAt: this.lastPollAt,
    };
  }

  // ------------------------------------------------------------ loop

  private async loop(): Promise<void> {
    const { storage, queue, config, logger, metrics } = this.deps;
    while (this.state === 'running') {
      const free = this.concurrency - this.active.size;
      try {
        // limit 0 still runs lease recovery, so jobs from crashed workers are
        // recovered even while every local slot is busy.
        const claimed = await storage.claimJobs({
          queue,
          workerId: this.id,
          limit: Math.max(0, free),
          leaseMs: config.worker.leaseMs,
          now: this.deps.now(),
        });
        this.lastPollAt = this.deps.now();
        this.consecutiveErrors = 0;
        this.lastError = null;
        if (this.state !== 'running') {
          // Stop was requested while the claim was in flight. Hand the jobs back.
          await Promise.all(claimed.map((job) => storage.releaseJob(queue, job.id, job.lockToken ?? '').catch(() => false)));
          break;
        }
        const claimedAt = this.deps.now();
        for (const job of claimed) {
          metrics.recordClaim(job.runAt, claimedAt);
          void this.execute(job);
        }
        if (free > 0 && claimed.length === free) continue;
      } catch (error) {
        this.consecutiveErrors++;
        this.lastError = describeError(error);
        metrics.storageErrors++;
        logger.warn('Worker could not poll storage; retrying', {
          queue,
          workerId: this.id,
          consecutiveErrors: this.consecutiveErrors,
          error: error as Error,
        });
        await this.waitForWork(Math.min(100 * 2 ** Math.min(this.consecutiveErrors, 7), 5000));
        continue;
      }
      await this.waitForWork(config.worker.pollIntervalMs);
    }
  }

  private waitForWork(ms: number): Promise<void> {
    if (this.pendingWake) {
      this.pendingWake = false;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wakeResolver = null;
        resolve();
      }, ms);
      this.wakeResolver = () => {
        clearTimeout(timer);
        this.wakeResolver = null;
        resolve();
      };
    });
  }

  /** Wakes the poll loop. A wake that arrives while the loop is busy is remembered, so it is not lost. */
  wake(): void {
    if (this.wakeResolver) {
      this.wakeResolver();
    } else {
      this.pendingWake = true;
    }
  }

  // ------------------------------------------------------------ execution

  private async execute(job: JobRecord): Promise<void> {
    const { storage, queue, config, logger, metrics } = this.deps;
    const controller = new AbortController();
    const attempt: ActiveAttempt = {
      job,
      token: job.lockToken ?? '',
      controller,
      timedOut: false,
      lost: false,
    };
    this.active.set(job.id, attempt);
    const startedAt = this.deps.now();
    let timer: NodeJS.Timeout | undefined;
    if (job.timeoutMs !== null) {
      timer = setTimeout(() => {
        attempt.timedOut = true;
        controller.abort(new JobTimeoutError(job.id, job.timeoutMs ?? 0));
      }, job.timeoutMs);
    }
    try {
      const publicJob = toPublicJob<T, R>(job);
      const result = await this.handler(publicJob, { signal: controller.signal });
      if (attempt.lost) {
        logger.warn('Discarded the result of a job whose lease was lost', { queue, jobId: job.id, workerId: this.id });
        return;
      }
      if (attempt.timedOut) throw new JobTimeoutError(job.id, job.timeoutMs ?? 0);
      const returnValue = result === undefined ? null : serialize(result, config.kv.maxValueBytes, 'Job return value');
      const completed = await storage.completeJob({
        queue,
        id: job.id,
        token: attempt.token,
        returnValue,
        now: this.deps.now(),
        keepCompleted: this.deps.retention.completed,
      });
      if (completed) {
        this.processed++;
        metrics.processed++;
        logger.debug('Job completed', { queue, jobId: job.id, attempt: job.attempts });
      } else {
        attempt.lost = true;
        logger.warn('Job completion ignored because the lease was lost', { queue, jobId: job.id, workerId: this.id });
      }
    } catch (error) {
      await this.handleFailure(attempt, error);
    } finally {
      if (timer) clearTimeout(timer);
      metrics.recordAttempt(this.deps.now() - startedAt);
      this.active.delete(job.id);
      this.wake();
    }
  }

  private async handleFailure(attempt: ActiveAttempt, error: unknown): Promise<void> {
    const { storage, queue, logger, metrics } = this.deps;
    const { job } = attempt;
    if (attempt.lost) {
      logger.warn('Job failure ignored because the lease was lost', { queue, jobId: job.id, workerId: this.id });
      return;
    }
    try {
      const now = this.deps.now();
      if (attempt.controller.signal.reason === SHUTDOWN_REASON && !attempt.timedOut) {
        const released = await storage.releaseJob(queue, job.id, attempt.token);
        if (released) {
          logger.info('Job released during shutdown; it will run again later', { queue, jobId: job.id });
        } else {
          attempt.lost = true;
        }
        return;
      }
      const effective = attempt.timedOut ? new JobTimeoutError(job.id, job.timeoutMs ?? 0) : error;
      const permanent = effective instanceof UnrecoverableError || effective instanceof SerializationError;
      const backoff = job.backoff === null ? null : deserialize<BackoffOptions>(job.backoff, 'backoff');
      const decision = permanent
        ? { retry: false, retryAt: null }
        : decideRetry(effective, job.attemptsMade, job.attempts, backoff, now);
      const outcome = await storage.failJob({
        queue,
        id: job.id,
        token: attempt.token,
        reason: describeError(effective),
        now,
        retryAt: decision.retryAt,
        keepFailed: this.deps.retention.failed,
      });
      if (outcome === 'lost') {
        attempt.lost = true;
        logger.warn('Job failure ignored because the lease was lost', { queue, jobId: job.id, workerId: this.id });
      } else if (outcome === 'retry') {
        metrics.retried++;
        logger.info('Job failed; retry scheduled', {
          queue,
          jobId: job.id,
          attemptsMade: job.attemptsMade,
          maxAttempts: job.attempts,
          retryAt: decision.retryAt,
          error: effective as Error,
        });
      } else {
        this.failed++;
        metrics.failed++;
        logger.warn('Job failed permanently', {
          queue,
          jobId: job.id,
          attemptsMade: job.attemptsMade,
          error: effective as Error,
        });
      }
    } catch (storageError) {
      metrics.storageErrors++;
      logger.error('Could not record job failure. The lease will expire and the job will be recovered.', {
        queue,
        jobId: job.id,
        error: storageError as Error,
      });
    }
  }

  private async renewActiveLeases(): Promise<void> {
    if (this.active.size === 0 || this.heartbeatInFlight) return;
    this.heartbeatInFlight = true;
    const { storage, queue, config, logger, metrics } = this.deps;
    try {
      const attempts = [...this.active.values()].filter((a) => !a.lost);
      if (attempts.length === 0) return;
      const renewed = new Set(
        await storage.renewLeases(
          queue,
          attempts.map((a) => ({ id: a.job.id, token: a.token })),
          config.worker.leaseMs,
          this.deps.now(),
        ),
      );
      for (const attempt of attempts) {
        if (!renewed.has(attempt.job.id)) {
          attempt.lost = true;
          attempt.controller.abort(new LeaseLostError(attempt.job.id));
          logger.warn('Lease lost; the job may be run by another worker', { queue, jobId: attempt.job.id, workerId: this.id });
        }
      }
    } catch (error) {
      metrics.storageErrors++;
      logger.warn('Lease renewal failed; leases may expire', { queue, workerId: this.id, error: error as Error });
    } finally {
      this.heartbeatInFlight = false;
    }
  }

  private async doStop(): Promise<void> {
    const { queue, config, logger, storage } = this.deps;
    this.state = 'stopping';
    this.wake();
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
    this.unsubscribe?.();
    this.unsubscribe = null;

    const deadline = this.deps.now() + config.worker.shutdownTimeoutMs;
    while (this.active.size > 0 && this.deps.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25));
    }
    if (this.active.size > 0) {
      logger.warn('Shutdown timeout reached; aborting active jobs', { queue, workerId: this.id, active: this.active.size });
      for (const attempt of this.active.values()) {
        if (!attempt.lost) attempt.controller.abort(SHUTDOWN_REASON);
      }
      // Give handlers that honor the signal a short grace period to release cleanly.
      const grace = Date.now() + 1000;
      while (this.active.size > 0 && Date.now() < grace) {
        await new Promise((r) => setTimeout(r, 25));
      }
      // Handlers that ignore the signal are released here. Their later results are discarded.
      for (const attempt of this.active.values()) {
        if (attempt.lost) continue;
        attempt.lost = true;
        await storage.releaseJob(queue, attempt.job.id, attempt.token).catch(() => false);
      }
    }
    await this.loopDone.catch(() => undefined);
    this.state = 'stopped';
    this.deps.onStopped(this);
  }
}
