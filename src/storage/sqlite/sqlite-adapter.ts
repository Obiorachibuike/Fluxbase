import Database from 'better-sqlite3';
import { EventEmitter } from 'node:events';
import { existsSync, mkdirSync, chmodSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { ConfigError, DuplicateJobError, StorageError, redactSecrets } from '../../core/errors.js';
import type { JobState } from '../../queue/job.js';
import { newId } from '../../utils/ids.js';
import { MEMORY_CAPABILITIES, SQLITE_CAPABILITIES, type StorageCapabilities } from '../capabilities.js';
import type {
  FailResult,
  JobRecord,
  Lease,
  NewJobRecord,
  RemoveResult,
  RetryResult,
  ScheduleRecord,
  StorageAdapter,
} from '../adapter.js';
import { SCHEMA_V1, SCHEMA_VERSION } from './schema.js';

type Db = Database.Database;

interface JobRow {
  id: string;
  queue: string;
  name: string;
  data: string;
  state: JobState;
  priority: number;
  attempts: number;
  attempts_made: number;
  backoff: string | null;
  timeout_ms: number | null;
  created_at: number;
  run_at: number;
  processed_at: number | null;
  finished_at: number | null;
  failed_reason: string | null;
  return_value: string | null;
  metadata: string;
  lock_token: string | null;
  locked_until: number | null;
  worker_id: string | null;
  remove_on_complete: number;
  remove_on_fail: number;
}

interface ScheduleRow {
  queue: string;
  name: string;
  cron: string;
  timezone: string;
  data: string;
  job_options: string;
  next_run_at: number;
  last_run_at: number | null;
  created_at: number;
  updated_at: number;
}

const JOB_COLUMNS = `id, queue, name, data, state, priority, attempts, attempts_made, backoff, timeout_ms,
  created_at, run_at, processed_at, finished_at, failed_reason, return_value, metadata,
  lock_token, locked_until, worker_id, remove_on_complete, remove_on_fail`;

function toJob(row: JobRow): JobRecord {
  return {
    id: row.id,
    queue: row.queue,
    name: row.name,
    data: row.data,
    state: row.state,
    priority: row.priority,
    attempts: row.attempts,
    attemptsMade: row.attempts_made,
    backoff: row.backoff,
    timeoutMs: row.timeout_ms,
    createdAt: row.created_at,
    runAt: row.run_at,
    processedAt: row.processed_at,
    finishedAt: row.finished_at,
    failedReason: row.failed_reason,
    returnValue: row.return_value,
    metadata: row.metadata,
    lockToken: row.lock_token,
    lockedUntil: row.locked_until,
    workerId: row.worker_id,
  };
}

function toSchedule(row: ScheduleRow): ScheduleRecord {
  return {
    queue: row.queue,
    name: row.name,
    cron: row.cron,
    timezone: row.timezone,
    data: row.data,
    jobOptions: row.job_options,
    nextRunAt: row.next_run_at,
    lastRunAt: row.last_run_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

/**
 * SQLite implementation of {@link StorageAdapter}, backed by better-sqlite3.
 *
 * - The database runs in WAL mode with `synchronous = FULL`, so a committed
 *   transaction survives a process crash or power loss on a filesystem that
 *   honors fsync.
 * - Every mutating operation runs in a `BEGIN IMMEDIATE` transaction. That takes
 *   the write lock before reading, so the read-check-update sequence used for
 *   claiming cannot interleave with another connection's claim.
 * - Each method performs one SQLite operation. The engine never sees SQL.
 */
export class SqliteStorageAdapter implements StorageAdapter {
  readonly kind = 'sqlite' as const;
  readonly capabilities: StorageCapabilities;
  private db: Db | null = null;
  private readonly notifier = new EventEmitter();
  private s!: ReturnType<SqliteStorageAdapter['prepareStatements']>;

  constructor(
    private readonly path: string,
    volatile: boolean,
  ) {
    this.capabilities = volatile ? MEMORY_CAPABILITIES : SQLITE_CAPABILITIES;
    this.notifier.setMaxListeners(0);
  }

  async open(): Promise<void> {
    if (this.db) return;
    const created = this.path !== ':memory:' && !existsSync(this.path);
    if (this.path !== ':memory:') {
      this.prepareFilesystem();
    }
    let db: Db;
    try {
      db = new Database(this.path, { timeout: 5000 });
    } catch (cause) {
      throw new StorageError(
        `Failed to open SQLite database at "${this.path}". Check that the directory exists and is writable. ${(cause as Error).message}`,
        cause,
      );
    }
    try {
      db.pragma('journal_mode = WAL');
      db.pragma('synchronous = FULL');
      db.pragma('foreign_keys = ON');
      db.pragma('busy_timeout = 5000');
      const check = db.pragma('quick_check', { simple: true });
      if (check !== 'ok') {
        throw new StorageError(`SQLite integrity check failed for "${this.path}": ${String(check)}`);
      }
      this.migrate(db);
    } catch (cause) {
      db.close();
      if (cause instanceof StorageError) throw cause;
      throw new StorageError(`Failed to initialize SQLite database at "${this.path}": ${(cause as Error).message}`, cause);
    }
    if (created && this.path !== ':memory:') {
      try {
        chmodSync(this.path, 0o600);
      } catch {
        // Best effort on platforms without POSIX permissions.
      }
    }
    this.db = db;
    this.s = this.prepareStatements(db);
  }

  async close(): Promise<void> {
    if (!this.db) return;
    this.notifier.removeAllListeners();
    this.db.close();
    this.db = null;
  }

  async ping(): Promise<void> {
    this.require().prepare('SELECT 1').get();
  }

  // ---------------------------------------------------------------- jobs

  async insertJob(job: NewJobRecord): Promise<JobRecord> {
    return this.run(() => {
      try {
        this.s.insertJob.run({
          queue: job.queue,
          id: job.id,
          name: job.name,
          data: job.data,
          state: job.state,
          priority: job.priority,
          attempts: job.attempts,
          backoff: job.backoff,
          timeout_ms: job.timeoutMs,
          created_at: job.createdAt,
          run_at: job.runAt,
          metadata: job.metadata,
          remove_on_complete: job.removeOnComplete ? 1 : 0,
          remove_on_fail: job.removeOnFail ? 1 : 0,
        });
      } catch (cause) {
        if ((cause as { code?: string }).code === 'SQLITE_CONSTRAINT_PRIMARYKEY') {
          throw new DuplicateJobError(job.queue, job.id);
        }
        throw cause;
      }
      const row = this.s.getJob.get(job.queue, job.id) as JobRow;
      return toJob(row);
    });
  }

  async getJob(queue: string, id: string): Promise<JobRecord | null> {
    return this.run(() => {
      const row = this.s.getJob.get(queue, id) as JobRow | undefined;
      return row ? toJob(row) : null;
    });
  }

  async listJobs(queue: string, state: JobState, limit: number): Promise<JobRecord[]> {
    return this.run(() => {
      let rows: JobRow[];
      switch (state) {
        case 'waiting':
          rows = this.s.listWaiting.all(queue, limit) as JobRow[];
          break;
        case 'delayed':
          rows = this.s.listDelayed.all(queue, limit) as JobRow[];
          break;
        case 'active':
          rows = this.s.listActive.all(queue, limit) as JobRow[];
          break;
        default:
          rows = this.s.listFinished.all(queue, state, limit) as JobRow[];
      }
      return rows.map(toJob);
    });
  }

  async countJobs(queue: string): Promise<Record<JobState, number>> {
    return this.run(() => {
      const counts: Record<JobState, number> = { waiting: 0, delayed: 0, active: 0, completed: 0, failed: 0 };
      for (const row of this.s.countJobs.all(queue) as Array<{ state: JobState; n: number }>) {
        counts[row.state] = row.n;
      }
      return counts;
    });
  }

  async claimJobs(args: { queue: string; workerId: string; limit: number; leaseMs: number; now: number }): Promise<JobRecord[]> {
    const { queue, workerId, limit, leaseMs, now } = args;
    return this.write(() => {
      // 1. Promote delayed jobs whose time has come.
      this.s.promoteDelayed.run(queue, now);

      // 2. Recover jobs whose lease expired (the worker crashed or stalled).
      const expired = this.s.expiredLeases.all(queue, now) as Array<{ id: string; attempts: number; attempts_made: number }>;
      for (const job of expired) {
        if (job.attempts_made >= job.attempts) {
          this.s.failExpired.run(
            now,
            `LeaseExpired: attempt ${job.attempts_made} of ${job.attempts} lost its lease (worker crashed or stalled)`,
            queue,
            job.id,
          );
        } else {
          this.s.requeueExpired.run(queue, job.id);
        }
      }

      // 3. Paused queues accept jobs but hand none out.
      if (this.isPausedInTx(queue)) return [];

      // 4. Claim waiting jobs in priority order, then FIFO.
      const candidates = this.s.readyIds.all(queue, limit) as Array<{ id: string }>;
      const claimed: JobRecord[] = [];
      for (const candidate of candidates) {
        const row = this.s.claim.get(newId(), now + leaseMs, workerId, now, queue, candidate.id) as JobRow | undefined;
        if (row) claimed.push(toJob(row));
      }
      return claimed;
    });
  }

  async renewLeases(queue: string, leases: Lease[], leaseMs: number, now: number): Promise<string[]> {
    return this.write(() => {
      const renewed: string[] = [];
      for (const lease of leases) {
        const result = this.s.renewLease.run(now + leaseMs, queue, lease.id, lease.token);
        if (result.changes === 1) renewed.push(lease.id);
      }
      return renewed;
    });
  }

  async completeJob(args: {
    queue: string;
    id: string;
    token: string;
    returnValue: string | null;
    now: number;
    keepCompleted: number;
  }): Promise<boolean> {
    const { queue, id, token, returnValue, now, keepCompleted } = args;
    return this.write(() => {
      const row = this.s.completeLease.get(now, returnValue, queue, id, token) as { remove_on_complete: number } | undefined;
      if (!row) return false;
      if (row.remove_on_complete === 1) {
        this.s.deleteJob.run(queue, id);
      } else {
        this.trimInTx(queue, 'completed', keepCompleted);
      }
      return true;
    });
  }

  async failJob(args: {
    queue: string;
    id: string;
    token: string;
    reason: string;
    now: number;
    retryAt: number | null;
    keepFailed: number;
  }): Promise<FailResult> {
    const { queue, id, token, reason, now, retryAt, keepFailed } = args;
    return this.write((): FailResult => {
      const row = this.s.lockedForFailure.get(queue, id, token) as { remove_on_fail: number } | undefined;
      if (!row) return 'lost';
      if (retryAt !== null) {
        this.s.retryLater.run(retryAt <= now ? 'waiting' : 'delayed', retryAt, reason, queue, id);
        return 'retry';
      }
      if (row.remove_on_fail === 1) {
        this.s.deleteJob.run(queue, id);
      } else {
        this.s.finalFail.run(now, reason, queue, id);
        this.trimInTx(queue, 'failed', keepFailed);
      }
      return 'failed';
    });
  }

  async releaseJob(queue: string, id: string, token: string): Promise<boolean> {
    return this.write(() => {
      const result = this.s.releaseLease.run(queue, id, token);
      return result.changes === 1;
    });
  }

  async retryJob(queue: string, id: string, now: number): Promise<RetryResult> {
    return this.write((): RetryResult => {
      const row = this.s.stateOf.get(queue, id) as { state: JobState } | undefined;
      if (!row) return 'not_found';
      if (row.state !== 'failed') return 'not_failed';
      this.s.resetFailed.run(now, queue, id);
      return 'retried';
    });
  }

  async removeJob(queue: string, id: string): Promise<RemoveResult> {
    return this.write((): RemoveResult => {
      const row = this.s.stateOf.get(queue, id) as { state: JobState } | undefined;
      if (!row) return 'not_found';
      if (row.state === 'active') return 'active';
      this.s.deleteJob.run(queue, id);
      return 'removed';
    });
  }

  async trimFinished(queue: string, state: 'completed' | 'failed', keep: number): Promise<number> {
    return this.write(() => this.trimInTx(queue, state, keep));
  }

  async setPaused(queue: string, paused: boolean): Promise<void> {
    await this.write(() => {
      this.s.setPaused.run(queue, paused ? 1 : 0);
    });
  }

  async isPaused(queue: string): Promise<boolean> {
    return this.run(() => this.isPausedInTx(queue));
  }

  async notifyQueue(queue: string): Promise<void> {
    this.notifier.emit(`queue:${queue}`);
  }

  async subscribeQueue(queue: string, callback: () => void): Promise<() => void> {
    const event = `queue:${queue}`;
    this.notifier.on(event, callback);
    return () => {
      this.notifier.off(event, callback);
    };
  }

  // ---------------------------------------------------------------- schedules

  async putSchedule(record: ScheduleRecord): Promise<void> {
    await this.write(() => {
      this.s.putSchedule.run({
        queue: record.queue,
        name: record.name,
        cron: record.cron,
        timezone: record.timezone,
        data: record.data,
        job_options: record.jobOptions,
        next_run_at: record.nextRunAt,
        created_at: record.createdAt,
        updated_at: record.updatedAt,
      });
    });
  }

  async getSchedule(queue: string, name: string): Promise<ScheduleRecord | null> {
    return this.run(() => {
      const row = this.s.getSchedule.get(queue, name) as ScheduleRow | undefined;
      return row ? toSchedule(row) : null;
    });
  }

  async listSchedules(queue: string): Promise<ScheduleRecord[]> {
    return this.run(() => (this.s.listSchedules.all(queue) as ScheduleRow[]).map(toSchedule));
  }

  async removeSchedule(queue: string, name: string): Promise<boolean> {
    return this.write(() => this.s.removeSchedule.run(queue, name).changes === 1);
  }

  async listDueSchedules(now: number, limit: number): Promise<ScheduleRecord[]> {
    return this.run(() => (this.s.dueSchedules.all(now, limit) as ScheduleRow[]).map(toSchedule));
  }

  async fireSchedule(args: {
    queue: string;
    name: string;
    expectedNextRunAt: number;
    nextRunAt: number;
    now: number;
    job: NewJobRecord;
  }): Promise<boolean> {
    const { queue, name, expectedNextRunAt, nextRunAt, now, job } = args;
    return this.write(() => {
      const advanced = this.s.advanceSchedule.run(nextRunAt, now, now, queue, name, expectedNextRunAt);
      if (advanced.changes !== 1) return false;
      this.s.insertJobIgnoreDuplicate.run({
        queue: job.queue,
        id: job.id,
        name: job.name,
        data: job.data,
        state: job.state,
        priority: job.priority,
        attempts: job.attempts,
        backoff: job.backoff,
        timeout_ms: job.timeoutMs,
        created_at: job.createdAt,
        run_at: job.runAt,
        metadata: job.metadata,
        remove_on_complete: job.removeOnComplete ? 1 : 0,
        remove_on_fail: job.removeOnFail ? 1 : 0,
      });
      return true;
    });
  }

  // ---------------------------------------------------------------- key-value

  async kvGet(key: string, now: number): Promise<string | null> {
    return this.write(() => {
      const row = this.s.kvGet.get(key) as { value: string; expires_at: number | null } | undefined;
      if (!row) return null;
      if (row.expires_at !== null && row.expires_at <= now) {
        this.s.kvDelete.run(key);
        return null;
      }
      return row.value;
    });
  }

  async kvGetMany(keys: string[], now: number): Promise<(string | null)[]> {
    return this.write(() =>
      keys.map((key) => {
        const row = this.s.kvGet.get(key) as { value: string; expires_at: number | null } | undefined;
        if (!row) return null;
        if (row.expires_at !== null && row.expires_at <= now) {
          this.s.kvDelete.run(key);
          return null;
        }
        return row.value;
      }),
    );
  }

  async kvSet(key: string, value: string, expiresAt: number | null): Promise<void> {
    await this.write(() => {
      this.s.kvSet.run(key, value, expiresAt);
    });
  }

  async kvDelete(key: string): Promise<boolean> {
    return this.write(() => this.s.kvDelete.run(key).changes === 1);
  }

  async kvSweep(now: number, limit: number): Promise<number> {
    return this.write(() => this.s.kvSweep.run(now, limit).changes);
  }

  // ---------------------------------------------------------------- internals

  private require(): Db {
    if (!this.db) throw new StorageError('SQLite storage is not open.');
    return this.db;
  }

  /** Runs a read-only operation, translating driver errors into StorageError. */
  private run<T>(fn: () => T): Promise<T> {
    return this.guard(fn);
  }

  /** Runs a write operation inside a `BEGIN IMMEDIATE` transaction. */
  private write<T>(fn: () => T): Promise<T> {
    return this.guard(() => this.require().transaction(fn).immediate());
  }

  private guard<T>(fn: () => T): Promise<T> {
    try {
      return Promise.resolve(fn());
    } catch (cause) {
      if (cause instanceof StorageError || cause instanceof DuplicateJobError) {
        return Promise.reject(cause);
      }
      return Promise.reject(new StorageError(`SQLite operation failed: ${redactSecrets((cause as Error).message)}`, cause));
    }
  }

  private isPausedInTx(queue: string): boolean {
    const row = this.s.isPaused.get(queue) as { paused: number } | undefined;
    return row?.paused === 1;
  }

  private trimInTx(queue: string, state: 'completed' | 'failed', keep: number): number {
    return this.s.trim.run(queue, state, keep).changes;
  }

  private prepareFilesystem(): void {
    const dir = dirname(this.path);
    if (existsSync(this.path) && statSync(this.path).isDirectory()) {
      throw new ConfigError(`storage.path "${this.path}" is a directory. Provide a file path such as "./data/fluxbase.db".`);
    }
    try {
      mkdirSync(dir, { recursive: true, mode: 0o700 });
    } catch (cause) {
      throw new ConfigError(`Cannot create directory "${dir}" for the SQLite database: ${(cause as Error).message}`);
    }
  }

  private migrate(db: Db): void {
    const version = db.pragma('user_version', { simple: true }) as number;
    if (version > SCHEMA_VERSION) {
      throw new StorageError(
        `The database schema version (${version}) is newer than this Fluxbase release supports (${SCHEMA_VERSION}). Upgrade Fluxbase.`,
      );
    }
    if (version < SCHEMA_VERSION) {
      db.transaction(() => {
        db.exec(SCHEMA_V1);
        db.pragma(`user_version = ${SCHEMA_VERSION}`);
      })();
    }
  }

  private prepareStatements(db: Db) {
    return {
      insertJob: db.prepare(`INSERT INTO jobs (queue, id, name, data, state, priority, attempts, attempts_made, backoff,
        timeout_ms, created_at, run_at, metadata, remove_on_complete, remove_on_fail)
        VALUES (@queue, @id, @name, @data, @state, @priority, @attempts, 0, @backoff, @timeout_ms, @created_at, @run_at,
        @metadata, @remove_on_complete, @remove_on_fail)`),
      insertJobIgnoreDuplicate: db.prepare(`INSERT OR IGNORE INTO jobs (queue, id, name, data, state, priority, attempts,
        attempts_made, backoff, timeout_ms, created_at, run_at, metadata, remove_on_complete, remove_on_fail)
        VALUES (@queue, @id, @name, @data, @state, @priority, @attempts, 0, @backoff, @timeout_ms, @created_at, @run_at,
        @metadata, @remove_on_complete, @remove_on_fail)`),
      getJob: db.prepare(`SELECT ${JOB_COLUMNS} FROM jobs WHERE queue = ? AND id = ?`),
      listWaiting: db.prepare(`SELECT ${JOB_COLUMNS} FROM jobs WHERE queue = ? AND state = 'waiting'
        ORDER BY priority DESC, rowid ASC LIMIT ?`),
      listDelayed: db.prepare(`SELECT ${JOB_COLUMNS} FROM jobs WHERE queue = ? AND state = 'delayed'
        ORDER BY run_at ASC LIMIT ?`),
      listActive: db.prepare(`SELECT ${JOB_COLUMNS} FROM jobs WHERE queue = ? AND state = 'active'
        ORDER BY locked_until ASC LIMIT ?`),
      listFinished: db.prepare(`SELECT ${JOB_COLUMNS} FROM jobs WHERE queue = ? AND state = ?
        ORDER BY finished_at DESC LIMIT ?`),
      countJobs: db.prepare('SELECT state, COUNT(*) AS n FROM jobs WHERE queue = ? GROUP BY state'),
      promoteDelayed: db.prepare(`UPDATE jobs SET state = 'waiting'
        WHERE rowid IN (SELECT rowid FROM jobs WHERE queue = ? AND state = 'delayed' AND run_at <= ? LIMIT 500)`),
      expiredLeases: db.prepare(`SELECT id, attempts, attempts_made FROM jobs
        WHERE queue = ? AND state = 'active' AND locked_until <= ? LIMIT 100`),
      failExpired: db.prepare(`UPDATE jobs SET state = 'failed', finished_at = ?, failed_reason = ?,
        lock_token = NULL, locked_until = NULL, worker_id = NULL
        WHERE queue = ? AND id = ? AND state = 'active'`),
      requeueExpired: db.prepare(`UPDATE jobs SET state = 'waiting', lock_token = NULL, locked_until = NULL, worker_id = NULL
        WHERE queue = ? AND id = ? AND state = 'active'`),
      isPaused: db.prepare('SELECT paused FROM queue_control WHERE queue = ?'),
      readyIds: db.prepare(`SELECT id FROM jobs WHERE queue = ? AND state = 'waiting'
        ORDER BY priority DESC, rowid ASC LIMIT ?`),
      claim: db.prepare(`UPDATE jobs SET state = 'active', lock_token = ?, locked_until = ?, worker_id = ?,
        attempts_made = attempts_made + 1, processed_at = ?
        WHERE queue = ? AND id = ? AND state = 'waiting' RETURNING ${JOB_COLUMNS}`),
      renewLease: db.prepare(`UPDATE jobs SET locked_until = ?
        WHERE queue = ? AND id = ? AND state = 'active' AND lock_token = ?`),
      completeLease: db.prepare(`UPDATE jobs SET state = 'completed', finished_at = ?, return_value = ?,
        lock_token = NULL, locked_until = NULL, worker_id = NULL
        WHERE queue = ? AND id = ? AND state = 'active' AND lock_token = ?
        RETURNING remove_on_complete`),
      lockedForFailure: db.prepare(`SELECT remove_on_fail FROM jobs
        WHERE queue = ? AND id = ? AND state = 'active' AND lock_token = ?`),
      retryLater: db.prepare(`UPDATE jobs SET state = ?, run_at = ?, failed_reason = ?,
        lock_token = NULL, locked_until = NULL, worker_id = NULL
        WHERE queue = ? AND id = ?`),
      finalFail: db.prepare(`UPDATE jobs SET state = 'failed', finished_at = ?, failed_reason = ?,
        lock_token = NULL, locked_until = NULL, worker_id = NULL
        WHERE queue = ? AND id = ?`),
      releaseLease: db.prepare(`UPDATE jobs SET state = 'waiting', attempts_made = MAX(attempts_made - 1, 0),
        lock_token = NULL, locked_until = NULL, worker_id = NULL
        WHERE queue = ? AND id = ? AND state = 'active' AND lock_token = ?`),
      stateOf: db.prepare('SELECT state FROM jobs WHERE queue = ? AND id = ?'),
      resetFailed: db.prepare(`UPDATE jobs SET state = 'waiting', attempts_made = 0, failed_reason = NULL,
        finished_at = NULL, run_at = ? WHERE queue = ? AND id = ? AND state = 'failed'`),
      deleteJob: db.prepare('DELETE FROM jobs WHERE queue = ? AND id = ?'),
      trim: db.prepare(`DELETE FROM jobs WHERE rowid IN (
        SELECT rowid FROM jobs WHERE queue = ? AND state = ? ORDER BY finished_at DESC LIMIT -1 OFFSET ?)`),
      setPaused: db.prepare(`INSERT INTO queue_control (queue, paused) VALUES (?, ?)
        ON CONFLICT (queue) DO UPDATE SET paused = excluded.paused`),
      putSchedule: db.prepare(`INSERT INTO schedules (queue, name, cron, timezone, data, job_options, next_run_at,
        created_at, updated_at) VALUES (@queue, @name, @cron, @timezone, @data, @job_options, @next_run_at,
        @created_at, @updated_at)
        ON CONFLICT (queue, name) DO UPDATE SET cron = excluded.cron, timezone = excluded.timezone,
        data = excluded.data, job_options = excluded.job_options, next_run_at = excluded.next_run_at,
        updated_at = excluded.updated_at`),
      getSchedule: db.prepare(`SELECT queue, name, cron, timezone, data, job_options, next_run_at, last_run_at,
        created_at, updated_at FROM schedules WHERE queue = ? AND name = ?`),
      listSchedules: db.prepare(`SELECT queue, name, cron, timezone, data, job_options, next_run_at, last_run_at,
        created_at, updated_at FROM schedules WHERE queue = ? ORDER BY name`),
      removeSchedule: db.prepare('DELETE FROM schedules WHERE queue = ? AND name = ?'),
      dueSchedules: db.prepare(`SELECT queue, name, cron, timezone, data, job_options, next_run_at, last_run_at,
        created_at, updated_at FROM schedules WHERE next_run_at <= ? ORDER BY next_run_at LIMIT ?`),
      advanceSchedule: db.prepare(`UPDATE schedules SET next_run_at = ?, last_run_at = ?, updated_at = ?
        WHERE queue = ? AND name = ? AND next_run_at = ?`),
      kvGet: db.prepare('SELECT value, expires_at FROM kv WHERE key = ?'),
      kvSet: db.prepare(`INSERT INTO kv (key, value, expires_at) VALUES (?, ?, ?)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at`),
      kvDelete: db.prepare('DELETE FROM kv WHERE key = ?'),
      kvSweep: db.prepare(`DELETE FROM kv WHERE key IN (
        SELECT key FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ? LIMIT ?)`),
    };
  }
}
