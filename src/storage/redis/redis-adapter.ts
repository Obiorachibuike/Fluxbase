import { Redis } from 'ioredis';
import { DuplicateJobError, RedisConnectionError, StorageError, redactSecrets, redactUrl } from '../../core/errors.js';
import type { JobState } from '../../queue/job.js';
import { newId } from '../../utils/ids.js';
import { REDIS_CAPABILITIES, type StorageCapabilities } from '../capabilities.js';
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
import {
  CLAIM_SCRIPT,
  COMPLETE_SCRIPT,
  FAIL_SCRIPT,
  FIRE_SCHEDULE_SCRIPT,
  INSERT_SCRIPT,
  PUT_SCHEDULE_SCRIPT,
  REMOVE_SCHEDULE_SCRIPT,
  REMOVE_SCRIPT,
  RELEASE_SCRIPT,
  RENEW_SCRIPT,
  RETRY_SCRIPT,
} from './scripts.js';

type LogFn = (level: 'warn' | 'error', message: string, meta?: Record<string, unknown>) => void;

const TRIM_SCRIPT = `
local zkey = KEYS[1]
local jp = ARGV[1]
local keep = tonumber(ARGV[2])
local excess = redis.call('ZCARD', zkey) - keep
if excess <= 0 then return 0 end
local ids = redis.call('ZRANGE', zkey, 0, excess - 1)
for _, id in ipairs(ids) do
  redis.call('DEL', jp .. id)
end
redis.call('ZREMRANGEBYRANK', zkey, 0, excess - 1)
return excess
`;

/** Lua scripts and the number of KEYS each one expects (the remaining arguments are ARGV). */
const SCRIPTS = {
  fbClaim: { lua: CLAIM_SCRIPT, numberOfKeys: 5 },
  fbRenew: { lua: RENEW_SCRIPT, numberOfKeys: 1 },
  fbComplete: { lua: COMPLETE_SCRIPT, numberOfKeys: 2 },
  fbFail: { lua: FAIL_SCRIPT, numberOfKeys: 4 },
  fbRelease: { lua: RELEASE_SCRIPT, numberOfKeys: 2 },
  fbRetry: { lua: RETRY_SCRIPT, numberOfKeys: 2 },
  fbRemove: { lua: REMOVE_SCRIPT, numberOfKeys: 5 },
  fbInsert: { lua: INSERT_SCRIPT, numberOfKeys: 3 },
  fbFireSchedule: { lua: FIRE_SCHEDULE_SCRIPT, numberOfKeys: 5 },
  fbPutSchedule: { lua: PUT_SCHEDULE_SCRIPT, numberOfKeys: 3 },
  fbRemoveSchedule: { lua: REMOVE_SCHEDULE_SCRIPT, numberOfKeys: 3 },
  fbTrim: { lua: TRIM_SCRIPT, numberOfKeys: 1 },
} as const;

type ScriptName = keyof typeof SCRIPTS;
type ScriptCommands = Record<ScriptName, (...args: (string | number)[]) => Promise<unknown>>;

/** Redis hash fields are strings. Empty string encodes null. */
type HashFields = Record<string, string>;

const nullable = (v: string | undefined): string | null => (v === undefined || v === '' ? null : v);
const nullableNumber = (v: string | undefined): number | null => (v === undefined || v === '' ? null : Number(v));

function flatToObject(flat: unknown): HashFields {
  const out: HashFields = {};
  const list = flat as string[];
  for (let i = 0; i + 1 < list.length; i += 2) {
    out[String(list[i])] = String(list[i + 1]);
  }
  return out;
}

function hashToJob(h: HashFields): JobRecord {
  return {
    id: h['id'] ?? '',
    queue: h['queue'] ?? '',
    name: h['name'] ?? '',
    data: h['data'] ?? 'null',
    state: (h['state'] ?? 'waiting') as JobState,
    priority: Number(h['priority'] ?? 0),
    attempts: Number(h['attempts'] ?? 1),
    attemptsMade: Number(h['attempts_made'] ?? 0),
    backoff: nullable(h['backoff']),
    timeoutMs: nullableNumber(h['timeout_ms']),
    createdAt: Number(h['created_at'] ?? 0),
    runAt: Number(h['run_at'] ?? 0),
    processedAt: nullableNumber(h['processed_at']),
    finishedAt: nullableNumber(h['finished_at']),
    failedReason: nullable(h['failed_reason']),
    returnValue: nullable(h['return_value']),
    metadata: h['metadata'] ?? '{}',
    lockToken: nullable(h['lock_token']),
    lockedUntil: nullableNumber(h['locked_until']),
    workerId: nullable(h['worker_id']),
  };
}

function hashToSchedule(h: HashFields): ScheduleRecord {
  return {
    queue: h['queue'] ?? '',
    name: h['name'] ?? '',
    cron: h['cron'] ?? '',
    timezone: h['timezone'] ?? 'UTC',
    data: h['data'] ?? 'null',
    jobOptions: h['job_options'] ?? '{}',
    nextRunAt: Number(h['next_run_at'] ?? 0),
    lastRunAt: nullableNumber(h['last_run_at']),
    createdAt: Number(h['created_at'] ?? 0),
    updatedAt: Number(h['updated_at'] ?? 0),
  };
}

function jobToFields(job: NewJobRecord): HashFields {
  return {
    id: job.id,
    queue: job.queue,
    name: job.name,
    data: job.data,
    state: job.state,
    priority: String(job.priority),
    attempts: String(job.attempts),
    attempts_made: '0',
    backoff: job.backoff ?? '',
    timeout_ms: job.timeoutMs === null ? '' : String(job.timeoutMs),
    created_at: String(job.createdAt),
    run_at: String(job.runAt),
    processed_at: '',
    finished_at: '',
    failed_reason: '',
    return_value: '',
    metadata: job.metadata,
    lock_token: '',
    locked_until: '',
    worker_id: '',
    remove_on_complete: job.removeOnComplete ? '1' : '0',
    remove_on_fail: job.removeOnFail ? '1' : '0',
  };
}

/**
 * Redis implementation of {@link StorageAdapter}.
 *
 * Data model (keys share the configured `keyPrefix`, default `fluxbase`):
 * - `:q:<queue>:job:<id>`      hash with the job fields
 * - `:q:<queue>:waiting|delayed|active|completed|failed`  sorted sets of job ids
 *   (waiting scores encode priority then FIFO; delayed scores are run times;
 *    active scores are lease expiries)
 * - `:q:<queue>:paused`, `:q:<queue>:seq`
 * - `:s:<queue>:<name>`, `:schedules`, `:schedidx:<queue>` for recurring schedules
 * - `:kv:<key>` strings with native expiry (PXAT)
 *
 * Every state transition runs as one Lua script, so no other client can interleave
 * a read and write inside a transition. This works on standalone Redis and on
 * single-primary deployments. Redis Cluster is not supported, because scripts touch
 * keys that hash to different slots.
 */
export class RedisStorageAdapter implements StorageAdapter {
  readonly kind = 'redis' as const;
  readonly capabilities: StorageCapabilities = REDIS_CAPABILITIES;
  private client: Redis | null = null;
  private sub: Redis | null = null;
  private started = false;
  private readonly channels = new Map<string, Set<(message: string) => void>>();

  constructor(
    private readonly url: string,
    private readonly keyPrefix: string,
    private readonly connectTimeoutMs: number,
    private readonly log: LogFn,
  ) {}

  // ------------------------------------------------------------ lifecycle

  async open(): Promise<void> {
    if (this.client) return;
    const client = new Redis(this.url, {
      lazyConnect: true,
      connectTimeout: this.connectTimeoutMs,
      enableReadyCheck: true,
      maxRetriesPerRequest: 3,
      // Before the first successful connection we fail fast. Afterwards we keep reconnecting.
      retryStrategy: (times: number) => (this.started ? Math.min(100 * 2 ** Math.min(times, 8), 2000) : null),
    });
    client.on('error', (error: Error) => {
      this.log('warn', 'Redis connection error (will retry)', { error: redactSecrets(error.message) });
    });
    for (const [name, definition] of Object.entries(SCRIPTS)) {
      client.defineCommand(name, definition);
    }
    try {
      await withTimeout(client.connect(), this.connectTimeoutMs, 'connect');
      await client.ping();
    } catch (cause) {
      client.disconnect();
      throw new RedisConnectionError(
        `Could not connect to Redis at ${redactUrl(this.url)} within ${this.connectTimeoutMs} ms (${redactSecrets(
          (cause as Error).message,
        )}). Check that Redis is running, that the URL is correct, and that TLS and credentials match the server. ` +
          'Fluxbase does not fall back to memory or SQLite when Redis is unavailable.',
        cause,
      );
    }
    this.client = client;
    this.started = true;
    this.sub = client.duplicate();
    this.sub.on('message', (channel: string, message: string) => this.dispatch(channel, message));
    this.sub.on('error', (error: Error) => {
      this.log('warn', 'Redis subscriber error (will retry)', { error: redactSecrets(error.message) });
    });
    await this.resubscribeAll();
  }

  async close(): Promise<void> {
    const client = this.client;
    const sub = this.sub;
    this.client = null;
    this.sub = null;
    this.started = false;
    this.channels.clear();
    if (sub) {
      await sub.quit().catch(() => sub.disconnect());
    }
    if (client) {
      await client.quit().catch(() => client.disconnect());
    }
  }

  async ping(): Promise<void> {
    await this.c().ping();
  }

  // ------------------------------------------------------------ jobs

  async insertJob(job: NewJobRecord): Promise<JobRecord> {
    const inserted = await this.exec(() =>
      this.s().fbInsert(
        this.key(job.queue, 'waiting'),
        this.key(job.queue, 'delayed'),
        this.key(job.queue, 'seq'),
        this.jobPrefix(job.queue),
        job.id,
        JSON.stringify(jobToFields(job)),
      ),
    );
    if (Number(inserted) !== 1) {
      throw new DuplicateJobError(job.queue, job.id);
    }
    const job2 = await this.getJob(job.queue, job.id);
    if (!job2) throw new StorageError(`Job "${job.id}" disappeared immediately after insert.`);
    return job2;
  }

  async getJob(queue: string, id: string): Promise<JobRecord | null> {
    const h = await this.exec(() => this.c().hgetall(this.jobKey(queue, id)));
    return Object.keys(h).length === 0 ? null : hashToJob(h);
  }

  async listJobs(queue: string, state: JobState, limit: number): Promise<JobRecord[]> {
    const finished = state === 'completed' || state === 'failed';
    const ids: string[] = await this.exec(() =>
      finished
        ? this.c().zrevrange(this.key(queue, state), "0", String(limit - 1))
        : this.c().zrange(this.key(queue, state), "0", String(limit - 1)),
    );
    const jobs: JobRecord[] = [];
    for (const id of ids) {
      const job = await this.getJob(queue, id);
      if (job) jobs.push(job);
    }
    return jobs;
  }

  async countJobs(queue: string): Promise<Record<JobState, number>> {
    const states: JobState[] = ['waiting', 'delayed', 'active', 'completed', 'failed'];
    const counts = await this.exec(() => {
      const pipeline = this.c().pipeline();
      for (const state of states) pipeline.zcard(this.key(queue, state));
      return pipeline.exec();
    });
    const out: Record<JobState, number> = { waiting: 0, delayed: 0, active: 0, completed: 0, failed: 0 };
    states.forEach((state, i) => {
      const entry = counts?.[i];
      out[state] = entry && !entry[0] ? Number(entry[1]) : 0;
    });
    return out;
  }

  async claimJobs(args: { queue: string; workerId: string; limit: number; leaseMs: number; now: number }): Promise<JobRecord[]> {
    const { queue, workerId, limit, leaseMs, now } = args;
    const result = await this.exec(() =>
      this.s().fbClaim(
        this.key(queue, 'waiting'),
        this.key(queue, 'delayed'),
        this.key(queue, 'active'),
        this.key(queue, 'paused'),
        this.key(queue, 'failed'),
        this.jobPrefix(queue),
        String(now),
        String(leaseMs),
        String(limit),
        workerId,
        newId(),
      ),
    );
    return (result as unknown[]).map((flat) => hashToJob(flatToObject(flat)));
  }

  async renewLeases(queue: string, leases: Lease[], leaseMs: number, now: number): Promise<string[]> {
    if (leases.length === 0) return [];
    const pairs = leases.flatMap((lease) => [lease.id, lease.token]);
    const renewed = await this.exec(() =>
      this.s().fbRenew(this.key(queue, 'active'), this.jobPrefix(queue), String(now), String(leaseMs), ...pairs),
    );
    return renewed as string[];
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
    const result = await this.exec(() =>
      this.s().fbComplete(
        this.key(queue, 'active'),
        this.key(queue, 'completed'),
        this.jobPrefix(queue),
        id,
        token,
        String(now),
        returnValue ?? '',
        String(keepCompleted),
      ),
    );
    return Number(result) === 1;
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
    const result = await this.exec(() =>
      this.s().fbFail(
        this.key(queue, 'active'),
        this.key(queue, 'waiting'),
        this.key(queue, 'delayed'),
        this.key(queue, 'failed'),
        this.jobPrefix(queue),
        id,
        token,
        String(now),
        retryAt === null ? '' : String(retryAt),
        reason,
        String(keepFailed),
      ),
    );
    return result as FailResult;
  }

  async releaseJob(queue: string, id: string, token: string): Promise<boolean> {
    const result = await this.exec(() =>
      this.s().fbRelease(this.key(queue, 'active'), this.key(queue, 'waiting'), this.jobPrefix(queue), id, token),
    );
    return Number(result) === 1;
  }

  async retryJob(queue: string, id: string, now: number): Promise<RetryResult> {
    const result = await this.exec(() =>
      this.s().fbRetry(this.key(queue, 'failed'), this.key(queue, 'waiting'), this.jobPrefix(queue), id, String(now)),
    );
    return result as RetryResult;
  }

  async removeJob(queue: string, id: string): Promise<RemoveResult> {
    const result = await this.exec(() =>
      this.s().fbRemove(
        this.key(queue, 'waiting'),
        this.key(queue, 'delayed'),
        this.key(queue, 'completed'),
        this.key(queue, 'failed'),
        this.key(queue, 'active'),
        this.jobPrefix(queue),
        id,
      ),
    );
    return result as RemoveResult;
  }

  async trimFinished(queue: string, state: 'completed' | 'failed', keep: number): Promise<number> {
    const result = await this.exec(() =>
      this.s().fbTrim(this.key(queue, state), this.jobPrefix(queue), String(keep)),
    );
    return Number(result);
  }

  async setPaused(queue: string, paused: boolean): Promise<void> {
    await this.exec(async () => {
      if (paused) {
        await this.c().set(this.key(queue, 'paused'), '1');
      } else {
        await this.c().del(this.key(queue, 'paused'));
      }
    });
  }

  async isPaused(queue: string): Promise<boolean> {
    return (await this.exec(() => this.c().exists(this.key(queue, 'paused')))) === 1;
  }

  async notifyQueue(queue: string): Promise<void> {
    await this.exec(() => this.c().publish(this.channel('notify', queue), ''));
  }

  async subscribeQueue(queue: string, callback: () => void): Promise<() => void> {
    return this.subscribeChannel(this.channel('notify', queue), () => callback());
  }

  // ------------------------------------------------------------ schedules

  async putSchedule(record: ScheduleRecord): Promise<void> {
    const fields: HashFields = {
      queue: record.queue,
      name: record.name,
      cron: record.cron,
      timezone: record.timezone,
      data: record.data,
      job_options: record.jobOptions,
      next_run_at: String(record.nextRunAt),
      last_run_at: record.lastRunAt === null ? '' : String(record.lastRunAt),
      created_at: String(record.createdAt),
      updated_at: String(record.updatedAt),
    };
    const flat = Object.entries(fields).flat();
    await this.exec(() =>
      this.s().fbPutSchedule(
        this.scheduleKey(record.queue, record.name),
        this.schedulesIndex(),
        this.scheduleIndexKey(record.queue),
        JSON.stringify(flat),
        String(record.nextRunAt),
        this.scheduleMember(record.queue, record.name),
        record.name,
      ),
    );
  }

  async getSchedule(queue: string, name: string): Promise<ScheduleRecord | null> {
    const h = await this.exec(() => this.c().hgetall(this.scheduleKey(queue, name)));
    return Object.keys(h).length === 0 ? null : hashToSchedule(h);
  }

  async listSchedules(queue: string): Promise<ScheduleRecord[]> {
    const names = await this.exec(() => this.c().smembers(this.scheduleIndexKey(queue)));
    const out: ScheduleRecord[] = [];
    for (const name of names.sort()) {
      const schedule = await this.getSchedule(queue, name);
      if (schedule) out.push(schedule);
    }
    return out;
  }

  async removeSchedule(queue: string, name: string): Promise<boolean> {
    const result = await this.exec(() =>
      this.s().fbRemoveSchedule(
        this.scheduleKey(queue, name),
        this.schedulesIndex(),
        this.scheduleIndexKey(queue),
        this.scheduleMember(queue, name),
        name,
      ),
    );
    return Number(result) === 1;
  }

  async listDueSchedules(now: number, limit: number): Promise<ScheduleRecord[]> {
    const members = await this.exec(() =>
      this.c().zrangebyscore(this.schedulesIndex(), '-inf', String(now), 'LIMIT', 0, limit),
    );
    const out: ScheduleRecord[] = [];
    for (const member of members) {
      const sep = member.indexOf('|');
      const schedule = await this.getSchedule(member.slice(0, sep), member.slice(sep + 1));
      if (schedule) out.push(schedule);
    }
    return out;
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
    const result = await this.exec(() =>
      this.s().fbFireSchedule(
        this.scheduleKey(queue, name),
        this.schedulesIndex(),
        this.key(queue, 'waiting'),
        this.key(queue, 'delayed'),
        this.key(queue, 'seq'),
        this.jobPrefix(queue),
        String(expectedNextRunAt),
        String(nextRunAt),
        String(now),
        job.id,
        JSON.stringify(jobToFields(job)),
        this.scheduleMember(queue, name),
      ),
    );
    return Number(result) === 1;
  }

  // ------------------------------------------------------------ key-value

  async kvGet(key: string): Promise<string | null> {
    return this.exec(() => this.c().get(this.kvKey(key)));
  }

  async kvGetMany(keys: string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    return this.exec(() => this.c().mget(...keys.map((k) => this.kvKey(k))));
  }

  async kvSet(key: string, value: string, expiresAt: number | null): Promise<void> {
    await this.exec(async () => {
      if (expiresAt === null) {
        await this.c().set(this.kvKey(key), value);
      } else {
        await this.c().set(this.kvKey(key), value, 'PXAT', expiresAt);
      }
    });
  }

  async kvDelete(key: string): Promise<boolean> {
    return (await this.exec(() => this.c().del(this.kvKey(key)))) === 1;
  }

  /** Redis expires keys natively, so no sweep is needed. */
  async kvSweep(): Promise<number> {
    return 0;
  }

  // ------------------------------------------------------------ broadcast

  async publishBroadcast(topic: string, message: string): Promise<void> {
    await this.exec(() => this.c().publish(this.channel('events', topic), message));
  }

  async subscribeBroadcast(topic: string, callback: (message: string) => void): Promise<() => void> {
    return this.subscribeChannel(this.channel('events', topic), callback);
  }

  // ------------------------------------------------------------ internals

  private c(): Redis {
    if (!this.client) throw new RedisConnectionError('Redis storage is not connected. Call fluxbase.start() first.');
    return this.client;
  }

  private s(): ScriptCommands {
    return this.c() as unknown as ScriptCommands;
  }

  /** Runs a Redis operation and maps driver errors to Fluxbase errors. */
  private async exec<T>(fn: () => T | Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (cause) {
      if (cause instanceof StorageError || cause instanceof DuplicateJobError || cause instanceof RedisConnectionError) {
        throw cause;
      }
      const message = redactSecrets((cause as Error).message ?? String(cause));
      const code = (cause as { code?: string }).code;
      if (code === 'ECONNREFUSED' || code === 'ETIMEDOUT' || /Connection is closed|connect|timed out/i.test(message)) {
        throw new RedisConnectionError(`Redis operation failed because the connection is unavailable: ${message}`, cause);
      }
      throw new StorageError(`Redis operation failed: ${message}`, cause);
    }
  }

  private key(queue: string, part: string): string {
    return `${this.keyPrefix}:q:${queue}:${part}`;
  }

  private jobPrefix(queue: string): string {
    return `${this.keyPrefix}:q:${queue}:job:`;
  }

  private jobKey(queue: string, id: string): string {
    return `${this.jobPrefix(queue)}${id}`;
  }

  private scheduleKey(queue: string, name: string): string {
    return `${this.keyPrefix}:s:${queue}:${name}`;
  }

  private schedulesIndex(): string {
    return `${this.keyPrefix}:schedules`;
  }

  private scheduleIndexKey(queue: string): string {
    return `${this.keyPrefix}:schedidx:${queue}`;
  }

  private scheduleMember(queue: string, name: string): string {
    return `${queue}|${name}`;
  }

  private kvKey(key: string): string {
    return `${this.keyPrefix}:kv:${key}`;
  }

  private channel(kind: 'notify' | 'events', name: string): string {
    return `${this.keyPrefix}:${kind}:${name}`;
  }

  private async subscribeChannel(channel: string, callback: (message: string) => void): Promise<() => void> {
    let listeners = this.channels.get(channel);
    if (!listeners) {
      listeners = new Set();
      this.channels.set(channel, listeners);
      if (this.sub) {
        await this.exec(() => this.sub!.subscribe(channel));
      }
    }
    listeners.add(callback);
    return () => {
      const current = this.channels.get(channel);
      if (!current) return;
      current.delete(callback);
      if (current.size === 0) {
        this.channels.delete(channel);
        this.sub?.unsubscribe(channel).catch(() => undefined);
      }
    };
  }

  private async resubscribeAll(): Promise<void> {
    if (!this.sub || this.channels.size === 0) return;
    await this.sub.subscribe(...this.channels.keys());
  }

  private dispatch(channel: string, message: string): void {
    const listeners = this.channels.get(channel);
    if (!listeners) return;
    for (const listener of listeners) {
      try {
        listener(message);
      } catch (error) {
        this.log('error', 'Redis subscription listener threw', { error: error as Error });
      }
    }
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
