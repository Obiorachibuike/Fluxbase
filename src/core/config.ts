import { ConfigError } from './errors.js';
import { limits } from '../utils/ids.js';
import type { Logger, LogLevel } from '../observability/logger.js';

/**
 * - `embedded`: durable, single-node. Uses a SQLite file. No Redis required.
 * - `distributed`: shared coordination through Redis. Multiple processes may share queues.
 * - `memory`: volatile SQLite `:memory:` database. Data is lost when the process exits.
 */
export type FluxbaseMode = 'embedded' | 'distributed' | 'memory';

export interface SqliteStorageOptions {
  type: 'sqlite';
  /** Path to the SQLite database file. Parent directories are created if missing. */
  path: string;
}

export interface RedisStorageOptions {
  type: 'redis';
  /** Connection URL, e.g. `rediss://user:pass@host:6380/0`. Never logged in full. */
  url: string;
  /** Namespace for all Redis keys and channels. Default: `fluxbase`. */
  keyPrefix?: string;
  /** Connection timeout in ms for the initial connection. Default: 5000. */
  connectTimeoutMs?: number;
}

export type StorageOptions = SqliteStorageOptions | RedisStorageOptions;

export interface BackoffOptions {
  /** `fixed` waits `delay` ms; `exponential` waits `delay * 2^(attemptsMade - 1)` ms. */
  type: 'fixed' | 'exponential';
  /** Base delay in milliseconds. */
  delay: number;
  /** Upper bound for any computed delay. Default: 24 hours. */
  maxDelay?: number;
}

export interface JobDefaults {
  /** Maximum total attempts, including the first. Default: 1 (no automatic retries). */
  attempts?: number;
  backoff?: BackoffOptions;
  /** Per-attempt timeout in ms. Default: no timeout. */
  timeout?: number;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
}

export interface FluxbaseOptions {
  mode?: FluxbaseMode;
  storage?: StorageOptions;
  defaults?: JobDefaults;
  logging?: {
    level?: LogLevel;
    logger?: Logger;
  };
  worker?: {
    /** Default number of jobs one worker processes at once. Default: 1. */
    concurrency?: number;
    /** Lease duration in ms. Renewed by heartbeat while a job runs. Default: 30000. */
    leaseMs?: number;
    /** Maximum idle wait between polls in ms. Default: 1000. */
    pollIntervalMs?: number;
    /** How long `close()` waits for active jobs before aborting them. Default: 10000. */
    shutdownTimeoutMs?: number;
  };
  scheduler?: {
    /** Run recurring schedules. Default: true. */
    enabled?: boolean;
    /** How often due schedules are checked, in ms. Default: 1000. */
    tickMs?: number;
  };
  kv?: {
    /** Prefix applied to every key stored by `fluxbase.kv`. Default: none. */
    prefix?: string;
    /** How often expired embedded KV rows are swept, in ms. Default: 60000. */
    sweepIntervalMs?: number;
    /** Maximum serialized value size in bytes. Default: 1 MiB. */
    maxValueBytes?: number;
  };
  /** Default retention for finished jobs per queue. Counts are the newest N jobs kept. */
  retention?: {
    completed?: number;
    failed?: number;
  };
}

export interface ResolvedConfig {
  mode: FluxbaseMode;
  storage:
    | { kind: 'sqlite'; path: string; volatile: boolean }
    | { kind: 'redis'; url: string; keyPrefix: string; connectTimeoutMs: number };
  defaults: Required<Pick<JobDefaults, 'attempts' | 'removeOnComplete' | 'removeOnFail'>> & {
    backoff: BackoffOptions | null;
    timeout: number | null;
  };
  logging: { level: LogLevel; logger: Logger | null };
  worker: { concurrency: number; leaseMs: number; pollIntervalMs: number; shutdownTimeoutMs: number };
  scheduler: { enabled: boolean; tickMs: number };
  kv: { prefix: string; sweepIntervalMs: number; maxValueBytes: number };
  retention: { completed: number; failed: number };
}

const DEFAULT_SQLITE_PATH = './data/fluxbase.db';
const KEY_PREFIX_PATTERN = /^[A-Za-z0-9_.-]{1,64}$/;
const LOG_LEVELS: readonly LogLevel[] = ['silent', 'error', 'warn', 'info', 'debug'];

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function int(name: string, value: unknown, min: number, max: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw new ConfigError(`${name} must be an integer between ${min} and ${max}. Received: ${String(value)}.`);
  }
  return value;
}

/** Validates a backoff configuration. Throws ConfigError on invalid input. */
export function validateBackoff(backoff: unknown, name = 'backoff'): BackoffOptions {
  if (!isObject(backoff)) {
    throw new ConfigError(`${name} must be an object such as { type: "exponential", delay: 1000 }.`);
  }
  if (backoff.type !== 'fixed' && backoff.type !== 'exponential') {
    throw new ConfigError(`${name}.type must be "fixed" or "exponential".`);
  }
  const delay = int(`${name}.delay`, backoff.delay, 0, limits.maxDelayMs);
  const maxDelay =
    backoff.maxDelay === undefined ? 24 * 60 * 60 * 1000 : int(`${name}.maxDelay`, backoff.maxDelay, 0, limits.maxDelayMs);
  return { type: backoff.type, delay, maxDelay };
}

/** Validates user options and fills in documented defaults. */
export function resolveConfig(input: FluxbaseOptions | undefined): ResolvedConfig {
  if (input !== undefined && !isObject(input)) {
    throw new ConfigError('Fluxbase options must be an object.');
  }
  const options = (input ?? {}) as FluxbaseOptions;

  const mode = options.mode ?? 'embedded';
  if (mode !== 'embedded' && mode !== 'distributed' && mode !== 'memory') {
    throw new ConfigError(`mode must be "embedded", "distributed", or "memory". Received: ${String(mode)}.`);
  }

  let storage: ResolvedConfig['storage'];
  if (mode === 'embedded') {
    const s = (options.storage ?? { type: 'sqlite', path: DEFAULT_SQLITE_PATH }) as StorageOptions;
    if (s.type !== 'sqlite') {
      throw new ConfigError('mode "embedded" requires storage.type "sqlite". Use mode "distributed" for Redis.');
    }
    storage = { kind: 'sqlite', path: validateSqlitePath(s.path), volatile: false };
  } else if (mode === 'memory') {
    if (options.storage !== undefined) {
      throw new ConfigError('mode "memory" does not accept storage options. Data is volatile and held in process memory.');
    }
    storage = { kind: 'sqlite', path: ':memory:', volatile: true };
  } else {
    const s = options.storage;
    if (!s) {
      throw new ConfigError('mode "distributed" requires storage: { type: "redis", url: process.env.REDIS_URL }.');
    }
    if (s.type !== 'redis') {
      throw new ConfigError('mode "distributed" requires storage.type "redis". SQLite is only supported in embedded mode.');
    }
    if (typeof s.url !== 'string' || s.url.length === 0) {
      throw new ConfigError(
        'Missing Redis URL. Pass storage.url, for example process.env.REDIS_URL = "redis://localhost:6379".',
      );
    }
    if (!/^rediss?:\/\//i.test(s.url)) {
      throw new ConfigError('Redis URL must start with "redis://" or "rediss://".');
    }
    const keyPrefix = s.keyPrefix ?? 'fluxbase';
    if (!KEY_PREFIX_PATTERN.test(keyPrefix)) {
      throw new ConfigError('storage.keyPrefix may contain 1-64 characters from A-Z, a-z, 0-9, "_", ".", "-".');
    }
    storage = {
      kind: 'redis',
      url: s.url,
      keyPrefix,
      connectTimeoutMs: int('storage.connectTimeoutMs', s.connectTimeoutMs ?? 5000, 100, 120_000),
    };
  }

  const defaults: JobDefaults = options.defaults ?? {};
  const attempts = int('defaults.attempts', defaults.attempts ?? 1, 1, limits.maxAttempts);
  const backoff = defaults.backoff === undefined ? null : validateBackoff(defaults.backoff, 'defaults.backoff');
  const timeout =
    defaults.timeout === undefined ? null : int('defaults.timeout', defaults.timeout, 1, limits.maxTimeoutMs);

  const logging: NonNullable<FluxbaseOptions['logging']> = options.logging ?? {};
  const logLevel = logging.level ?? 'warn';
  if (!LOG_LEVELS.includes(logLevel)) {
    throw new ConfigError(`logging.level must be one of ${LOG_LEVELS.join(', ')}.`);
  }

  const worker: NonNullable<FluxbaseOptions['worker']> = options.worker ?? {};
  const scheduler: NonNullable<FluxbaseOptions['scheduler']> = options.scheduler ?? {};
  const kv: NonNullable<FluxbaseOptions['kv']> = options.kv ?? {};
  const retention: NonNullable<FluxbaseOptions['retention']> = options.retention ?? {};
  const kvPrefix = kv.prefix ?? '';
  if (typeof kvPrefix !== 'string' || kvPrefix.length > 128) {
    throw new ConfigError('kv.prefix must be a string of at most 128 characters.');
  }

  return {
    mode,
    storage,
    defaults: {
      attempts,
      backoff,
      timeout,
      removeOnComplete: defaults.removeOnComplete ?? false,
      removeOnFail: defaults.removeOnFail ?? false,
    },
    logging: { level: logLevel, logger: logging.logger ?? null },
    worker: {
      concurrency: int('worker.concurrency', worker.concurrency ?? 1, 1, 1000),
      leaseMs: int('worker.leaseMs', worker.leaseMs ?? 30_000, 1000, 24 * 60 * 60 * 1000),
      pollIntervalMs: int('worker.pollIntervalMs', worker.pollIntervalMs ?? 1000, 10, 60_000),
      shutdownTimeoutMs: int('worker.shutdownTimeoutMs', worker.shutdownTimeoutMs ?? 10_000, 0, 600_000),
    },
    scheduler: {
      enabled: scheduler.enabled ?? true,
      tickMs: int('scheduler.tickMs', scheduler.tickMs ?? 1000, 100, 3_600_000),
    },
    kv: {
      prefix: kvPrefix,
      sweepIntervalMs: int('kv.sweepIntervalMs', kv.sweepIntervalMs ?? 60_000, 100, 86_400_000),
      maxValueBytes: int('kv.maxValueBytes', kv.maxValueBytes ?? limits.maxValueBytes, 1, 64 * 1024 * 1024),
    },
    retention: {
      completed: int('retention.completed', retention.completed ?? 1000, 0, 1_000_000),
      failed: int('retention.failed', retention.failed ?? 1000, 0, 1_000_000),
    },
  };
}

function validateSqlitePath(path: unknown): string {
  if (typeof path !== 'string' || path.trim().length === 0) {
    throw new ConfigError('storage.path must be a non-empty file path, for example "./data/fluxbase.db".');
  }
  if (path.includes('\0')) {
    throw new ConfigError('storage.path must not contain NUL characters.');
  }
  if (path === ':memory:') {
    throw new ConfigError('Use mode "memory" for an in-memory database instead of storage.path ":memory:".');
  }
  return path;
}
