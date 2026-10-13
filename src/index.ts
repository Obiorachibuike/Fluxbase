/**
 * Fluxbase: an embedded job queue, key-value store, cache, and event bus for Node.js,
 * with an optional Redis-backed distributed mode.
 *
 * @packageDocumentation
 */
export { Fluxbase, type HealthReport } from './core/client.js';
export type {
  FluxbaseMode,
  FluxbaseOptions,
  BackoffOptions,
  JobDefaults,
  RedisStorageOptions,
  SqliteStorageOptions,
  StorageOptions,
} from './core/config.js';
export {
  FluxbaseError,
  ConfigError,
  ValidationError,
  NotStartedError,
  ClosedError,
  StorageError,
  RedisConnectionError,
  JobNotFoundError,
  DuplicateJobError,
  InvalidStateError,
  SerializationError,
  UnsupportedCapabilityError,
  LeaseLostError,
  JobTimeoutError,
  UnrecoverableError,
  type FluxbaseErrorCode,
} from './core/errors.js';
export { Queue, type QueueOptions, type QueueStats, type ScheduleInfo, type ScheduleOptions } from './queue/queue.js';
export { type Job, type JobOptions, type JobState, JOB_STATES, JOB_TRANSITIONS } from './queue/job.js';
export { QueueWorker, type Processor, type ProcessContext, type ProcessOptions, type WorkerHealth, type WorkerStatus } from './queue/worker.js';
export { computeBackoffDelay } from './queue/backoff.js';
export { compileCron } from './queue/cron.js';
export type { Cache, CacheSetOptions, CacheStats, MemoryCacheOptions } from './cache/cache.js';
export { createMemoryCache } from './cache/cache.js';
export type { KeyValueSetOptions } from './kv/store.js';
export { KeyValueStore } from './kv/store.js';
export { EventBus } from './events/event-bus.js';
export type { EventHandler, EventMap, EventScope, FluxbaseEvent, PublishOptions } from './events/event.js';
export type { Logger, LogLevel } from './observability/logger.js';
export type { LocalQueueMetrics } from './observability/metrics.js';
export type { StorageCapabilities } from './storage/capabilities.js';
