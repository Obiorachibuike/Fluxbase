import { type FluxbaseMode, type FluxbaseOptions, type ResolvedConfig, resolveConfig } from './config.js';
import { ClosedError } from './errors.js';
import { Lifecycle } from './lifecycle.js';
import { guardStorage } from './guarded-storage.js';
import { StorageCache, type Cache } from '../cache/cache.js';
import { KeyValueStore } from '../kv/store.js';
import { ExpirationSweeper } from '../kv/expiration.js';
import { EventBus } from '../events/event-bus.js';
import type { EventMap } from '../events/event.js';
import { createLogger, type Logger } from '../observability/logger.js';
import { QueueMetrics } from '../observability/metrics.js';
import { Queue, type QueueHost, type QueueOptions } from '../queue/queue.js';
import { Scheduler } from '../queue/scheduler.js';
import type { QueueWorker, WorkerHealth } from '../queue/worker.js';
import { createStorageAdapter } from '../storage/create.js';
import type { StorageAdapter } from '../storage/adapter.js';
import type { StorageCapabilities } from '../storage/capabilities.js';
import { newId } from '../utils/ids.js';

export interface HealthReport {
  status: 'ok' | 'error';
  state: 'created' | 'starting' | 'running' | 'closing' | 'closed';
  mode: FluxbaseMode;
  storage: 'sqlite' | 'redis';
  volatile: boolean;
  capabilities: StorageCapabilities;
  error: string | null;
  workers: WorkerHealth[];
}

/**
 * The Fluxbase client: one instance per application process (or per independent
 * component). Create it, `await start()` it, use it, and `await close()` it.
 *
 * @typeParam TEvents  Optional map of event topics to payload types for `events`.
 */
export class Fluxbase<TEvents extends EventMap = EventMap> implements QueueHost {
  /** Unique id of this instance. Used as the event source and in worker ids. */
  readonly instanceId = newId();
  readonly mode: FluxbaseMode;
  /** Key-value store with optional TTL (seconds). */
  readonly kv: KeyValueStore;
  /** Cache. Persistent or Redis-backed depending on mode. Use `createMemoryCache()` for a volatile in-process cache. */
  readonly cache: Cache;
  /** Local events, and broadcast events in distributed mode. */
  readonly events: EventBus<TEvents>;
  readonly logger: Logger;
  readonly config: ResolvedConfig;
  /** Storage with lifecycle guards. Use the public APIs instead of calling this directly. */
  readonly storage: StorageAdapter;

  private readonly rawStorage: StorageAdapter;
  private readonly lifecycle = new Lifecycle();
  private readonly workers = new Map<string, QueueWorker<never, never>>();
  private readonly metrics = new Map<string, QueueMetrics>();
  private readonly clock = () => Date.now();
  private scheduler: Scheduler | null = null;
  private sweeper: ExpirationSweeper | null = null;
  private startPromise: Promise<void> | null = null;
  private closePromise: Promise<void> | null = null;

  /**
   * Validates the options and prepares the client. No connection is made until
   * `start()`. Invalid options throw ConfigError immediately.
   */
  constructor(options: FluxbaseOptions = {}) {
    this.config = resolveConfig(options);
    this.mode = this.config.mode;
    this.logger = createLogger(this.config.logging.level, this.config.logging.logger);
    this.rawStorage = createStorageAdapter(this.config, this.logger);
    this.storage = guardStorage(this.rawStorage, this.lifecycle);
    this.kv = new KeyValueStore(this.storage, this.config.kv.prefix, this.config.kv.maxValueBytes, this.clock);
    this.cache = StorageCache.create(this.storage, this.config.kv.maxValueBytes, this.clock);
    this.events = new EventBus<TEvents>(this.storage, this.instanceId, this.logger, this.config.kv.maxValueBytes, this.clock);
  }

  /**
   * Opens storage (creating the database file or connecting to Redis), runs
   * migrations, and starts the scheduler. Throws on failure. There is no silent
   * fallback to another storage mode. Calling `start()` again returns the same promise.
   */
  start(): Promise<void> {
    this.startPromise ??= this.doStart().catch((error: unknown) => {
      this.startPromise = null;
      throw error;
    });
    return this.startPromise;
  }

  /**
   * Gets a named queue. Queue objects are lightweight, so calling this repeatedly is fine.
   * `options` applies to this instance of the queue object.
   */
  queue<T = unknown, R = unknown>(name: string, options?: QueueOptions): Queue<T, R> {
    return new Queue<T, R>(this, name, options);
  }

  /** Health of storage plus the workers running in this process. Never throws. */
  async health(): Promise<HealthReport> {
    let error: string | null = null;
    let status: HealthReport['status'] = 'ok';
    if (this.lifecycle.state !== 'running') {
      status = 'error';
      error = `Fluxbase is ${this.lifecycle.state}.`;
    } else {
      try {
        await this.rawStorage.ping();
      } catch (cause) {
        status = 'error';
        error = (cause as Error).message;
      }
    }
    return {
      status,
      state: this.lifecycle.state,
      mode: this.mode,
      storage: this.rawStorage.kind,
      volatile: this.config.storage.kind === 'sqlite' ? this.config.storage.volatile : false,
      capabilities: this.rawStorage.capabilities,
      error,
      workers: this.getWorkers(),
    };
  }

  /** Health of the workers running in this process. */
  getWorkers(): WorkerHealth[] {
    return [...this.workers.values()].map((w) => w.health());
  }

  /**
   * Stops the scheduler and every worker in this process, waits for active jobs
   * up to `worker.shutdownTimeoutMs`, releases anything still running, then
   * closes event subscriptions and storage. Idempotent.
   */
  close(): Promise<void> {
    this.closePromise ??= this.doClose();
    return this.closePromise;
  }

  // ------------------------------------------------------------ QueueHost

  /** Current time in epoch ms. Injected into queues and workers. */
  now(): number {
    return this.clock();
  }

  assertRunning(): void {
    this.lifecycle.assertRunning();
  }

  metricsFor(queue: string): QueueMetrics {
    let m = this.metrics.get(queue);
    if (!m) {
      m = new QueueMetrics();
      this.metrics.set(queue, m);
    }
    return m;
  }

  registerWorker(worker: QueueWorker<never, never>): void {
    this.workers.set(worker.id, worker);
  }

  unregisterWorker(worker: { id: string }): void {
    this.workers.delete(worker.id);
  }

  workersFor(queue: string): number {
    let count = 0;
    for (const worker of this.workers.values()) {
      if (worker.queue === queue && worker.health().status !== 'stopped') count++;
    }
    return count;
  }

  // ------------------------------------------------------------ lifecycle internals

  private async doStart(): Promise<void> {
    const state = this.lifecycle.state;
    if (state === 'closing' || state === 'closed') throw new ClosedError();
    this.lifecycle.transition('starting');
    try {
      await this.rawStorage.open();
    } catch (error) {
      this.lifecycle.transition('created');
      this.logger.error('Failed to start Fluxbase', { mode: this.mode, error: error as Error });
      throw error;
    }
    this.lifecycle.transition('running');

    if (this.mode === 'memory') {
      this.logger.warn('Fluxbase is running in memory mode. All data is volatile and is lost when the process exits.');
    }
    this.logger.info('Fluxbase started', {
      mode: this.mode,
      storage: this.rawStorage.kind,
      volatile: this.mode === 'memory',
    });

    if (this.config.scheduler.enabled) {
      this.scheduler = new Scheduler(
        this.storage,
        this.config.scheduler.tickMs,
        this.logger,
        this.clock,
        (queue) => void this.storage.notifyQueue(queue).catch(() => undefined),
      );
      this.scheduler.start();
    }
    this.sweeper = new ExpirationSweeper(this.storage, this.config.kv.sweepIntervalMs, this.logger, this.clock);
    this.sweeper.start();
  }

  private async doClose(): Promise<void> {
    if (this.startPromise) {
      await this.startPromise.catch(() => undefined);
    }
    if (this.lifecycle.state === 'created' || this.lifecycle.state === 'closed') {
      this.lifecycle.transition('closed');
      return;
    }
    this.lifecycle.transition('closing');
    this.logger.info('Fluxbase closing', { workers: this.workers.size });
    await this.scheduler?.stop();
    await Promise.all([...this.workers.values()].map((w) => w.stop()));
    await this.sweeper?.stop();
    await this.events.close();
    await this.rawStorage.close();
    this.lifecycle.transition('closed');
    this.logger.info('Fluxbase closed');
  }
}
