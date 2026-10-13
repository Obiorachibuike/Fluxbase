import { ValidationError } from '../core/errors.js';
import type { StorageAdapter } from '../storage/adapter.js';
import { limits } from '../utils/ids.js';
import { deserialize, serialize } from '../utils/serialization.js';
import { assertKey, assertTtl, NAMESPACE_SEPARATOR } from '../kv/store.js';
import { ttlToExpiresAt } from '../kv/expiration.js';

export interface CacheSetOptions {
  /** Time to live in SECONDS. Omit for no expiry (until evicted, for memory caches). */
  ttl?: number;
}

export interface CacheStats {
  hits: number;
  misses: number;
  sets: number;
  deletes: number;
  /** Entries evicted by the size limit. Only memory caches evict. */
  evictions: number;
}

/** Cache API shared by the storage-backed and the in-memory implementations. */
export interface Cache {
  /** `"storage"` (persistent or Redis-backed, depending on the Fluxbase mode) or `"memory"` (volatile). */
  readonly backend: 'storage' | 'memory';
  /** True when entries are lost on process exit. */
  readonly volatile: boolean;
  get<T = unknown>(key: string): Promise<T | undefined>;
  has(key: string): Promise<boolean>;
  set<T>(key: string, value: T, options?: CacheSetOptions): Promise<void>;
  delete(key: string): Promise<boolean>;
  /** Bulk read. Misses are omitted from the Map. */
  getMany<T = unknown>(keys: string[]): Promise<Map<string, T>>;
  stats(): CacheStats;
  /** Returns a view of this cache with an isolated key space. */
  namespace(name: string): Cache;
}

/** Low-level text store behind a cache. Values are JSON text, so no object references are shared. */
export interface CacheTextStore {
  getText(key: string, now: number): Promise<string | null>;
  setText(key: string, text: string, expiresAt: number | null): Promise<void>;
  deleteKey(key: string): Promise<boolean>;
  getTexts(keys: string[], now: number): Promise<(string | null)[]>;
}

const NAMESPACE_PATTERN = /^[A-Za-z0-9_.-]{1,64}$/;

function assertNamespace(name: unknown): asserts name is string {
  if (typeof name !== 'string' || !NAMESPACE_PATTERN.test(name)) {
    throw new ValidationError('Cache namespaces must be 1-64 characters from A-Z, a-z, 0-9, "_", ".", "-".');
  }
}

class CacheImpl implements Cache {
  private readonly counters: CacheStats = { hits: 0, misses: 0, sets: 0, deletes: 0, evictions: 0 };

  constructor(
    private readonly store: CacheTextStore,
    private readonly prefix: string,
    readonly backend: 'storage' | 'memory',
    readonly volatile: boolean,
    private readonly maxValueBytes: number,
    private readonly makeNamespace: (name: string) => Cache,
    private readonly now: () => number,
  ) {}

  async get<T = unknown>(key: string): Promise<T | undefined> {
    assertKey(key);
    const text = await this.store.getText(this.qualify(key), this.now());
    if (text === null) {
      this.counters.misses++;
      return undefined;
    }
    this.counters.hits++;
    return deserialize<T>(text, 'cache value');
  }

  async has(key: string): Promise<boolean> {
    assertKey(key);
    const text = await this.store.getText(this.qualify(key), this.now());
    return text !== null;
  }

  async set<T>(key: string, value: T, options: CacheSetOptions = {}): Promise<void> {
    assertKey(key);
    if (options.ttl !== undefined) assertTtl(options.ttl);
    const text = serialize(value, this.maxValueBytes, 'Cache value');
    const expiresAt = options.ttl === undefined ? null : ttlToExpiresAt(options.ttl, this.now());
    await this.store.setText(this.qualify(key), text, expiresAt);
    this.counters.sets++;
  }

  async delete(key: string): Promise<boolean> {
    assertKey(key);
    const removed = await this.store.deleteKey(this.qualify(key));
    this.counters.deletes++;
    return removed;
  }

  async getMany<T = unknown>(keys: string[]): Promise<Map<string, T>> {
    keys.forEach(assertKey);
    const texts = await this.store.getTexts(keys.map((k) => this.qualify(k)), this.now());
    const out = new Map<string, T>();
    keys.forEach((key, i) => {
      const text = texts[i];
      if (text === null || text === undefined) {
        this.counters.misses++;
      } else {
        this.counters.hits++;
        out.set(key, deserialize<T>(text, 'cache value'));
      }
    });
    return out;
  }

  stats(): CacheStats {
    return { ...this.counters };
  }

  namespace(name: string): Cache {
    assertNamespace(name);
    return this.makeNamespace(name);
  }

  private qualify(key: string): string {
    return `${this.prefix}${key}`;
  }
}

/** Storage-backed cache. Uses the Fluxbase storage adapter, so it is persistent in embedded mode and shared in distributed mode. */
export class StorageCache extends CacheImpl {
  static create(storage: StorageAdapter, maxValueBytes: number, now: () => number = Date.now, name = 'default'): Cache {
    assertNamespace(name);
    const store: CacheTextStore = {
      getText: (key) => storage.kvGet(key, now()),
      getTexts: (keys) => storage.kvGetMany(keys, now()),
      setText: (key, text, expiresAt) => storage.kvSet(key, text, expiresAt),
      deleteKey: (key) => storage.kvDelete(key),
    };
    const make = (ns: string): Cache => StorageCache.create(storage, maxValueBytes, now, ns);
    return new StorageCache(store, `cache:${name}${NAMESPACE_SEPARATOR}`, 'storage', !storage.capabilities.durable, maxValueBytes, make, now);
  }
}

export interface MemoryCacheOptions {
  /** Maximum number of entries. Least recently used entries are evicted first. Default: 1000. */
  maxEntries?: number;
  /** Maximum serialized size of a single value in bytes. Default: 1 MiB. */
  maxValueBytes?: number;
}

/**
 * Creates a bounded in-process LRU cache. It is VOLATILE: entries disappear when
 * the process exits and are never shared with other processes. Values are stored
 * as JSON text, so callers always receive fresh copies.
 */
export function createMemoryCache(options: MemoryCacheOptions = {}): Cache {
  const maxEntries = options.maxEntries ?? 1000;
  if (!Number.isInteger(maxEntries) || maxEntries < 1 || maxEntries > 1_000_000) {
    throw new ValidationError('maxEntries must be an integer between 1 and 1000000.');
  }
  const maxValueBytes = options.maxValueBytes ?? limits.maxValueBytes;
  const entries = new Map<string, { text: string; expiresAt: number | null }>();
  let evictions = 0;
  const now = Date.now;

  const store: CacheTextStore = {
    async getText(key, at) {
      const entry = entries.get(key);
      if (!entry) return null;
      if (entry.expiresAt !== null && entry.expiresAt <= at) {
        entries.delete(key);
        return null;
      }
      // Re-insert to mark as most recently used.
      entries.delete(key);
      entries.set(key, entry);
      return entry.text;
    },
    async getTexts(keys, at) {
      return Promise.all(keys.map((k) => store.getText(k, at)));
    },
    async setText(key, text, expiresAt) {
      entries.delete(key);
      entries.set(key, { text, expiresAt });
      while (entries.size > maxEntries) {
        const oldest = entries.keys().next().value as string;
        entries.delete(oldest);
        evictions++;
      }
    },
    async deleteKey(key) {
      return entries.delete(key);
    },
  };

  const make = (name: string): Cache => memoryNamespace(name);
  const memoryNamespace = (name: string): Cache => {
    assertNamespace(name);
    return new MemoryCacheView(store, `${name}${NAMESPACE_SEPARATOR}`, maxValueBytes, make, () => now(), () => evictions);
  };
  return new MemoryCacheView(store, '', maxValueBytes, make, () => now(), () => evictions);
}

class MemoryCacheView extends CacheImpl {
  constructor(
    store: CacheTextStore,
    prefix: string,
    maxValueBytes: number,
    make: (name: string) => Cache,
    now: () => number,
    private readonly evictionCount: () => number,
  ) {
    super(store, prefix, 'memory', true, maxValueBytes, make, now);
  }

  override stats(): CacheStats {
    return { ...super.stats(), evictions: this.evictionCount() };
  }
}
