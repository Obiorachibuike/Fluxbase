import type { StorageAdapter } from '../storage/adapter.js';
import { SerializationError, ValidationError } from '../core/errors.js';
import { deserialize, serialize } from '../utils/serialization.js';
import { limits } from '../utils/ids.js';
import { MAX_TTL_SECONDS, ttlToExpiresAt } from './expiration.js';

export interface KeyValueSetOptions {
  /** Time to live in SECONDS. Omit for no expiry. */
  ttl?: number;
}

/** Separator between namespace and user key. Control characters are rejected in keys, so it cannot collide. */
export const NAMESPACE_SEPARATOR = '\u001f';

// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;

export function assertKey(key: unknown): asserts key is string {
  if (typeof key !== 'string' || key.length === 0 || key.length > limits.maxKeyLength) {
    throw new ValidationError(`Keys must be non-empty strings of at most ${limits.maxKeyLength} characters.`);
  }
  if (CONTROL_CHARS.test(key)) {
    throw new ValidationError('Keys must not contain control characters.');
  }
}

export function assertTtl(ttl: unknown): asserts ttl is number {
  if (typeof ttl !== 'number' || !Number.isFinite(ttl) || ttl <= 0 || ttl > MAX_TTL_SECONDS) {
    throw new ValidationError(`ttl must be a positive number of seconds no greater than ${MAX_TTL_SECONDS}.`);
  }
}

/**
 * Redis-inspired key-value store. Values are JSON-compatible and are always
 * stored as JSON text, so readers receive fresh copies and never shared
 * references. TTL is in seconds.
 */
export class KeyValueStore {
  constructor(
    private readonly storage: StorageAdapter,
    private readonly namespace: string,
    private readonly maxValueBytes: number,
    private readonly now: () => number = Date.now,
  ) {}

  /** Stores `value` under `key`, replacing any existing value and TTL. */
  async set<T>(key: string, value: T, options: KeyValueSetOptions = {}): Promise<void> {
    assertKey(key);
    if (options.ttl !== undefined) assertTtl(options.ttl);
    const text = serialize(value, this.maxValueBytes, 'KV value');
    const now = this.now();
    const expiresAt = options.ttl === undefined ? null : ttlToExpiresAt(options.ttl, now);
    await this.storage.kvSet(this.qualify(key), text, expiresAt);
  }

  /** Returns the value, or `undefined` when the key is missing or expired. Malformed stored data throws SerializationError. */
  async get<T = unknown>(key: string): Promise<T | undefined> {
    assertKey(key);
    const text = await this.storage.kvGet(this.qualify(key), this.now());
    return text === null ? undefined : parseStored<T>(text);
  }

  /** Returns true when a live (unexpired) value exists for `key`. */
  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== undefined;
  }

  /** Deletes `key`. Returns true when a live value was removed. */
  async delete(key: string): Promise<boolean> {
    assertKey(key);
    return this.storage.kvDelete(this.qualify(key));
  }

  /** Bulk read. Missing or expired keys are omitted from the returned Map. */
  async getMany<T = unknown>(keys: string[]): Promise<Map<string, T>> {
    keys.forEach(assertKey);
    const texts = await this.storage.kvGetMany(keys.map((k) => this.qualify(k)), this.now());
    const out = new Map<string, T>();
    keys.forEach((key, i) => {
      const text = texts[i];
      if (text !== null && text !== undefined) out.set(key, parseStored<T>(text));
    });
    return out;
  }

  /** @internal Used by the cache layer, which applies its own namespace. */
  async getText(key: string): Promise<string | null> {
    return this.storage.kvGet(this.qualify(key), this.now());
  }

  /** @internal */
  async setText(key: string, text: string, ttlSeconds?: number): Promise<void> {
    const expiresAt = ttlSeconds === undefined ? null : ttlToExpiresAt(ttlSeconds, this.now());
    await this.storage.kvSet(this.qualify(key), text, expiresAt);
  }

  private qualify(key: string): string {
    return `${this.namespace}${NAMESPACE_SEPARATOR}${key}`;
  }
}

/** Parses stored JSON text. A malformed row is reported as an error, never returned as data. */
export function parseStored<T>(text: string): T {
  try {
    return deserialize<T>(text, 'KV value');
  } catch (cause) {
    throw new SerializationError('A stored key-value entry is not valid JSON.', cause);
  }
}
