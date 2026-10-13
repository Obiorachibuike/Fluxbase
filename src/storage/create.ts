import type { ResolvedConfig } from '../core/config.js';
import type { Logger } from '../observability/logger.js';
import type { StorageAdapter } from './adapter.js';
import { RedisStorageAdapter } from './redis/redis-adapter.js';
import { SqliteStorageAdapter } from './sqlite/sqlite-adapter.js';

/** Creates (but does not open) the storage adapter selected by the resolved configuration. */
export function createStorageAdapter(config: ResolvedConfig, logger: Logger): StorageAdapter {
  const storage = config.storage;
  if (storage.kind === 'sqlite') {
    return new SqliteStorageAdapter(storage.path, storage.volatile);
  }
  return new RedisStorageAdapter(storage.url, storage.keyPrefix, storage.connectTimeoutMs, (level, message, meta) => {
    logger[level](message, meta);
  });
}
