/**
 * What a storage backend can guarantee. Features that a backend cannot
 * provide are rejected with UnsupportedCapabilityError instead of degrading silently.
 */
export interface StorageCapabilities {
  /** Records survive a process restart. */
  readonly durable: boolean;
  /** Several application processes can safely share the same storage. */
  readonly multiProcess: boolean;
  /** Pushes wake-ups to workers in other processes (otherwise workers poll). */
  readonly crossProcessNotifications: boolean;
  /** `events.publish(..., { scope: "broadcast" })` can reach other processes. */
  readonly broadcastEvents: boolean;
  /** KV expiry is enforced by the backend itself rather than by lazy reads and sweeps. */
  readonly nativeKvExpiry: boolean;
}

export const SQLITE_CAPABILITIES: StorageCapabilities = {
  durable: true,
  multiProcess: false,
  crossProcessNotifications: false,
  broadcastEvents: false,
  nativeKvExpiry: false,
};

export const MEMORY_CAPABILITIES: StorageCapabilities = {
  durable: false,
  multiProcess: false,
  crossProcessNotifications: false,
  broadcastEvents: false,
  nativeKvExpiry: false,
};

export const REDIS_CAPABILITIES: StorageCapabilities = {
  durable: true,
  multiProcess: true,
  crossProcessNotifications: true,
  broadcastEvents: true,
  nativeKvExpiry: true,
};
