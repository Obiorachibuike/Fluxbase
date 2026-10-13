import type { StorageAdapter } from '../storage/adapter.js';
import type { Logger } from '../observability/logger.js';

export const MAX_TTL_SECONDS = 10 * 365 * 24 * 60 * 60;

/** Converts a TTL in seconds to an absolute expiry in epoch ms. */
export function ttlToExpiresAt(ttlSeconds: number, now: number): number {
  return now + Math.max(1, Math.round(ttlSeconds * 1000));
}

/**
 * Periodically deletes expired rows for backends without native expiry. Reads
 * already ignore expired rows, so the sweep only reclaims space. Redis expires
 * keys natively and never starts this sweeper.
 *
 * The timer is unref'd, so housekeeping alone never keeps the process alive.
 */
export class ExpirationSweeper {
  private timer: NodeJS.Timeout | null = null;
  private stopped = true;
  private inFlight: Promise<number> | null = null;

  constructor(
    private readonly storage: StorageAdapter,
    private readonly intervalMs: number,
    private readonly logger: Logger,
    private readonly now: () => number = Date.now,
  ) {}

  start(): void {
    if (!this.stopped || this.storage.capabilities.nativeKvExpiry) return;
    this.stopped = false;
    this.schedule();
  }

  /** Deletes expired entries in batches. Returns the total deleted. */
  sweep(): Promise<number> {
    this.inFlight ??= this.runSweep().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (this.inFlight) await this.inFlight.catch(() => undefined);
  }

  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.sweep().finally(() => this.schedule());
    }, this.intervalMs);
    this.timer.unref();
  }

  private async runSweep(): Promise<number> {
    let total = 0;
    try {
      for (;;) {
        const deleted = await this.storage.kvSweep(this.now(), 500);
        total += deleted;
        if (deleted < 500) break;
      }
    } catch (error) {
      this.logger.warn('KV expiration sweep failed', { error: error as Error });
    }
    return total;
  }
}
