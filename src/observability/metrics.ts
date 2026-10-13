/**
 * Process-local counters for one queue. These describe only the current process
 * since `start()`. Authoritative counts of waiting, active, and finished jobs come
 * from storage (see `queue.getStats()`).
 */
export interface LocalQueueMetrics {
  processed: number;
  failed: number;
  retried: number;
  storageErrors: number;
  /** Average handler duration of finished attempts, or null when none have finished. */
  avgProcessingMs: number | null;
  /** Average time between a job becoming eligible and being claimed, or null when none. */
  avgQueueLatencyMs: number | null;
}

export class QueueMetrics {
  processed = 0;
  failed = 0;
  retried = 0;
  storageErrors = 0;
  private processingSamples = 0;
  private processingTotalMs = 0;
  private latencySamples = 0;
  private latencyTotalMs = 0;

  recordClaim(eligibleAt: number, claimedAt: number): void {
    this.latencySamples++;
    this.latencyTotalMs += Math.max(0, claimedAt - eligibleAt);
  }

  recordAttempt(durationMs: number): void {
    this.processingSamples++;
    this.processingTotalMs += Math.max(0, durationMs);
  }

  snapshot(): LocalQueueMetrics {
    return {
      processed: this.processed,
      failed: this.failed,
      retried: this.retried,
      storageErrors: this.storageErrors,
      avgProcessingMs: this.processingSamples === 0 ? null : this.processingTotalMs / this.processingSamples,
      avgQueueLatencyMs: this.latencySamples === 0 ? null : this.latencyTotalMs / this.latencySamples,
    };
  }
}
