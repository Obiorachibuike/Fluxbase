import type { BackoffOptions } from '../core/config.js';

/** Job options stored with a schedule and applied to every run. Stored as JSON. */
export interface ScheduleJobSpec {
  priority: number;
  attempts: number;
  backoff: BackoffOptions | null;
  timeoutMs: number | null;
  removeOnComplete: boolean;
  removeOnFail: boolean;
  /** JSON text of the metadata object. */
  metadata: string;
}
