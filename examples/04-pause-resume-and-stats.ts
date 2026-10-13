// Pause a queue for maintenance, inspect counts, and manage individual jobs.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/ops.db' } });
  await fb.start();

  const reports = fb.queue<{ month: string }>('reports', { retention: { completed: 100, failed: 500 } });
  const job = await reports.add('monthly', { month: '2026-09' }, { jobId: 'report-2026-09' });

  await reports.pause();
  console.log('paused:', await reports.isPaused()); // true, nothing is claimed while paused

  const stats = await reports.getStats();
  console.log('waiting:', stats.waiting, 'failed:', stats.failed, 'local processed:', stats.local.processed);

  // Remove a job that is no longer needed. Active jobs cannot be removed.
  await reports.remove(job.id);
  await reports.resume();
  await fb.close();
}

void main();
