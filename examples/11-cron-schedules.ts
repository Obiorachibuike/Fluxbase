// Repeatable jobs with cron. Times are evaluated in the given IANA timezone.
// A missed run (downtime) is coalesced: one run is enqueued, not one per missed slot.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/cron.db' } });
  await fb.start();

  const jobs = fb.queue<{ report: string }>('reports');
  const info = await jobs.schedule('daily-summary', { report: 'summary' }, {
    cron: '0 9 * * *',
    timezone: 'Europe/London',
    attempts: 3,
  });
  console.log('next run:', new Date(info.nextRunAt).toISOString());

  jobs.process(async (job) => {
    console.log('running', job.id, job.data.report);
  });

  console.log((await jobs.listSchedules()).map((s) => s.name));
  await jobs.removeSchedule('daily-summary');
  await fb.close();
}

void main();
