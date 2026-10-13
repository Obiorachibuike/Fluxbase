// Delayed jobs run after `delay` milliseconds. Higher priority runs first.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/delayed.db' } });
  await fb.start();

  const jobs = fb.queue<{ label: string }>('work');
  await jobs.pause(); // enqueue everything first so priority decides the order

  await jobs.add('background', { label: 'low' }, { priority: -10 });
  await jobs.add('normal', { label: 'normal' });
  await jobs.add('urgent', { label: 'high' }, { priority: 100 });
  const reminder = await jobs.add('remind', { label: 'delayed' }, { delay: 60_000 });
  console.log('reminder state:', await jobs.getJobState(reminder.id)); // "delayed"

  const worker = jobs.process(async (job) => {
    console.log('running', job.data.label);
  }, { concurrency: 1 });

  await jobs.resume();
  await new Promise((resolve) => setTimeout(resolve, 200));
  await worker.stop();
  await fb.close();
}

void main();
