// Retries with exponential backoff. attempts counts total attempts, including the first.
import { Fluxbase, UnrecoverableError } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/retries.db' } });
  await fb.start();

  const webhooks = fb.queue<{ url: string }>('webhooks');
  const worker = webhooks.process(async (job, { signal }) => {
    if (job.data.url.startsWith('http://insecure')) {
      // Retrying cannot fix this, so skip the remaining attempts.
      throw new UnrecoverableError('plain HTTP is not allowed');
    }
    const response = await fetch(job.data.url, { signal });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.status;
  });

  await webhooks.add(
    'deliver',
    { url: 'https://example.com/hook' },
    { attempts: 5, backoff: { type: 'exponential', delay: 1_000, maxDelay: 60_000 } },
  );

  await webhooks.add('bad', { url: 'http://insecure.example.com/hook' }, { attempts: 5 });

  await new Promise((resolve) => setTimeout(resolve, 200));
  console.log('failed jobs:', (await webhooks.getJobs('failed')).map((j) => j.failedReason));
  await worker.stop();
  await fb.close();
}

void main();
