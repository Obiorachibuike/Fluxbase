// Graceful shutdown on SIGTERM. close() stops the scheduler and workers, waits up to
// worker.shutdownTimeoutMs for active jobs, then releases any job still running so
// another process can take it. Released jobs do not consume an attempt.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({
    storage: { type: 'sqlite', path: './data/shutdown.db' },
    worker: { shutdownTimeoutMs: 10_000 },
  });
  await fb.start();

  const jobs = fb.queue<{ n: number }>('batch');
  jobs.process(async (job, { signal }) => {
    // Observe the signal so long work stops promptly during shutdown.
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, 500);
      signal.addEventListener('abort', () => {
        clearTimeout(timer);
        reject(new Error('shutting down'));
      });
    });
    console.log('finished', job.data.n);
  }, { concurrency: 4 });

  const stop = async (): Promise<void> => {
    console.log('shutting down');
    await fb.close();
    process.exit(0);
  };
  process.once('SIGTERM', () => void stop());
  process.once('SIGINT', () => void stop());
}

void main();
