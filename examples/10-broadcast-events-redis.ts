// Broadcast events across processes (distributed mode only). Redis Pub/Sub is NOT durable:
// a process that is not subscribed when a message is published never receives it.
// Use the job queue for work that must not be lost.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const url = process.env.REDIS_URL;
  if (!url) throw new Error('Set REDIS_URL');

  const fb = new Fluxbase({ mode: 'distributed', storage: { type: 'redis', url, keyPrefix: 'myapp' } });
  await fb.start();

  fb.events.subscribe('cache.invalidate', (event) => {
    // Runs in every process that is subscribed. Publisher echoes are skipped.
    console.log('invalidate', event.data);
  });

  await fb.events.publish('cache.invalidate', { key: 'user:1' }, { scope: 'broadcast' });
  await fb.close();
}

void main();
