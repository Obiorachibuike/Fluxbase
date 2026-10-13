// Distributed mode: several processes share one Redis. Pass the URL from the environment.
// Never hard-code credentials. Distributed mode never falls back to embedded storage.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const url = process.env.REDIS_URL;
  if (!url) throw new Error('Set REDIS_URL, for example redis://127.0.0.1:6379');

  const fb = new Fluxbase({
    mode: 'distributed',
    storage: { type: 'redis', url, keyPrefix: 'myapp' },
  });
  await fb.start(); // throws RedisConnectionError if Redis is unreachable

  const orders = fb.queue<{ orderId: string }>('orders');
  const worker = orders.process(async (job) => {
    console.log('processing', job.data.orderId, 'on', fb.instanceId);
  }, { concurrency: 8 });

  await orders.add('fulfil', { orderId: 'o-1001' }, { attempts: 3 });
  console.log('health:', (await fb.health()).status);

  await worker.stop();
  await fb.close();
}

void main();
