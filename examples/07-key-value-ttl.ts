// Key-value store. TTLs are in SECONDS. Values are JSON.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/kv.db' } });
  await fb.start();

  await fb.kv.set('session:abc', { userId: 'u-1', roles: ['admin'] }, { ttl: 3600 });
  const session = await fb.kv.get<{ userId: string; roles: string[] }>('session:abc');
  console.log(session?.userId);

  await fb.kv.set('feature:beta', true); // no expiry
  const flags = await fb.kv.getMany(['feature:beta', 'feature:missing']);
  console.log(flags.get('feature:beta'), flags.has('feature:missing'));

  await fb.kv.delete('session:abc');
  await fb.close();
}

void main();
