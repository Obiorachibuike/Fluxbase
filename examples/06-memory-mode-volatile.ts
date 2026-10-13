// Memory mode is explicit opt-in and VOLATILE: everything is lost when the process exits.
// Useful for tests and short-lived tools, never for data you need to keep.
import { Fluxbase } from 'fluxbase';

async function main(): Promise<void> {
  const fb = new Fluxbase({ mode: 'memory' });
  await fb.start();

  const health = await fb.health();
  console.log('volatile:', health.volatile); // true

  await fb.kv.set('scratch', { ok: true }, { ttl: 30 });
  console.log(await fb.kv.get('scratch'));
  await fb.close(); // nothing is persisted
}

void main();
