// Cache with TTL in seconds. Use the storage-backed cache for persistence across restarts,
// or createMemoryCache() for a volatile in-process LRU.
import { Fluxbase, createMemoryCache } from 'fluxbase';

interface Report {
  rows: number;
}

async function loadReport(id: string): Promise<Report> {
  return { rows: id.length * 100 }; // stand-in for an expensive query
}

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/cache.db' } });
  await fb.start();

  const cached = await fb.cache.get<Report>('report:42');
  const report = cached ?? (await loadReport('42'));
  if (!cached) await fb.cache.set('report:42', report, { ttl: 300 });
  console.log(report);
  console.log('stats:', fb.cache.stats());

  // Volatile, in-process alternative. Bounded by maxEntries (LRU).
  const local = createMemoryCache({ maxEntries: 500 });
  await local.set('flags', { beta: true });
  console.log(await local.get('flags'));

  await fb.close();
}

void main();
