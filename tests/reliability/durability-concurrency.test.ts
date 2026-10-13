import { afterEach, describe, expect, it } from 'vitest';
import { Fluxbase } from '../../src/index.js';
import { backends, cleanupDirs, embeddedOptions, waitFor } from '../helpers.js';

const open: Fluxbase[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((fb) => fb.close()));
  cleanupDirs();
});

async function client(options: ConstructorParameters<typeof Fluxbase>[0]): Promise<Fluxbase> {
  const fb = new Fluxbase(options);
  open.push(fb);
  await fb.start();
  return fb;
}

for (const backend of backends) {
  describe.skipIf(backend.skip)(`durability and concurrency on ${backend.name}`, () => {
    it.skipIf(backend.name.startsWith('memory'))('a scheduled retry survives a restart and runs once its backoff elapses', async () => {
      // Memory mode is volatile by design, so it cannot satisfy this test.
      const opts = backend.options();
      const first = await client(opts);
      const q1 = first.queue('restart');
      const id = (await q1.add('r', {}, { attempts: 2, backoff: { type: 'fixed', delay: 400 }, jobId: `restart-${Math.random()}` })).id;
      const p1 = q1.process(async () => {
        throw new Error('first attempt fails');
      });
      await waitFor(async () => (await q1.getJobState(id)) === 'delayed', { message: 'delayed retry' });
      await p1.stop();
      await first.close();
      open.splice(open.indexOf(first), 1);

      // Reopen with the same options.
      const second = await client(opts);
      const q2 = second.queue('restart');
      const ranAt: number[] = [];
      q2.process(async () => {
        ranAt.push(Date.now());
        return 'second';
      });
      const job = await waitFor(async () => {
        const j = await q2.getJob(id);
        return j?.state === 'completed' ? j : null;
      }, { timeoutMs: 10_000, message: 'retry after restart' });
      expect(job.attemptsMade).toBe(2);
      expect(ranAt).toHaveLength(1);
    });

    it.skipIf(backend.name.startsWith('memory'))('two clients sharing one queue process every job exactly once', async () => {
      // Memory clients each get a private database, so they cannot share a queue.
      const opts = backend.options();
      const a = await client(opts);
      // Both clients must use the same options object: same database file or Redis prefix.
      const b = await client(opts);
      const qa = a.queue('shared', { retention: { completed: 1000 } });
      const qb = b.queue('shared', { retention: { completed: 1000 } });
      const total = 200;
      const runs = new Map<string, number>();
      const handler = async (job: { id: string }) => {
        runs.set(job.id, (runs.get(job.id) ?? 0) + 1);
        await new Promise((r) => setTimeout(r, Math.random() * 3));
      };
      qa.process(handler, { concurrency: 4 });
      qb.process(handler, { concurrency: 4 });
      const ids: string[] = [];
      for (let i = 0; i < total; i++) {
        ids.push((await qa.add('item', { i })).id);
      }
      await waitFor(async () => (await qa.getStats()).completed === total, { timeoutMs: 60_000, message: 'all jobs completed' });
      // Both clients see the same queue: a single completed count covers all jobs.
      expect((await qb.getStats()).completed).toBe(total);
      expect(runs.size).toBe(total);
      expect([...runs.values()].every((n) => n === 1)).toBe(true);
      expect(ids.every((id) => runs.has(id))).toBe(true);
    }, 90_000);
  });
}

describe('embedded durability', () => {
  it('completed results persist across reopen', async () => {
    const opts = embeddedOptions();
    const first = await client(opts);
    const q = first.queue('persist');
    const job = await q.add('p', { v: 7 }, { jobId: 'persist-1' });
    q.process(async (j) => (j.data as { v: number }).v * 6);
    await waitFor(async () => (await q.getJobState(job.id)) === 'completed', { message: 'completion' });
    await first.close();
    open.splice(open.indexOf(first), 1);
    const second = await client(opts);
    const stored = await second.queue('persist').getJob('persist-1');
    expect(stored).toMatchObject({ state: 'completed', returnValue: 42 });
  });
});
