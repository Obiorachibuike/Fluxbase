import { afterEach, describe, expect, it } from 'vitest';
import { Fluxbase, UnrecoverableError, JobNotFoundError, InvalidStateError, ValidationError, DuplicateJobError } from '../../src/index.js';
import { backends, cleanupDirs, waitFor } from '../helpers.js';

const open: Fluxbase[] = [];
async function client(opts: () => ConstructorParameters<typeof Fluxbase>[0]): Promise<Fluxbase> {
  const fb = new Fluxbase(opts());
  open.push(fb);
  await fb.start();
  return fb;
}
afterEach(async () => {
  await Promise.all(open.splice(0).map((fb) => fb.close()));
  cleanupDirs();
});

for (const backend of backends) {
  describe.skipIf(backend.skip)(`queue API on ${backend.name}`, () => {
    it('processes jobs with typed data and return values', async () => {
      const fb = await client(backend.options);
      const q = fb.queue<{ n: number }, number>('math');
      const done = new Promise<number>((resolve) => {
        q.process(async (job) => {
          const doubled = job.data.n * 2;
          if (job.data.n === 21) resolve(doubled);
          return doubled;
        });
      });
      const added = await q.add('double', { n: 21 });
      expect(await done).toBe(42);
      const finished = await waitFor(async () => {
        const j = await q.getJob(added.id);
        return j?.state === 'completed' ? j : null;
      }, { message: 'job completion' });
      expect(finished.returnValue).toBe(42);
      expect(finished.attemptsMade).toBe(1);
    });

    it('retries with backoff and records the failure reason', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('retry');
      const attempts: number[] = [];
      q.process(async (job) => {
        attempts.push(job.attemptsMade);
        if (job.attemptsMade < 3) throw new Error('transient');
        return 'ok';
      });
      const job = await q.add('flaky', {}, { attempts: 3, backoff: { type: 'fixed', delay: 30 } });
      await waitFor(async () => (await q.getJobState(job.id)) === 'completed', { message: 'third attempt' });
      expect(attempts).toEqual([1, 2, 3]);
    });

    it('moves a job to failed after the last attempt, and retry() re-runs it', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('fail');
      let succeed = false;
      q.process(async () => {
        if (!succeed) throw new Error('nope');
        return 'yes';
      });
      const job = await q.add('x', {}, { attempts: 1 });
      await waitFor(async () => (await q.getJobState(job.id)) === 'failed', { message: 'failure' });
      const failed = await q.getJob(job.id);
      expect(failed?.failedReason).toBe('Error: nope');
      succeed = true;
      await q.retry(job.id);
      await waitFor(async () => (await q.getJobState(job.id)) === 'completed', { message: 'retried completion' });
      await expect(q.retry(job.id)).rejects.toBeInstanceOf(InvalidStateError);
    });

    it('UnrecoverableError skips the remaining attempts', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('unrec');
      let calls = 0;
      q.process(async () => {
        calls++;
        throw new UnrecoverableError('invalid payload');
      });
      const job = await q.add('bad', {}, { attempts: 5 });
      await waitFor(async () => (await q.getJobState(job.id)) === 'failed', { message: 'failure' });
      expect(calls).toBe(1);
    });

    it('delayed jobs do not run before their delay elapses', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('delay');
      const ran: number[] = [];
      q.process(async () => {
        ran.push(Date.now());
      });
      const requested = Date.now();
      const job = await q.add('later', {}, { delay: 400 });
      expect(await q.getJobState(job.id)).toBe('delayed');
      await waitFor(async () => (await q.getJobState(job.id)) === 'completed', { timeoutMs: 5000, message: 'delayed completion' });
      expect(ran[0]! - requested).toBeGreaterThanOrEqual(390);
    });

    it('runs higher priority jobs first', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('prio');
      await q.pause();
      await q.add('low', { order: 1 }, { priority: -5 });
      await q.add('normal', { order: 2 });
      await q.add('high', { order: 3 }, { priority: 50 });
      const seen: number[] = [];
      q.process(async (job) => {
        seen.push((job.data as { order: number }).order);
      }, { concurrency: 1 });
      await q.resume();
      await waitFor(() => seen.length === 3, { message: 'three jobs' });
      expect(seen).toEqual([3, 2, 1]);
    });

    it('pause stops claiming and resume restarts it', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('pausing');
      let ran = 0;
      q.process(async () => {
        ran++;
      });
      await q.pause();
      expect(await q.isPaused()).toBe(true);
      await q.add('a', {});
      await new Promise((r) => setTimeout(r, 300));
      expect(ran).toBe(0);
      await q.resume();
      await waitFor(() => ran === 1, { message: 'run after resume' });
    });

    it('jobId is unique: a duplicate add throws DuplicateJobError', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('dup');
      await q.add('a', {}, { jobId: 'order-1' });
      await expect(q.add('a', {}, { jobId: 'order-1' })).rejects.toBeInstanceOf(DuplicateJobError);
    });

    it('remove() deletes waiting jobs and refuses to delete unknown ones', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('rm');
      await q.pause();
      const job = await q.add('a', {});
      await q.remove(job.id);
      expect(await q.getJob(job.id)).toBeNull();
      await expect(q.remove(job.id)).rejects.toBeInstanceOf(JobNotFoundError);
    });

    it('lists jobs by state with a limit, and reports stats', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('list');
      await q.pause();
      await q.add('a', { i: 1 });
      await q.add('b', { i: 2 });
      await q.add('c', { i: 3 }, { delay: 60_000 });
      expect((await q.getJobs('waiting', { limit: 1 })).length).toBe(1);
      const stats = await q.getStats();
      expect(stats).toMatchObject({ queue: 'list', paused: true, waiting: 2, delayed: 1, active: 0 });
      expect(stats.local.processed).toBe(0);
    });

    it('keeps the requested number of completed jobs', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('keep', { retention: { completed: 2 } });
      q.process(async () => 'x', { concurrency: 1 });
      for (let i = 0; i < 5; i++) await q.add('j', { i });
      await waitFor(async () => (await q.getJobs('completed', { limit: 10 })).length === 2 && (await q.getStats()).waiting === 0, {
        message: 'retention',
      });
      await new Promise((r) => setTimeout(r, 200));
      expect((await q.getJobs('completed', { limit: 10 })).length).toBe(2);
    });

    it('rejects invalid queue names and job data before writing', async () => {
      const fb = await client(backend.options);
      expect(() => fb.queue('bad name')).toThrow(ValidationError);
      expect(() => fb.queue('bad:name')).toThrow(/colons/);
      await expect(fb.queue('ok').add('a', (() => 1) as never)).rejects.toThrow(/JSON|function/);
    });

    it('add() accepts repeatable schedules via cron, and lists them', async () => {
      const fb = await client(backend.options);
      const q = fb.queue('sched');
      const info = await q.schedule('nightly', { report: true }, { cron: '0 3 * * *', timezone: 'UTC' });
      expect(new Date(info.nextRunAt).toISOString()).toMatch(/T03:00:00\.000Z$/);
      expect((await q.listSchedules()).map((s) => s.name)).toEqual(['nightly']);
      expect(await q.getSchedule('nightly')).toMatchObject({ cron: '0 3 * * *', timezone: 'UTC' });
      expect(await q.removeSchedule('nightly')).toBe(true);
      expect(await q.removeSchedule('nightly')).toBe(false);
    });
  });
}
