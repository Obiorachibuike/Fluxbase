import { afterAll, describe, expect, it } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { Fluxbase } from '../../src/index.js';
import { cleanupDirs, distributedOptions, embeddedOptions, hasRedis, waitFor } from '../helpers.js';

const FIXTURE = fileURLToPath(new URL('../fixtures/crash-worker.ts', import.meta.url));
const children: ChildProcess[] = [];

afterAll(() => {
  for (const c of children) c.kill('SIGKILL');
  cleanupDirs();
});

/** Starts a worker process, and resolves with its first claimed job id. */
function startCrashingWorker(options: object): Promise<{ child: ChildProcess; jobId: string }> {
  return new Promise((resolve, reject) => {
    // Run node directly with the tsx loader. Going through the `tsx` wrapper would
    // leave the real worker alive after SIGKILL, because the wrapper is a parent process.
    const child = spawn(process.execPath, ['--import', 'tsx', FIXTURE, JSON.stringify(options)], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    children.push(child);
    let out = '';
    let err = '';
    child.stderr!.on('data', (d: Buffer) => (err += d.toString()));
    child.stdout!.on('data', (d: Buffer) => {
      out += d.toString();
      const match = /STARTED (\S+)/.exec(out);
      if (match) resolve({ child, jobId: match[1]! });
    });
    child.on('exit', (code) => reject(new Error(`worker exited early (code ${code}): ${err}`)));
    setTimeout(() => reject(new Error(`worker did not claim a job: ${err}`)), 20_000).unref();
  });
}

function leaseFriendly(options: ReturnType<typeof embeddedOptions>) {
  return { ...options, worker: { pollIntervalMs: 50, leaseMs: 1000, shutdownTimeoutMs: 500 } };
}

const scenarios = [
  { name: 'embedded (sqlite file)', make: () => embeddedOptions() as Record<string, unknown>, skip: false },
  { name: 'distributed (redis)', make: () => distributedOptions() as Record<string, unknown>, skip: !hasRedis },
];

for (const s of scenarios) {
  describe.skipIf(s.skip)(`crash recovery on ${s.name}`, () => {
    it('a job whose worker is SIGKILLed mid-run is recovered by another worker', async () => {
      const base = leaseFriendly(s.make() as ReturnType<typeof embeddedOptions>);
      // Enqueue from the parent first, so the job exists before the crash.
      const producer = new Fluxbase(base);
      await producer.start();
      const added = await producer.queue('crash').add('work', { n: 1 }, { attempts: 3 });
      await producer.close();

      const { child, jobId } = await startCrashingWorker(base);
      expect(jobId).toBe(added.id);
      child.kill('SIGKILL');
      await new Promise((r) => child.once('exit', r));

      const recovered = new Fluxbase(base);
      await recovered.start();
      try {
        const q = recovered.queue('crash');
        const processed: number[] = [];
        q.process(async (job) => {
          processed.push(job.attemptsMade);
          return 'recovered';
        });
        const job = await waitFor(async () => {
          const j = await q.getJob(added.id);
          return j?.state === 'completed' ? j : null;
        }, { timeoutMs: 15_000, message: 'recovered job completion' });
        expect(job.returnValue).toBe('recovered');
        // The crashed attempt counted, so this is attempt 2 of 3.
        expect(processed).toEqual([2]);
        expect(job.attemptsMade).toBe(2);
      } finally {
        await recovered.close();
      }
    }, 40_000);

    it('a crashed job with no attempts left ends in failed with a LeaseExpired reason', async () => {
      const base = leaseFriendly(s.make() as ReturnType<typeof embeddedOptions>);
      const producer = new Fluxbase(base);
      await producer.start();
      const added = await producer.queue('crash').add('last-chance', {}, { attempts: 1 });
      await producer.close();

      const { child } = await startCrashingWorker(base);
      child.kill('SIGKILL');
      await new Promise((r) => child.once('exit', r));

      const reader = new Fluxbase(base);
      await reader.start();
      try {
        const q = reader.queue('crash');
        // Claim loop recovers the expired lease even with no handler running.
        await q.pause();
        await q.resume();
        const process = q.process(async () => undefined);
        const failed = await waitFor(async () => {
          const j = await q.getJob(added.id);
          return j?.state === 'failed' ? j : null;
        }, { timeoutMs: 15_000, message: 'lease-expired failure' });
        expect(failed.failedReason).toMatch(/LeaseExpired/);
        await process.stop();
      } finally {
        await reader.close();
      }
    }, 40_000);
  });
}
