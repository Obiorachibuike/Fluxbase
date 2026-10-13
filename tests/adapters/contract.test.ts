/**
 * Contract tests. The same assertions run against every storage adapter, so the
 * SQLite and Redis implementations must behave identically for the operations
 * the queue engine depends on. Redis cases are skipped unless REDIS_URL is set.
 */
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { SqliteStorageAdapter } from '../../src/storage/sqlite/sqlite-adapter.js';
import { RedisStorageAdapter } from '../../src/storage/redis/redis-adapter.js';
import type { NewJobRecord, StorageAdapter } from '../../src/storage/adapter.js';
import { DuplicateJobError } from '../../src/core/errors.js';
import { cleanupDirs, hasRedis, REDIS_URL, tempDir, uniquePrefix } from '../helpers.js';
import { join } from 'node:path';

type Factory = { name: string; make: () => StorageAdapter; skip: boolean };

const factories: Factory[] = [
  { name: 'sqlite file', make: () => new SqliteStorageAdapter(join(tempDir(), 'contract.db'), false), skip: false },
  { name: 'sqlite memory', make: () => new SqliteStorageAdapter(':memory:', true), skip: false },
  {
    name: 'redis',
    make: () => new RedisStorageAdapter(REDIS_URL!, uniquePrefix('contract'), 3000, () => undefined),
    skip: !hasRedis,
  },
];

afterAll(() => cleanupDirs());

function job(overrides: Partial<NewJobRecord> & { id: string }): NewJobRecord {
  return {
    queue: 'contract',
    name: 'task',
    data: '{"n":1}',
    state: 'waiting',
    priority: 0,
    attempts: 3,
    backoff: null,
    timeoutMs: null,
    createdAt: 1_000,
    runAt: 1_000,
    metadata: '{}',
    removeOnComplete: false,
    removeOnFail: false,
    ...overrides,
  };
}

for (const f of factories) {
  describe.skipIf(f.skip)(`storage adapter contract: ${f.name}`, () => {
    let storage: StorageAdapter;
    const NOW = 10_000;

    beforeEach(async () => {
      storage = f.make();
      await storage.open();
      // A fresh queue name per test keeps Redis cases independent of each other.
    });

    const queue = () => `q${Math.random().toString(36).slice(2, 10)}`;

    it('inserts, reads, and rejects duplicate ids', async () => {
      const q = queue();
      const stored = await storage.insertJob(job({ queue: q, id: 'a' }));
      expect(stored).toMatchObject({ id: 'a', state: 'waiting', attemptsMade: 0, attempts: 3, runAt: 1_000 });
      expect(await storage.getJob(q, 'a')).toMatchObject({ id: 'a' });
      await expect(storage.insertJob(job({ queue: q, id: 'a' }))).rejects.toBeInstanceOf(DuplicateJobError);
      expect(await storage.getJob(q, 'missing')).toBeNull();
    });

    it('claims in priority order, then FIFO, and increments attemptsMade', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'low1' }));
      await storage.insertJob(job({ queue: q, id: 'low2' }));
      await storage.insertJob(job({ queue: q, id: 'high', priority: 10 }));
      const claimed = await storage.claimJobs({ queue: q, workerId: 'w', limit: 3, leaseMs: 5_000, now: NOW });
      expect(claimed.map((j) => j.id)).toEqual(['high', 'low1', 'low2']);
      expect(claimed.every((j) => j.attemptsMade === 1 && j.state === 'active' && j.lockToken)).toBe(true);
      expect(await storage.claimJobs({ queue: q, workerId: 'w', limit: 3, leaseMs: 5_000, now: NOW })).toEqual([]);
    });

    it('never hands the same job to two claimers', async () => {
      const q = queue();
      for (let i = 0; i < 50; i++) await storage.insertJob(job({ queue: q, id: `j${i}` }));
      const results = await Promise.all(
        Array.from({ length: 8 }, (_, w) => storage.claimJobs({ queue: q, workerId: `w${w}`, limit: 10, leaseMs: 5_000, now: NOW })),
      );
      const ids = results.flat().map((j) => j.id);
      expect(ids.length).toBe(50);
      expect(new Set(ids).size).toBe(50);
    });

    it('completes only with the matching lease token', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'c' }));
      const [claimed] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      const ok = await storage.completeJob({ queue: q, id: 'c', token: 'wrong', returnValue: null, now: NOW, keepCompleted: 10 });
      expect(ok).toBe(false);
      const done = await storage.completeJob({ queue: q, id: 'c', token: claimed!.lockToken!, returnValue: '"ok"', now: NOW + 1, keepCompleted: 10 });
      expect(done).toBe(true);
      expect(await storage.getJob(q, 'c')).toMatchObject({ state: 'completed', returnValue: '"ok"' });
    });

    it('schedules retries into delayed, and promotes them when due', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'r' }));
      const [claimed] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      const result = await storage.failJob({
        queue: q,
        id: 'r',
        token: claimed!.lockToken!,
        reason: 'boom',
        now: NOW,
        retryAt: NOW + 500,
        keepFailed: 10,
      });
      expect(result).toBe('retry');
      expect(await storage.getJob(q, 'r')).toMatchObject({ state: 'delayed', failedReason: 'boom', runAt: NOW + 500 });
      expect(await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW + 499 })).toEqual([]);
      const [again] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW + 500 });
      expect(again).toMatchObject({ id: 'r', attemptsMade: 2 });
    });

    it('moves a job to failed when no retry is given, and retry resets attempts', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'f' }));
      const [claimed] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      expect(await storage.failJob({ queue: q, id: 'f', token: claimed!.lockToken!, reason: 'final', now: NOW, retryAt: null, keepFailed: 10 })).toBe('failed');
      expect(await storage.getJob(q, 'f')).toMatchObject({ state: 'failed', failedReason: 'final', finishedAt: NOW });
      expect(await storage.retryJob(q, 'f', NOW + 1)).toBe('retried');
      expect(await storage.getJob(q, 'f')).toMatchObject({ state: 'waiting', attemptsMade: 0, failedReason: null });
      expect(await storage.retryJob(q, 'f', NOW + 2)).toBe('not_failed');
      expect(await storage.retryJob(q, 'nope', NOW + 2)).toBe('not_found');
    });

    it('recovers jobs whose lease expired, and fails them once attempts are exhausted', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'x', attempts: 2 }));
      await storage.claimJobs({ queue: q, workerId: 'crashed', limit: 1, leaseMs: 1_000, now: NOW });
      // Before expiry nothing is recovered.
      expect(await storage.claimJobs({ queue: q, workerId: 'w2', limit: 1, leaseMs: 1_000, now: NOW + 999 })).toEqual([]);
      // After expiry the attempt is recovered and runs again.
      const [second] = await storage.claimJobs({ queue: q, workerId: 'w2', limit: 1, leaseMs: 1_000, now: NOW + 1_000 });
      expect(second).toMatchObject({ id: 'x', attemptsMade: 2, workerId: 'w2' });
      // The second lease also expires, and attempts are exhausted.
      expect(await storage.claimJobs({ queue: q, workerId: 'w3', limit: 1, leaseMs: 1_000, now: NOW + 2_000 })).toEqual([]);
      const failed = await storage.getJob(q, 'x');
      expect(failed).toMatchObject({ state: 'failed' });
      expect(failed!.failedReason).toMatch(/LeaseExpired/);
    });

    it('rejects a stale lease after recovery, so the old worker cannot overwrite the result', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 's', attempts: 3 }));
      const [stale] = await storage.claimJobs({ queue: q, workerId: 'slow', limit: 1, leaseMs: 1_000, now: NOW });
      await storage.claimJobs({ queue: q, workerId: 'fast', limit: 1, leaseMs: 1_000, now: NOW + 5_000 });
      expect(await storage.renewLeases(q, [{ id: 's', token: stale!.lockToken! }], 1_000, NOW + 5_000)).toEqual([]);
      expect(await storage.completeJob({ queue: q, id: 's', token: stale!.lockToken!, returnValue: '1', now: NOW + 5_001, keepCompleted: 10 })).toBe(false);
    });

    it('renews leases for the current token only', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'h' }));
      const [claimed] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 1_000, now: NOW });
      expect(await storage.renewLeases(q, [{ id: 'h', token: claimed!.lockToken! }], 60_000, NOW + 500)).toEqual(['h']);
      // Still active at NOW+30s because the lease was renewed.
      expect(await storage.claimJobs({ queue: q, workerId: 'w2', limit: 1, leaseMs: 1_000, now: NOW + 30_000 })).toEqual([]);
    });

    it('releases a claim without counting the attempt', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'rel' }));
      const [claimed] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      expect(await storage.releaseJob(q, 'rel', claimed!.lockToken!)).toBe(true);
      const [again] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      expect(again!.attemptsMade).toBe(1);
    });

    it('removes non-active jobs and refuses active ones', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'rm1' }));
      await storage.insertJob(job({ queue: q, id: 'rm2' }));
      await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      expect(await storage.removeJob(q, 'rm1')).toBe('active');
      expect(await storage.removeJob(q, 'rm2')).toBe('removed');
      expect(await storage.removeJob(q, 'rm2')).toBe('not_found');
    });

    it('pauses claiming without losing jobs', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'p' }));
      await storage.setPaused(q, true);
      expect(await storage.isPaused(q)).toBe(true);
      expect(await storage.claimJobs({ queue: q, workerId: 'w', limit: 5, leaseMs: 5_000, now: NOW })).toEqual([]);
      await storage.setPaused(q, false);
      expect(await storage.claimJobs({ queue: q, workerId: 'w', limit: 5, leaseMs: 5_000, now: NOW })).toHaveLength(1);
    });

    it('trims finished jobs beyond the retention count', async () => {
      const q = queue();
      for (let i = 0; i < 5; i++) {
        await storage.insertJob(job({ queue: q, id: `t${i}`, runAt: NOW }));
        const [c] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW + i });
        await storage.completeJob({ queue: q, id: `t${i}`, token: c!.lockToken!, returnValue: null, now: NOW + i, keepCompleted: 1000 });
      }
      expect(await storage.trimFinished(q, 'completed', 2)).toBe(3);
      const kept = await storage.listJobs(q, 'completed', 10);
      expect(kept.map((j) => j.id)).toEqual(['t4', 't3']);
    });

    it('deletes a completed job immediately when removeOnComplete is set', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'gone', removeOnComplete: true }));
      const [c] = await storage.claimJobs({ queue: q, workerId: 'w', limit: 1, leaseMs: 5_000, now: NOW });
      await storage.completeJob({ queue: q, id: 'gone', token: c!.lockToken!, returnValue: null, now: NOW, keepCompleted: 10 });
      expect(await storage.getJob(q, 'gone')).toBeNull();
    });

    it('counts jobs by state', async () => {
      const q = queue();
      await storage.insertJob(job({ queue: q, id: 'w1' }));
      await storage.insertJob(job({ queue: q, id: 'd1', state: 'delayed', runAt: NOW + 10_000 }));
      const counts = await storage.countJobs(q);
      expect(counts).toEqual({ waiting: 1, delayed: 1, active: 0, completed: 0, failed: 0 });
    });

    it('stores KV values with TTL and treats expired entries as missing', async () => {
      // Redis expires keys on its own clock, so this uses real time, not the injected NOW.
      const t = Date.now();
      const suffix = Math.random().toString(36).slice(2);
      const a = `kv:a:${suffix}`;
      const b = `kv:b:${suffix}`;
      await storage.kvSet(a, '{"v":1}', t + 300);
      await storage.kvSet(b, '"forever"', null);
      expect(await storage.kvGet(a, t)).toBe('{"v":1}');
      expect(await storage.kvGetMany([a, b, `kv:none:${suffix}`], t)).toEqual(['{"v":1}', '"forever"', null]);
      await new Promise((r) => setTimeout(r, 400));
      expect(await storage.kvGet(a, Date.now())).toBeNull();
      expect(await storage.kvGetMany([a, b], Date.now())).toEqual([null, '"forever"']);
      expect(await storage.kvDelete(b)).toBe(true);
      expect(await storage.kvDelete(b)).toBe(false);
    });

    it('fires a schedule exactly once per slot (compare-and-set)', async () => {
      const q = queue();
      await storage.putSchedule({
        queue: q,
        name: 'daily',
        cron: '0 9 * * *',
        timezone: 'UTC',
        data: '{}',
        jobOptions: '{}',
        nextRunAt: NOW,
        lastRunAt: null,
        createdAt: 1,
        updatedAt: 1,
      });
      const due = await storage.listDueSchedules(NOW, 10);
      expect(due.map((s) => s.name)).toEqual(['daily']);
      const args = {
        queue: q,
        name: 'daily',
        expectedNextRunAt: NOW,
        nextRunAt: NOW + 86_400_000,
        now: NOW,
        job: job({ queue: q, id: `daily:${NOW}`, runAt: NOW }),
      };
      const results = await Promise.all([storage.fireSchedule(args), storage.fireSchedule(args), storage.fireSchedule(args)]);
      expect(results.filter(Boolean)).toHaveLength(1);
      expect((await storage.countJobs(q)).waiting).toBe(1);
      expect(await storage.listDueSchedules(NOW, 10)).toEqual([]);
      expect(await storage.removeSchedule(q, 'daily')).toBe(true);
    });

    it('reports capabilities that match the backend', () => {
      if (storage.kind === 'redis') {
        expect(storage.capabilities).toMatchObject({ durable: true, multiProcess: true, broadcastEvents: true });
      } else {
        expect(storage.capabilities.multiProcess).toBe(false);
        expect(storage.capabilities.broadcastEvents).toBe(false);
      }
    });

    it('closes cleanly', async () => {
      await storage.close();
      await storage.close(); // idempotent
    });
  });
}
