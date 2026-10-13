import { afterEach, describe, expect, it } from 'vitest';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { Fluxbase, ClosedError, NotStartedError, RedisConnectionError, UnsupportedCapabilityError, ValidationError } from '../../src/index.js';
import { backends, cleanupDirs, distributedOptions, embeddedOptions, hasRedis, memoryOptions, sleep, tempDir, waitFor, REDIS_URL } from '../helpers.js';

const open: Fluxbase[] = [];
type Options = NonNullable<ConstructorParameters<typeof Fluxbase>[0]>;
async function client(opts: Options): Promise<Fluxbase> {
  const fb = new Fluxbase(opts);
  open.push(fb);
  await fb.start();
  return fb;
}
afterEach(async () => {
  await Promise.all(open.splice(0).map((fb) => fb.close()));
  cleanupDirs();
});

for (const backend of backends) {
  describe.skipIf(backend.skip)(`key-value store and cache on ${backend.name}`, () => {
    it('stores typed JSON values and returns undefined for missing keys', async () => {
      const fb = await client(backend.options());
      await fb.kv.set('user:1', { name: 'Ada', roles: ['admin'] });
      expect(await fb.kv.get<{ name: string }>('user:1')).toEqual({ name: 'Ada', roles: ['admin'] });
      expect(await fb.kv.get('nope')).toBeUndefined();
      expect(await fb.kv.has('user:1')).toBe(true);
      expect(await fb.kv.delete('user:1')).toBe(true);
      expect(await fb.kv.has('user:1')).toBe(false);
    });

    it('expires values after a TTL given in seconds', async () => {
      const fb = await client(backend.options());
      await fb.kv.set('session', 'abc', { ttl: 1 });
      expect(await fb.kv.get('session')).toBe('abc');
      await sleep(1100);
      expect(await fb.kv.get('session')).toBeUndefined();
    });

    it('reads several keys at once', async () => {
      const fb = await client(backend.options());
      await fb.kv.set('m:a', 1);
      await fb.kv.set('m:c', 3);
      const map = await fb.kv.getMany(['m:a', 'm:b', 'm:c']);
      expect([...map.entries()]).toEqual([
        ['m:a', 1],
        ['m:c', 3],
      ]);
    });

    it('rejects control characters and bad TTLs', async () => {
      const fb = await client(backend.options());
      await expect(fb.kv.set('a\u0000b', 1)).rejects.toBeInstanceOf(ValidationError);
      await expect(fb.kv.set('ok', 1, { ttl: 0 })).rejects.toBeInstanceOf(ValidationError);
      await expect(fb.kv.set('ok', 1, { ttl: 1e12 })).rejects.toBeInstanceOf(ValidationError);
    });

    it('rejects values that are not JSON-serializable', async () => {
      const fb = await client(backend.options());
      await expect(fb.kv.set('fn', () => 1 as never)).rejects.toThrow(/JSON/);
      await expect(fb.kv.set('undef', undefined as never)).rejects.toThrow(/undefined/);
    });

    it('caches with the same TTL semantics, and separates cache namespaces', async () => {
      const fb = await client(backend.options());
      await fb.cache.set('report', { rows: 10 }, { ttl: 60 });
      expect(await fb.cache.get('report')).toEqual({ rows: 10 });
      const other = fb.cache.namespace('other');
      expect(await other.get('report')).toBeUndefined();
      expect(await fb.cache.delete('report')).toBe(true);
    });

    it('a cache-level getOrSet-style read does not return stale expired data', async () => {
      const fb = await client(backend.options());
      await fb.cache.set('short', 'v', { ttl: 1 });
      await sleep(1100);
      expect(await fb.cache.get('short')).toBeUndefined();
    });
  });

  describe.skipIf(backend.skip)(`local events on ${backend.name}`, () => {
    it('delivers published events to subscribers with source and time', async () => {
      const fb = await client(backend.options());
      const received: Array<{ data: unknown; source: string }> = [];
      const unsubscribe = fb.events.subscribe('order.created', (event) => {
        received.push({ data: event.data, source: event.source });
      });
      await fb.events.publish('order.created', { id: 1 });
      expect(received).toEqual([{ data: { id: 1 }, source: fb.instanceId }]);
      unsubscribe();
      await fb.events.publish('order.created', { id: 2 });
      expect(received).toHaveLength(1);
    });

    it('isolates handler failures from publishers and other handlers', async () => {
      const fb = await client(backend.options());
      let healthy = 0;
      fb.events.subscribe('t', () => {
        throw new Error('handler exploded');
      });
      fb.events.subscribe('t', () => {
        healthy++;
      });
      // publish() resolves with the delivered event envelope.
      await expect(fb.events.publish('t', {})).resolves.toMatchObject({ topic: 't', data: {} });
      expect(healthy).toBe(1);
    });

    it('rejects invalid topics', async () => {
      const fb = await client(backend.options());
      await expect(fb.events.publish('bad topic', {})).rejects.toBeInstanceOf(ValidationError);
    });
  });
}

describe('embedded mode specifics', () => {
  it('scope "broadcast" fails with UnsupportedCapabilityError instead of silently staying local', async () => {
    const fb = await client(embeddedOptions());
    await expect(fb.events.publish('t', {}, { scope: 'broadcast' })).rejects.toBeInstanceOf(UnsupportedCapabilityError);
  });

  it('data survives close and reopen of the same file', async () => {
    const opts = embeddedOptions();
    const first = await client(opts);
    await first.kv.set('persist', 'yes');
    await first.queue('durable').add('job', { n: 1 }, { jobId: 'keep-me' });
    await first.close();
    open.splice(open.indexOf(first), 1);
    const second = await client(opts);
    expect(await second.kv.get('persist')).toBe('yes');
    expect(await second.queue('durable').getJobState('keep-me')).toBe('waiting');
  });

  it('memory mode is volatile: a new client starts empty', async () => {
    const first = await client(memoryOptions());
    await first.kv.set('x', 1);
    expect((await first.health()).volatile).toBe(true);
    await first.close();
    const second = await client(memoryOptions());
    expect(await second.kv.get('x')).toBeUndefined();
  });

  it('cannot silently fall back: an unusable path fails start() with an actionable error', async () => {
    // A regular file used as a directory fails the same way on every platform.
    // (Recursive mkdir under /proc hangs in some sandboxes, so it is not used here.)
    const blocker = join(tempDir(), 'not-a-dir');
    writeFileSync(blocker, 'x');
    const fb = new Fluxbase({ mode: 'embedded', storage: { type: 'sqlite', path: join(blocker, 'fluxbase.db') }, logging: { level: 'silent' } });
    await expect(fb.start()).rejects.toThrow(/SQLite|directory|writable/);
    await fb.close();
    expect((await fb.health()).state).toBe('closed');
  });
});

describe('lifecycle', () => {
  it('rejects operations before start() and after close()', async () => {
    const fb = new Fluxbase(embeddedOptions());
    await expect(fb.kv.get('x')).rejects.toBeInstanceOf(NotStartedError);
    await fb.start();
    await fb.close();
    await expect(fb.kv.get('x')).rejects.toBeInstanceOf(ClosedError);
    await expect(fb.queue('q').add('a', {})).rejects.toBeInstanceOf(ClosedError);
  });

  it('start() and close() are idempotent', async () => {
    const fb = new Fluxbase(embeddedOptions());
    await Promise.all([fb.start(), fb.start()]);
    await Promise.all([fb.close(), fb.close()]);
    await fb.close();
    expect((await fb.health()).state).toBe('closed');
  });

  it('close() waits for an in-flight job to finish', async () => {
    const fb = await client(embeddedOptions());
    const q = fb.queue('slow');
    let finished = false;
    let started!: () => void;
    const startedP = new Promise<void>((r) => (started = r));
    q.process(async () => {
      started();
      await sleep(300);
      finished = true;
    });
    const job = await q.add('slow', {});
    await startedP;
    await fb.close();
    open.splice(open.indexOf(fb), 1);
    expect(finished).toBe(true);
    expect(await fb.health()).toMatchObject({ state: 'closed' });
    expect(job.id).toBeTruthy();
  });

  it('close() past the shutdown timeout releases the job so it is not lost', async () => {
    const opts = embeddedOptions({ worker: { pollIntervalMs: 50, shutdownTimeoutMs: 100 } });
    const fb = await client(opts);
    const q = fb.queue('stuck');
    const job = await q.add('stuck', {}, { attempts: 3 });
    q.process(async (_job, ctx) => {
      await new Promise((_, reject) => ctx.signal.addEventListener('abort', () => reject(new Error('aborted'))));
    });
    await waitFor(async () => (await q.getJobState(job.id)) === 'active', { message: 'job active' });
    await fb.close();
    open.splice(open.indexOf(fb), 1);
    const again = await client(opts);
    // Released without consuming an attempt, so it can be claimed right away.
    const j = await again.queue('stuck').getJob(job.id);
    expect(j?.state).toBe('waiting');
    expect(j?.attemptsMade).toBe(0);
  });

  it('health() reports storage and workers without throwing', async () => {
    const fb = await client(embeddedOptions());
    const q = fb.queue('h');
    q.process(async () => undefined);
    const report = await fb.health();
    expect(report).toMatchObject({ status: 'ok', state: 'running', mode: 'embedded', storage: 'sqlite', volatile: false });
    expect(report.workers).toHaveLength(1);
    expect(report.workers[0]).toMatchObject({ queue: 'h', status: 'running' });
  });
});

describe.skipIf(!hasRedis)('distributed connection failures', () => {
  it('an unreachable Redis fails start() with RedisConnectionError and never falls back', async () => {
    const fb = new Fluxbase({
      mode: 'distributed',
      storage: { type: 'redis', url: 'redis://user:topsecret@127.0.0.1:1', keyPrefix: 'x', connectTimeoutMs: 500 },
      logging: { level: 'silent' },
    });
    const error = await fb.start().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RedisConnectionError);
    expect((error as Error).message).toMatch(/Redis/);
    expect((error as Error).message).not.toContain('topsecret');
    expect(JSON.stringify(error)).not.toContain('topsecret');
    await fb.close();
    expect(REDIS_URL).toBeTruthy();
  });

  it('distributed mode with a valid URL connects and reports redis capabilities', async () => {
    const fb = await client(distributedOptions());
    const report = await fb.health();
    expect(report.storage).toBe('redis');
    expect(report.capabilities.broadcastEvents).toBe(true);
  });
});
