import { afterEach, describe, expect, it } from 'vitest';
import { Fluxbase, ValidationError } from '../../src/index.js';
import { cleanupDirs, distributedOptions, embeddedOptions, hasRedis, waitFor, sleep } from '../helpers.js';

const open: Fluxbase[] = [];
async function client(options: ConstructorParameters<typeof Fluxbase>[0]): Promise<Fluxbase> {
  const fb = new Fluxbase(options);
  open.push(fb);
  await fb.start();
  return fb;
}
afterEach(async () => {
  await Promise.all(open.splice(0).map((fb) => fb.close()));
  cleanupDirs();
});

describe('cron schedules (embedded)', () => {
  it('enqueues a job when a schedule is due, with a deterministic id per slot', async () => {
    const fb = await client(embeddedOptions());
    const q = fb.queue('cron');
    const ran: string[] = [];
    q.process(async (job) => {
      ran.push(job.id);
    });
    // "Every second" in 6-field form fires quickly in tests.
    await q.schedule('tick', { t: 1 }, { cron: '* * * * * *', timezone: 'UTC' });
    await waitFor(() => ran.length >= 1, { timeoutMs: 5000, message: 'scheduled run' });
    expect(ran[0]).toMatch(/^tick:\d+$/);
  }, 15_000);

  it('updateSchedule-style replacement recomputes the next run from now', async () => {
    const fb = await client(embeddedOptions());
    const q = fb.queue('cron2');
    const first = await q.schedule('nightly', {}, { cron: '0 3 * * *', timezone: 'UTC' });
    const second = await q.schedule('nightly', {}, { cron: '0 4 * * *', timezone: 'UTC' });
    expect(new Date(second.nextRunAt).toISOString().slice(11, 16)).toBe('04:00');
    expect(second.nextRunAt).not.toBe(first.nextRunAt);
    expect(await q.listSchedules()).toHaveLength(1);
  });

  it('rejects an invalid cron expression and an unknown timezone', async () => {
    const fb = await client(embeddedOptions());
    const q = fb.queue('cron3');
    await expect(q.schedule('bad', {}, { cron: 'not cron' })).rejects.toBeInstanceOf(ValidationError);
    await expect(q.schedule('bad', {}, { cron: '* * * * *', timezone: 'Nowhere/City' })).rejects.toThrow(/timezone/i);
    await expect(q.schedule('bad name!', {}, { cron: '* * * * *' })).rejects.toBeInstanceOf(ValidationError);
  });

  it('schedules survive a restart', async () => {
    const opts = embeddedOptions();
    const first = await client(opts);
    await first.queue('persist-cron').schedule('weekly', {}, { cron: '0 0 * * 0', timezone: 'UTC' });
    await first.close();
    open.splice(open.indexOf(first), 1);
    const second = await client(opts);
    expect((await second.queue('persist-cron').listSchedules()).map((s) => s.name)).toEqual(['weekly']);
  });
});

describe.skipIf(!hasRedis)('Redis broadcast events across instances', () => {
  it('a broadcast from one instance reaches subscribers in another instance', async () => {
    const opts = distributedOptions();
    const a = await client(opts);
    const b = await client(opts);
    const received: Array<{ source: string; data: unknown }> = [];
    b.events.subscribe('cache.invalidate', (e) => {
      received.push({ source: e.source, data: e.data });
    });
    await sleep(100); // allow the Pub/Sub subscription to be established
    await a.events.publish('cache.invalidate', { key: 'user:1' }, { scope: 'broadcast' });
    await waitFor(() => received.length === 1, { timeoutMs: 5000, message: 'broadcast delivery' });
    expect(received[0]).toEqual({ source: a.instanceId, data: { key: 'user:1' } });
  });

  it('the publishing instance does not receive its own broadcast twice', async () => {
    const a = await client(distributedOptions());
    let count = 0;
    a.events.subscribe('self.echo', () => {
      count++;
    });
    await sleep(100);
    await a.events.publish('self.echo', {}, { scope: 'broadcast' });
    await sleep(300);
    expect(count).toBe(1);
  });

  it('a broadcast is not durable: a subscriber that is not running misses it', async () => {
    const opts = distributedOptions();
    const publisher = await client(opts);
    await publisher.events.publish('lost.event', { n: 1 }, { scope: 'broadcast' });
    const late = await client(opts);
    let seen = 0;
    late.events.subscribe('lost.event', () => {
      seen++;
    });
    await sleep(300);
    expect(seen).toBe(0);
  });
});
