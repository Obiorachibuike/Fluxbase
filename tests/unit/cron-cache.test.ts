import { describe, expect, it } from 'vitest';
import { compileCron } from '../../src/queue/cron.js';
import { ValidationError } from '../../src/core/errors.js';
import { createMemoryCache } from '../../src/cache/cache.js';

describe('compileCron', () => {
  it('computes the next UTC occurrence strictly after the given time', () => {
    const c = compileCron('0 9 * * *', 'UTC');
    expect(new Date(c.nextAfter(Date.parse('2026-10-13T08:00:00Z'))).toISOString()).toBe('2026-10-13T09:00:00.000Z');
    expect(new Date(c.nextAfter(Date.parse('2026-10-13T09:00:00Z'))).toISOString()).toBe('2026-10-14T09:00:00.000Z');
  });

  it('honors IANA timezones', () => {
    const c = compileCron('0 9 * * *', 'America/New_York');
    // 09:00 EDT (UTC-4) on 2026-10-13 is 13:00Z.
    expect(new Date(c.nextAfter(Date.parse('2026-10-13T00:00:00Z'))).toISOString()).toBe('2026-10-13T13:00:00.000Z');
  });

  it('documents DST fall-back: a repeated local time runs once, on the first occurrence', () => {
    // 01:30 happens twice on 2026-11-01 in America/New_York (05:30Z EDT and 06:30Z EST).
    const c = compileCron('30 1 * * *', 'America/New_York');
    const first = c.nextAfter(Date.parse('2026-10-31T12:00:00Z'));
    expect(new Date(first).toISOString()).toBe('2026-11-01T05:30:00.000Z');
    const second = c.nextAfter(first);
    expect(new Date(second).toISOString()).toBe('2026-11-02T06:30:00.000Z');
  });

  it('documents DST spring-forward: a skipped local time moves to the next valid time', () => {
    // 02:30 does not exist on 2026-03-08 in America/New_York; it resolves to 03:30 EDT (07:30Z).
    const c = compileCron('30 2 * * *', 'America/New_York');
    expect(new Date(c.nextAfter(Date.parse('2026-03-07T12:00:00Z'))).toISOString()).toBe('2026-03-08T07:30:00.000Z');
  });

  it('accepts 6-field expressions with seconds', () => {
    const c = compileCron('*/15 * * * * *', 'UTC');
    const t = Date.parse('2026-01-01T00:00:00Z');
    expect(c.nextAfter(t) - t).toBeLessThanOrEqual(15_000);
  });

  it('rejects invalid expressions and unknown timezones at definition time', () => {
    expect(() => compileCron('every day', 'UTC')).toThrow(ValidationError);
    expect(() => compileCron('', 'UTC')).toThrow(ValidationError);
    expect(() => compileCron('0 9 * * *', 'Mars/Olympus')).toThrow(/Unknown timezone/);
  });
});

describe('createMemoryCache (volatile)', () => {
  it('is labelled volatile and round-trips values through JSON', async () => {
    const cache = createMemoryCache();
    expect(cache.volatile).toBe(true);
    expect(cache.backend).toBe('memory');
    const original = { items: [1, 2] };
    await cache.set('k', original);
    original.items.push(3); // mutating the source must not change the cache
    const read = await cache.get<{ items: number[] }>('k');
    expect(read).toEqual({ items: [1, 2] });
    read!.items.push(9); // nor may mutating a read result
    expect(await cache.get('k')).toEqual({ items: [1, 2] });
  });

  it('expires entries by TTL in seconds', async () => {
    const cache = createMemoryCache();
    await cache.set('short', 'v', { ttl: 0.05 });
    expect(await cache.has('short')).toBe(true);
    await new Promise((r) => setTimeout(r, 120));
    expect(await cache.get('short')).toBeUndefined();
  });

  it('evicts least recently used entries when full', async () => {
    const cache = createMemoryCache({ maxEntries: 2 });
    await cache.set('a', 1);
    await cache.set('b', 2);
    await cache.get('a'); // a is now most recent
    await cache.set('c', 3); // evicts b
    expect(await cache.has('a')).toBe(true);
    expect(await cache.has('b')).toBe(false);
    expect(await cache.has('c')).toBe(true);
    expect(cache.stats().evictions).toBe(1);
  });

  it('counts hits, misses, sets, and deletes', async () => {
    const cache = createMemoryCache();
    await cache.set('x', 1);
    await cache.get('x');
    await cache.get('missing');
    await cache.delete('x');
    expect(cache.stats()).toMatchObject({ hits: 1, misses: 1, sets: 1, deletes: 1 });
  });

  it('isolates namespaces', async () => {
    const cache = createMemoryCache();
    const a = cache.namespace('a');
    const b = cache.namespace('b');
    await a.set('k', 'from-a');
    await b.set('k', 'from-b');
    expect(await a.get('k')).toBe('from-a');
    expect(await b.get('k')).toBe('from-b');
    expect(await cache.get('k')).toBeUndefined();
  });

  it('bulk-reads only the keys that exist', async () => {
    const cache = createMemoryCache();
    await cache.set('one', 1);
    await cache.set('three', 3);
    const found = await cache.getMany(['one', 'two', 'three']);
    expect([...found.entries()]).toEqual([
      ['one', 1],
      ['three', 3],
    ]);
  });

  it('rejects invalid keys, TTLs, and namespaces', async () => {
    const cache = createMemoryCache();
    await expect(cache.set('', 1)).rejects.toThrow(ValidationError);
    await expect(cache.set('bad\nkey', 1)).rejects.toThrow(/control characters/);
    await expect(cache.set('k', 1, { ttl: -5 })).rejects.toThrow(/ttl/);
    expect(() => cache.namespace('has space')).toThrow(ValidationError);
    expect(() => createMemoryCache({ maxEntries: 0 })).toThrow(ValidationError);
  });
});
