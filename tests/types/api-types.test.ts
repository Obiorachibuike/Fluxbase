/**
 * Compile-time checks for the public API. These run as part of `npm run typecheck`,
 * which type-checks this file, so a breaking type change fails the build. The
 * runtime assertions only confirm that the module loads.
 */
import { describe, expect, expectTypeOf, it } from 'vitest';
import { Fluxbase, type Job, type FluxbaseOptions, type ProcessContext, type Cache } from '../../src/index.js';
import type { Processor } from '../../src/queue/worker.js';
import type { KeyValueStore } from '../../src/kv/store.js';
import type { EventBus } from '../../src/events/event-bus.js';
import type { Queue } from '../../src/queue/queue.js';

type Events = {
  'user.registered': { userId: string };
  'order.paid': { orderId: string; amountCents: number };
};

describe('public types', () => {
  it('infers job data and return types from the queue generics', () => {
    const fb = new Fluxbase();
    const q = fb.queue<{ to: string }, { messageId: string }>('mail');
    expectTypeOf(q).toEqualTypeOf<Queue<{ to: string }, { messageId: string }>>();
    expectTypeOf(q.process).parameter(0).toEqualTypeOf<Processor<{ to: string }, { messageId: string }>>();
    expectTypeOf<Processor<{ to: string }, { messageId: string }>>().parameter(1).toEqualTypeOf<ProcessContext>();
    expectTypeOf(q.add).returns.resolves.toEqualTypeOf<Job<{ to: string }, { messageId: string }>>();
    expect(typeof q.add).toBe('function');
  });

  it('types typed event topics', () => {
    const fb = new Fluxbase<Events>();
    expectTypeOf(fb.events).toEqualTypeOf<EventBus<Events>>();
    fb.events.subscribe('user.registered', (event) => {
      expectTypeOf(event.data).toEqualTypeOf<{ userId: string }>();
    });
    // @ts-expect-error unknown topic is rejected at compile time
    fb.events.subscribe('user.deleted', () => undefined);
    expect(fb).toBeDefined();
  });

  it('exposes the KV and cache surfaces with precise return types', () => {
    const fb = new Fluxbase();
    expectTypeOf(fb.kv).toEqualTypeOf<KeyValueStore>();
    expectTypeOf(fb.cache).toMatchTypeOf<Cache>();
    expectTypeOf(fb.kv.get<{ a: number }>).returns.resolves.toEqualTypeOf<{ a: number } | undefined>();
    expect(fb).toBeDefined();
  });

  it('options types reject unknown modes at compile time', () => {
    // @ts-expect-error "remote" is not a mode
    const bad: FluxbaseOptions = { mode: 'remote' };
    expect(bad).toBeDefined();
    const good: FluxbaseOptions = { mode: 'memory' };
    expect(good.mode).toBe('memory');
  });
});
