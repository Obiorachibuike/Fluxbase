import type { StorageAdapter } from '../storage/adapter.js';
import { ClosedError, NotStartedError } from './errors.js';
import type { Lifecycle } from './lifecycle.js';

/**
 * Wraps the adapter so every data operation checks the lifecycle first. Before
 * start, operations fail with NotStartedError. After close, they fail with ClosedError.
 * `open`, `close`, and `ping` are passed through unchanged, so start and close can run.
 */
export function guardStorage(raw: StorageAdapter, lifecycle: Lifecycle): StorageAdapter {
  const passthrough = new Set<PropertyKey>(['open', 'close', 'ping', 'kind', 'capabilities']);
  return new Proxy(raw, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target) as unknown;
      if (passthrough.has(prop) || typeof value !== 'function') {
        return value;
      }
      return (...args: unknown[]) => {
        const state = lifecycle.state;
        if (state === 'created' || state === 'starting') throw new NotStartedError();
        if (state === 'closed') throw new ClosedError();
        return (value as (...a: unknown[]) => unknown).apply(target, args);
      };
    },
  });
}
