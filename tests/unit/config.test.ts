import { describe, expect, it } from 'vitest';
import { resolveConfig, validateBackoff } from '../../src/core/config.js';
import { ConfigError } from '../../src/core/errors.js';
import { Fluxbase } from '../../src/index.js';

describe('resolveConfig', () => {
  it('defaults to embedded mode with a SQLite file', () => {
    const c = resolveConfig(undefined);
    expect(c.mode).toBe('embedded');
    expect(c.storage).toEqual({ kind: 'sqlite', path: './data/fluxbase.db', volatile: false });
    expect(c.defaults.attempts).toBe(1);
    expect(c.logging.level).toBe('warn');
  });

  it('memory mode is volatile and rejects storage options', () => {
    const c = resolveConfig({ mode: 'memory' });
    expect(c.storage).toEqual({ kind: 'sqlite', path: ':memory:', volatile: true });
    expect(() => resolveConfig({ mode: 'memory', storage: { type: 'sqlite', path: 'x.db' } })).toThrow(ConfigError);
  });

  it('distributed mode requires a Redis URL with a valid scheme', () => {
    expect(() => resolveConfig({ mode: 'distributed' })).toThrow(/requires storage/);
    expect(() => resolveConfig({ mode: 'distributed', storage: { type: 'redis', url: '' } })).toThrow(/Missing Redis URL/);
    expect(() => resolveConfig({ mode: 'distributed', storage: { type: 'redis', url: 'http://x' } })).toThrow(/redis:\/\//);
    const c = resolveConfig({ mode: 'distributed', storage: { type: 'redis', url: 'redis://localhost:6379' } });
    expect(c.storage).toMatchObject({ kind: 'redis', keyPrefix: 'fluxbase', connectTimeoutMs: 5000 });
  });

  it('rejects mismatched mode and storage combinations', () => {
    expect(() => resolveConfig({ mode: 'embedded', storage: { type: 'redis', url: 'redis://x' } })).toThrow(/requires storage.type "sqlite"/);
    expect(() => resolveConfig({ mode: 'distributed', storage: { type: 'sqlite', path: 'a.db' } })).toThrow(/requires storage.type "redis"/);
  });

  it('rejects invalid paths', () => {
    expect(() => resolveConfig({ storage: { type: 'sqlite', path: '' } })).toThrow(/non-empty file path/);
    expect(() => resolveConfig({ storage: { type: 'sqlite', path: ':memory:' } })).toThrow(/mode "memory"/);
    expect(() => resolveConfig({ storage: { type: 'sqlite', path: 'bad\0name.db' } })).toThrow(/NUL/);
  });

  it('validates retry defaults', () => {
    expect(() => resolveConfig({ defaults: { attempts: 0 } })).toThrow(/defaults.attempts/);
    expect(() => resolveConfig({ defaults: { attempts: 1000 } })).toThrow(/defaults.attempts/);
    expect(() => resolveConfig({ defaults: { backoff: { type: 'linear' as never, delay: 1 } } })).toThrow(/"fixed" or "exponential"/);
    expect(() => resolveConfig({ defaults: { backoff: { type: 'fixed', delay: -1 } } })).toThrow(/delay/);
  });

  it('validates worker and scheduler limits', () => {
    expect(() => resolveConfig({ worker: { concurrency: 0 } })).toThrow(/worker.concurrency/);
    expect(() => resolveConfig({ worker: { leaseMs: 10 } })).toThrow(/worker.leaseMs/);
    expect(() => resolveConfig({ scheduler: { tickMs: 1 } })).toThrow(/scheduler.tickMs/);
    expect(() => resolveConfig({ logging: { level: 'verbose' as never } })).toThrow(/logging.level/);
  });

  it('rejects non-object options', () => {
    expect(() => resolveConfig('nope' as never)).toThrow(ConfigError);
  });
});

describe('validateBackoff', () => {
  it('accepts fixed and exponential configurations and fills maxDelay', () => {
    expect(validateBackoff({ type: 'exponential', delay: 1000 })).toEqual({ type: 'exponential', delay: 1000, maxDelay: 86_400_000 });
    expect(validateBackoff({ type: 'fixed', delay: 0, maxDelay: 5 })).toEqual({ type: 'fixed', delay: 0, maxDelay: 5 });
  });
});

describe('Fluxbase constructor', () => {
  it('does not touch storage until start()', async () => {
    const fb = new Fluxbase({ mode: 'distributed', storage: { type: 'redis', url: 'redis://127.0.0.1:1' } });
    expect(fb.mode).toBe('distributed');
    await expect(fb.queue('x').add('a', {})).rejects.toMatchObject({ code: 'NOT_STARTED' });
    await fb.close();
    await expect(fb.queue('x').add('a', {})).rejects.toMatchObject({ code: 'CLOSED' });
  });

  it('rejects invalid options synchronously with ConfigError', () => {
    expect(() => new Fluxbase({ mode: 'distributed' })).toThrow(ConfigError);
  });
});
