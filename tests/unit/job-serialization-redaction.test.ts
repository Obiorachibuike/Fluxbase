import { describe, expect, it } from 'vitest';
import { InvalidStateError, SerializationError, ValidationError, redactSecrets } from '../../src/core/errors.js';
import { JOB_TRANSITIONS, assertTransition, canTransition, normalizeJobOptions, toPublicJob, type StoredJobFields } from '../../src/queue/job.js';
import { deserialize, serialize } from '../../src/utils/serialization.js';
import { createLogger, redactMeta } from '../../src/observability/logger.js';
import { limits } from '../../src/utils/ids.js';
import { resolveConfig } from '../../src/core/config.js';
import { silentLogger } from '../../src/observability/logger.js';

const defaults = resolveConfig(undefined).defaults;
const gen = () => 'generated-id';

describe('normalizeJobOptions', () => {
  it('applies defaults', () => {
    const spec = normalizeJobOptions(undefined, defaults, gen);
    expect(spec).toEqual({
      id: 'generated-id',
      delay: 0,
      priority: 0,
      attempts: 1,
      backoff: null,
      timeout: null,
      removeOnComplete: false,
      removeOnFail: false,
      metadata: '{}',
    });
  });

  it('uses a custom jobId and validates it', () => {
    expect(normalizeJobOptions({ jobId: 'order-42' }, defaults, gen).id).toBe('order-42');
    expect(() => normalizeJobOptions({ jobId: 'has space' }, defaults, gen)).toThrow(ValidationError);
    expect(() => normalizeJobOptions({ jobId: 'x'.repeat(201) }, defaults, gen)).toThrow(ValidationError);
  });

  it('rejects out-of-range numbers', () => {
    expect(() => normalizeJobOptions({ attempts: 0 }, defaults, gen)).toThrow(/attempts/);
    expect(() => normalizeJobOptions({ attempts: limits.maxAttempts + 1 }, defaults, gen)).toThrow(/attempts/);
    expect(() => normalizeJobOptions({ delay: -1 }, defaults, gen)).toThrow(/delay/);
    expect(() => normalizeJobOptions({ delay: 1.5 }, defaults, gen)).toThrow(/delay/);
    expect(() => normalizeJobOptions({ priority: 5000 }, defaults, gen)).toThrow(/priority/);
    expect(() => normalizeJobOptions({ timeout: 0 }, defaults, gen)).toThrow(/timeout/);
  });

  it('rejects metadata that is not a plain object or is too large', () => {
    expect(() => normalizeJobOptions({ metadata: [] as never }, defaults, gen)).toThrow(/plain object/);
    expect(() => normalizeJobOptions({ metadata: { big: 'x'.repeat(limits.maxMetadataBytes) } }, defaults, gen)).toThrow(/limit/);
  });

  it('uses defaults supplied by the client', () => {
    const d = resolveConfig({ defaults: { attempts: 4, backoff: { type: 'fixed', delay: 10 } } }).defaults;
    const spec = normalizeJobOptions(undefined, d, gen);
    expect(spec.attempts).toBe(4);
    expect(spec.backoff).toEqual({ type: 'fixed', delay: 10, maxDelay: 86_400_000 });
  });
});

describe('job state transitions', () => {
  it('allows the documented edges', () => {
    expect(canTransition('waiting', 'active')).toBe(true);
    expect(canTransition('active', 'delayed')).toBe(true);
    expect(canTransition('failed', 'waiting')).toBe(true);
    expect(canTransition('completed', 'deleted')).toBe(true);
  });

  it('rejects invalid edges', () => {
    expect(canTransition('completed', 'active')).toBe(false);
    expect(canTransition('active', 'deleted')).toBe(false);
    expect(canTransition('waiting', 'completed')).toBe(false);
    expect(() => assertTransition('completed', 'waiting')).toThrow(InvalidStateError);
  });

  it('every state has a defined transition list', () => {
    expect(Object.keys(JOB_TRANSITIONS).sort()).toEqual(['active', 'completed', 'delayed', 'failed', 'waiting']);
  });
});

describe('serialization', () => {
  it('round-trips JSON values', () => {
    const value = { a: [1, 'two', null, true], nested: { x: 1.5 } };
    expect(deserialize(serialize(value, 1000))).toEqual(value);
  });

  it('rejects undefined, functions, bigint, and cycles with SerializationError', () => {
    expect(() => serialize(undefined, 1000)).toThrow(SerializationError);
    expect(() => serialize(() => 1, 1000)).toThrow(SerializationError);
    expect(() => serialize(10n, 1000)).toThrow(SerializationError);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => serialize(cyclic, 1000)).toThrow(SerializationError);
  });

  it('enforces the byte limit', () => {
    expect(() => serialize('x'.repeat(50), 10)).toThrow(/exceeds the 10 byte limit/);
  });

  it('never evaluates stored text; malformed JSON is an error', () => {
    expect(() => deserialize('{not json')).toThrow(SerializationError);
    expect(() => deserialize('process.exit(1)')).toThrow(SerializationError);
  });
});

describe('toPublicJob', () => {
  const base: StoredJobFields = {
    id: 'j',
    queue: 'q',
    name: 'n',
    data: '{"a":1}',
    state: 'waiting',
    priority: 0,
    attempts: 1,
    attemptsMade: 0,
    backoff: null,
    timeoutMs: null,
    createdAt: 1,
    runAt: 1,
    processedAt: null,
    finishedAt: null,
    failedReason: null,
    returnValue: null,
    metadata: '{}',
    lockedUntil: null,
    workerId: null,
  };

  it('parses stored fields into the public view', () => {
    const job = toPublicJob<{ a: number }, unknown>(base);
    expect(job.data).toEqual({ a: 1 });
    expect(job.maxAttempts).toBe(1);
    expect(job.metadata).toEqual({});
  });

  it('surfaces malformed stored data as SerializationError', () => {
    expect(() => toPublicJob({ ...base, data: '{oops' })).toThrow(SerializationError);
  });
});

describe('redaction', () => {
  it('masks URL credentials and secret assignments', () => {
    expect(redactSecrets('redis://default:s3cr3t@host:6379')).toBe('redis://***@host:6379');
    expect(redactSecrets('password=abc token: xyz')).toBe('password=*** token: ***');
    expect(redactSecrets('nothing to see')).toBe('nothing to see');
  });

  it('drops payload-like and secret-like metadata keys from logs', () => {
    const out = redactMeta({ queue: 'q', data: { email: 'a@b' }, apiToken: 'abc', url: 'redis://u:p@h', error: new Error('password=zzz') });
    expect(out).toEqual({
      queue: 'q',
      data: '[redacted]',
      apiToken: '[redacted]',
      url: '[redacted]',
      error: { name: 'Error', code: undefined, message: 'password=***' },
    });
  });

  it('respects the log level', () => {
    const calls: string[] = [];
    const custom = { ...silentLogger, info: (m: string) => calls.push(m), debug: (m: string) => calls.push(m) };
    const log = createLogger('warn', custom);
    log.info('hidden');
    log.debug('hidden');
    expect(calls).toEqual([]);
  });
});
