import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { FluxbaseOptions } from '../src/core/config.js';

/** Redis is used when REDIS_URL is set. CI provides it as a service container. */
export const REDIS_URL = process.env.REDIS_URL;
export const hasRedis = Boolean(REDIS_URL);

const dirs: string[] = [];

/** Creates a private temporary directory, removed by `cleanupDirs()`. */
export function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'fluxbase-test-'));
  dirs.push(dir);
  return dir;
}

export function cleanupDirs(): void {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
}

let counter = 0;
/** A Redis key prefix unique to this test run, so parallel test files never share keys. */
export function uniquePrefix(label = 't'): string {
  counter++;
  return `test-${label}-${process.pid}-${Date.now().toString(36)}-${counter}`;
}

/** Options for an embedded SQLite database in a fresh temp directory. */
export function embeddedOptions(overrides: Partial<FluxbaseOptions> = {}): FluxbaseOptions {
  return {
    mode: 'embedded',
    storage: { type: 'sqlite', path: join(tempDir(), 'fluxbase.db') },
    logging: { level: 'silent' },
    worker: { pollIntervalMs: 50, shutdownTimeoutMs: 2000 },
    scheduler: { tickMs: 100 },
    ...overrides,
  };
}

/** Options for distributed mode against the Redis in REDIS_URL. */
export function distributedOptions(overrides: Partial<FluxbaseOptions> = {}): FluxbaseOptions {
  return {
    mode: 'distributed',
    storage: { type: 'redis', url: REDIS_URL!, keyPrefix: uniquePrefix('d') },
    logging: { level: 'silent' },
    worker: { pollIntervalMs: 50, shutdownTimeoutMs: 2000 },
    scheduler: { tickMs: 100 },
    ...overrides,
  };
}

export function memoryOptions(overrides: Partial<FluxbaseOptions> = {}): FluxbaseOptions {
  return {
    mode: 'memory',
    logging: { level: 'silent' },
    worker: { pollIntervalMs: 50, shutdownTimeoutMs: 2000 },
    scheduler: { tickMs: 100 },
    ...overrides,
  };
}

/** Polls `predicate` until it returns a truthy value or the timeout elapses. */
export async function waitFor<T>(
  predicate: () => T | Promise<T>,
  { timeoutMs = 10_000, intervalMs = 20, message = 'condition' }: { timeoutMs?: number; intervalMs?: number; message?: string } = {},
): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  let last: T | undefined;
  while (Date.now() < deadline) {
    last = await predicate();
    if (last) return last as NonNullable<T>;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`Timed out after ${timeoutMs} ms waiting for ${message}. Last value: ${String(last)}`);
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** The storage backends the shared integration suites run against. */
export const backends: Array<{ name: string; options: () => FluxbaseOptions; skip: boolean }> = [
  { name: 'embedded (sqlite file)', options: () => embeddedOptions(), skip: false },
  { name: 'memory (volatile sqlite)', options: () => memoryOptions(), skip: false },
  { name: 'distributed (redis)', options: () => distributedOptions(), skip: !hasRedis },
];
