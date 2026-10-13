# Fluxbase

A TypeScript-first **embedded job queue**, key-value store, cache, and event bus for Node.js.
It runs with **no external services** (SQLite file by default), and scales to several processes
or hosts with **Redis** when you opt in.

> **Status: 0.1.0 release candidate. Not published to npm.** The API may still change before 1.0.

```bash
npm install fluxbase
```

Requires **Node.js 22 or later**. `better-sqlite3` is a native module, so install needs a
supported platform or a build toolchain (prebuilt binaries cover common platforms).

## Quick start

```ts
import { Fluxbase } from 'fluxbase';

const fb = new Fluxbase(); // embedded mode, data in ./data/fluxbase.db
await fb.start();

const emails = fb.queue<{ to: string }, string>('emails');

const worker = emails.process(async (job) => {
  await sendEmail(job.data.to);     // your code
  return 'sent';
});

await emails.add('welcome', { to: 'ada@example.com' }, { attempts: 5, backoff: { type: 'exponential', delay: 1000 } });

process.once('SIGTERM', () => void fb.close().then(() => process.exit(0)));
```

## Modes

| Mode | Command | Durable | Use it for |
| --- | --- | --- | --- |
| **embedded** (default) | `new Fluxbase()` | Yes, single process | Apps and services on one host |
| **distributed** | `mode: 'distributed'`, `storage: { type: 'redis', url }` | Yes, per your Redis persistence | Several processes or hosts |
| **memory** | `mode: 'memory'` | **No. Volatile.** | Tests and throwaway tools |

Modes never fall back to one another. If Redis is unreachable, `start()` throws `RedisConnectionError`.
Memory mode must be selected explicitly.

## Features

- **Jobs:** priority (`-1000..1000`), delayed jobs, retries with fixed or exponential backoff and a cap,
  `UnrecoverableError` to skip retries, per-attempt timeouts, custom `jobId`, `remove`, `retry`,
  listing by state, retention limits per queue.
- **Workers:** configurable concurrency, lease-based crash recovery, heartbeats, graceful shutdown
  with a drain timeout, `AbortSignal` for handlers, health reports.
- **Queues:** `pause()` / `resume()`, `getStats()`.
- **Cron schedules:** IANA timezones, coalesced missed runs, safe across multiple instances.
- **Key-value store:** JSON values, TTL in seconds, bulk reads.
- **Cache:** storage-backed cache with namespaces, plus an in-process LRU (`createMemoryCache`).
- **Events:** typed local events. In distributed mode, `scope: 'broadcast'` uses Redis Pub/Sub (non-durable).
- **Observability:** structured JSON logs with redaction, per-queue metrics, `health()`.

## Important semantics (read before production use)

- **Jobs are at-least-once.** A handler can run again after a crash, a stalled lease, or a
  shutdown timeout. Make handlers idempotent. Use `jobId` to deduplicate enqueues.
- **Durability depends on the backend.** SQLite is durable on one host. Redis is durable only as far
  as its persistence settings allow. Broadcast events are not durable.
- **Schedules:** DST fall-back runs a repeated local time once. A skipped spring-forward time moves
  forward. Missed slots are collapsed into one run.
- **Redis Cluster is not supported.** Standalone or single-primary only.

Full details: [docs/SEMANTICS.md](docs/SEMANTICS.md).

## Documentation

- [Architecture](docs/ARCHITECTURE.md): modes, state machine, leases, capabilities
- [Delivery semantics and durability](docs/SEMANTICS.md): at-least-once, idempotency, DST, clocks, adapter differences
- [Operations](docs/OPERATIONS.md): shutdown, health, Redis requirements, tuning
- [Security](docs/SECURITY.md): JSON-only serialization, redaction, input validation
- [Releasing](docs/RELEASING.md): dry run and manual publish steps
- [API reference](docs/api): generated with `npm run docs` (TypeDoc)
- [Examples](examples): 12 runnable examples, type-checked against the real API

## Examples

| File | Shows |
| --- | --- |
| `01-embedded-quickstart.ts` | minimal embedded setup |
| `02-retries-and-backoff.ts` | retries, backoff, `UnrecoverableError` |
| `03-delayed-and-priority.ts` | delays and priority ordering |
| `04-pause-resume-and-stats.ts` | pause, stats, remove |
| `05-distributed-redis.ts` | Redis mode with an environment URL |
| `06-memory-mode-volatile.ts` | explicit volatile mode |
| `07-key-value-ttl.ts` | KV store with TTL |
| `08-cache.ts` | storage cache and in-memory LRU |
| `09-local-events-typed.ts` | typed events, isolated handler errors |
| `10-broadcast-events-redis.ts` | Redis broadcast across processes |
| `11-cron-schedules.ts` | cron schedules with timezones |
| `12-graceful-shutdown.ts` | SIGTERM handling and drain |

Run any example from the repo root (`npx tsx examples/01-embedded-quickstart.ts`).
Redis examples need `REDIS_URL`.

## Development

```bash
npm ci
npm run typecheck    # src, tests, and examples
npm run lint
npm test             # all suites; Redis suites run when REDIS_URL is set
npm run test:redis   # fails if REDIS_URL is missing, so Redis tests cannot be skipped silently
npm run build        # ESM + CJS + .d.ts in dist/
npm run test:package # pack, install into a clean project, check ESM/CJS/types
npm run docs         # TypeDoc into docs/api
```

Run a local Redis for the distributed suites, for example `docker run -p 6379:6379 redis:7-alpine`,
then `REDIS_URL=redis://127.0.0.1:6379 npm test`.

### Test layout

| Directory | Covers |
| --- | --- |
| `tests/unit` | config validation, backoff, job rules, serialization, redaction, cron, memory cache |
| `tests/adapters` | one contract suite run against SQLite (file and memory) and Redis |
| `tests/integration` | public API on every backend, KV and cache, events, lifecycle, schedules, Redis broadcast |
| `tests/reliability` | SIGKILL of a real worker process with lease recovery, restarts, two-client exactly-once |
| `tests/types` | compile-time checks of the public types |

## License

MIT. See [LICENSE](LICENSE).
