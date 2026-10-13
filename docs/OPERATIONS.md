# Operations

## Choosing a mode

- **One process, one host:** use the default embedded mode. No Redis, no configuration beyond a file path.
- **Several processes or hosts:** use distributed mode with a standalone Redis (or a single-primary deployment with failover). Redis Cluster is not supported.
- **Tests and throwaway tools:** `mode: 'memory'`. It is volatile by design.

## Starting and stopping

```ts
const fb = new Fluxbase({ /* options */ });
await fb.start();           // throws on bad config or unreachable storage
// ...
await fb.close();           // graceful shutdown
```

`close()` does the following, in order:

1. stops the scheduler,
2. stops each worker: no new claims, then waits up to `worker.shutdownTimeoutMs` (default 10 s) for active jobs,
3. aborts the handlers' `signal` for jobs still running, releases their leases (without consuming an attempt), and gives handlers 1 s to finish,
4. stops the expiration sweeper,
5. closes event subscriptions and the storage connections.

Wire it to signals:

```ts
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.once(signal, () => void fb.close().then(() => process.exit(0)));
}
```

Handlers should watch `context.signal` and stop promptly. Fluxbase cannot force-kill code that ignores it.

## Health

```ts
const report = await fb.health();   // never throws
// { status: 'ok' | 'error', state, mode, storage, volatile, capabilities, error, workers }
```

`health()` pings storage and reports per-worker status. Expose it on your readiness endpoint. Per-queue counts come from `queue.getStats()`.

## Redis requirements

- Redis 6.2 or later (Lua scripts and `PXAT`). CI tests Redis 7.
- Standalone or single-primary. Not Cluster.
- Persistence per your durability needs (see [SEMANTICS.md](./SEMANTICS.md#distributed-redis)).
- Keep the connection URL in a secret store. Fluxbase redacts credentials from logs and errors, but the URL still grants access.
- Set `storage.keyPrefix` per application, so several apps can share one Redis without key collisions.

## Tuning

| Option | Default | Notes |
| --- | --- | --- |
| `worker.concurrency` | 1 | jobs in parallel per `process()` call |
| `worker.leaseMs` | 30 000 | longer = slower crash recovery; shorter = more false recoveries under stalls |
| `worker.pollIntervalMs` | 1 000 | how often each worker claims when idle; lower = lower latency, more storage load |
| `worker.shutdownTimeoutMs` | 10 000 | graceful drain budget |
| `scheduler.tickMs` | 1 000 | schedule latency bound |
| `retention.completed` / `retention.failed` | 1 000 | finished jobs kept per queue |
| `kv.sweepIntervalMs` | 60 000 | SQLite only |

## Upgrading the SQLite schema

The schema version is stored in `PRAGMA user_version`. Startup applies pending migrations in order. Back up the database file before upgrading a major version.

## Logs

Logs are one JSON object per line. Levels are `silent|error|warn|info|debug`. `warn` and `error` go to stderr, the rest to stdout. Payloads and secret-like fields are redacted. Plug in your own sink with `logging: { logger }`.
