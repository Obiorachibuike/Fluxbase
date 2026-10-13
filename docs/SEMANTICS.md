# Delivery semantics, idempotency, and durability

Read this before you build on Fluxbase. The guarantees here are deliberately narrow.

## Jobs are at-least-once

A job's handler may run **more than once**. This happens when:

- a worker crashes or is killed after starting a job and its lease expires; another worker runs the job again (`attemptsMade` increases each time),
- a worker's event loop or network stalls longer than the lease and the lease is recovered,
- `close()` exceeds `worker.shutdownTimeoutMs` and the active job is released. Released jobs do **not** consume an attempt, but the handler may already have produced side effects.

Fluxbase never reports success for a job whose lease was lost. The late result is discarded. Side effects that happened before the crash are not undone.

**Therefore, write handlers to be idempotent.** Patterns that work:

1. **Idempotency key at enqueue.** Pass a stable `jobId` (for example `order-${orderId}-invoice`). A duplicate `add()` throws `DuplicateJobError`, so the same logical request is queued once.
2. **Idempotency record at the side effect.** Before a side effect, check a key in the key-value store, and record it after success. The external system's own idempotency key (Stripe, SES, etc.) is even better. Remember that KV writes are not part of any transaction with the external call.
3. **Make the work a no-op if already done.** Read the current state first, then act only if it is still needed.

`attempts` counts total attempts (default `1`). `UnrecoverableError` skips the remaining attempts.

## Exactly-once is not provided

Fluxbase does not provide exactly-once execution across an external system. Within the queue, each job is **completed or failed by at most one successful write** (the lease token check), but the handler can run twice.

## Durability

### Embedded (SQLite)
- WAL journal, `synchronous = FULL`, `busy_timeout = 5000`, `quick_check` on open.
- A committed transition survives a process crash and power loss on a filesystem that honours fsync.
- Put the database on local disk. Network filesystems commonly break SQLite locking.
- **Single process per file.** Two clients in one process can share a file (the test suite exercises this), but cross-process sharing is not a supported claim (`multiProcess: false`). Use distributed mode for multiple processes or hosts.

### Distributed (Redis)
- Fluxbase's guarantees are only as strong as your Redis persistence settings. For durable queues, enable AOF (`appendonly yes`, `appendfsync everysec` or stronger) and use a replicated or managed Redis with a documented failover story. A Redis that loses its last few seconds of writes can lose or duplicate jobs.
- Redis Pub/Sub broadcast events are **not** durable. A subscriber that is down when the event is published never receives it.

### Memory
- Everything is lost on exit. It is for tests and throwaway tools only.

## Clocks

Lease expiry, delayed-job due times, and schedule slots use the timestamp of the process that makes the call (`Date.now()`). In distributed mode, **keep host clocks synchronised** (NTP or chrony). Skew of several seconds delays or hastens lease recovery. Redis KV TTLs use the Redis server clock.

## Schedules

Cron expressions are evaluated in an IANA timezone (default `UTC`) with `cron-parser`. An invalid expression or timezone is rejected when the schedule is created.

- **Ticks.** The scheduler checks for due schedules every `scheduler.tickMs`. A run is enqueued on the first tick after its slot, so it can start up to `tickMs` late.
- **Run IDs.** Each run has the id `<scheduleName>:<slotEpochMs>`, so the same slot never enqueues twice.
- **Missed runs.** If the process was down across several slots, **one** run is enqueued for the newest missed slot. Missed slots are collapsed, not replayed.
- **Multiple instances.** Each due slot is claimed with a compare-and-set on `next_run_at`, so only one instance enqueues it.
- **DST, spring forward.** A local time that does not exist (for example `02:30` on the spring-forward night in `America/New_York`) resolves to the first valid time after the gap (`03:30` EDT). It runs once.
- **DST, fall back.** A local time that occurs twice (for example `01:30` on the fall-back night) runs **once**, at its first occurrence. The second occurrence is not run.
- **Changing a schedule.** `queue.schedule(name, …)` with a changed cron recomputes the next run from *now*.

These behaviours are asserted in `tests/unit/cron-cache.test.ts` and `tests/integration/scheduler-broadcast.test.ts`.

## Key-value and cache TTL

TTL is in **seconds** and may be fractional (`ttl: 0.5`). Expired entries are never returned. SQLite removes them lazily and in a periodic sweep (every `kv.sweepIntervalMs`, default 60 s). Redis removes them natively.

## Event delivery

- Local `publish()` resolves after all local handlers settle. Handler errors are logged, not thrown.
- Handlers for one topic run concurrently. Do not assume ordering between events.

## Adapter capability differences

| Behaviour | Embedded/memory (SQLite) | Distributed (Redis) |
| --- | --- | --- |
| KV expiry | app clock, lazy + sweep | Redis server clock, native |
| Broadcast events | `UnsupportedCapabilityError` | Pub/Sub, non-durable |
| Job listing order | identical (see below) | identical |
| Cluster | n/a | not supported |

Listing order is identical in both backends: `waiting` in dispatch order; `delayed` and `active` by due time; `completed` and `failed` newest first.
