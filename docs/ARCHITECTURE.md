# Architecture

Fluxbase is one npm package with a small core and pluggable storage. The core never imports a web framework, an AI provider, or a Redis client directly. Storage adapters implement one interface, and the queue engine is written against that interface.

```
Fluxbase (client)                          src/core/client.ts
 ├── lifecycle state machine               src/core/lifecycle.ts
 ├── guarded storage (Proxy)               src/core/guarded-storage.ts
 ├── Queue / QueueWorker / Scheduler       src/queue/*
 ├── KeyValueStore + ExpirationSweeper     src/kv/*
 ├── Cache (StorageCache, memory LRU)      src/cache/cache.ts
 └── EventBus (local + broadcast)          src/events/*
            │
            ▼
StorageAdapter interface                   src/storage/adapter.ts
 ├── SqliteStorageAdapter (embedded/memory) src/storage/sqlite/*
 └── RedisStorageAdapter (distributed)      src/storage/redis/*  (Lua scripts)
```

## Modes

| Mode | Storage | Durable | Multi-process | Selected by |
| --- | --- | --- | --- | --- |
| `embedded` (default) | SQLite file (`./data/fluxbase.db`) | Yes (WAL, `synchronous=FULL`) | No, single process per file | default, or `mode: 'embedded'` |
| `distributed` | Redis (standalone or single primary) | Depends on Redis persistence config | Yes | `mode: 'distributed'` plus a Redis URL |
| `memory` | SQLite `:memory:` | **No, volatile** | No | `mode: 'memory'` only |

Modes never fall back to one another. If Redis is unreachable at `start()`, you get `RedisConnectionError`, not an embedded database.

## Lifecycle

`created → starting → running → closing → closed`. Every data operation is guarded:

- before `start()`: `NotStartedError`
- after `close()`: `ClosedError`

`start()` and `close()` are idempotent. Startup failure leaves the client in `created`, so you can retry.

## Job state machine

```
waiting ──claim──▶ active ──complete──▶ completed
   ▲  ▲               │  │
   │  └─retry/recover─┘  └─fail (no attempts left)─▶ failed
   │                     │
   │                     └─retry scheduled─▶ delayed ──due──▶ waiting
   └──────────────────── retry(id) from failed ─────────────────┘
```

`src/queue/job.ts` defines the table (`JOB_TRANSITIONS`). Each transition is one atomic operation: a `BEGIN IMMEDIATE` transaction in SQLite, or one Lua script in Redis. Every write that acts on an active job checks the lease token, so a stale worker cannot change a job it no longer owns.

## Claiming and leases

A claim:

1. promotes due `delayed` jobs to `waiting`,
2. recovers `active` jobs whose lease expired (back to `waiting` if attempts remain, otherwise `failed` with reason `LeaseExpired`),
3. returns nothing if the queue is paused,
4. takes the highest-priority `waiting` jobs (priority descending, then FIFO) and sets a lease with a new random token.

Workers renew their leases every `leaseMs / 3`. A worker that loses its lease discards the result. Redis claim order uses a sorted set score of `(1000 - priority) * 1e12 + seq`.

## Schedules

Schedules are stored as rows (SQLite) or hashes/sorted sets (Redis). The scheduler runs every `scheduler.tickMs` (default 1 s). It finds due schedules, compares and sets `next_run_at` atomically so that only one process enqueues each slot, enqueues the run, and computes the next occurrence. See [SEMANTICS.md](./SEMANTICS.md#schedules) for DST and missed-run rules.

## Events

`events.publish` delivers to local subscribers in-process and waits for them to settle. A handler failure is logged and does not reach the publisher. In distributed mode, `scope: 'broadcast'` also publishes to Redis Pub/Sub. Pub/Sub is at-most-once and non-durable. Each instance skips its own echoes.

## Capabilities

`storage.capabilities` describes what each backend guarantees. The engine checks these before using a feature, so unsupported features fail with `UnsupportedCapabilityError`.

| Capability | SQLite (embedded) | Memory | Redis |
| --- | --- | --- | --- |
| `durable` | yes | no | yes, given Redis persistence |
| `multiProcess` | no | no | yes |
| `crossProcessNotifications` | no (workers poll) | no | yes (wake-ups over Pub/Sub) |
| `broadcastEvents` | no | no | yes (non-durable) |
| `nativeKvExpiry` | no (lazy reads plus sweep) | no | yes (`PXAT`) |

Redis Cluster is not supported: the Lua scripts touch several keys that hash to different slots.
